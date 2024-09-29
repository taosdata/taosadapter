package rest

import (
	"encoding/json"
	"net/http"
	"sync/atomic"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	taoserrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
)

type ConfigController struct {
}

func (ctl *ConfigController) Init(r gin.IRouter) {
	r.PUT("config", prepareCtx, CheckAuth, ctl.changeConfig)
}

const (
	unlocked = 0
	locked   = 1
)

var locking int32 = unlocked

func tryLock() bool {
	return atomic.CompareAndSwapInt32(&locking, unlocked, locked)
}
func unlock() {
	atomic.StoreInt32(&locking, unlocked)
}

type ModifyConfig struct {
	LogLevel *string `json:"log.level"`
}

func (ctl *ConfigController) changeConfig(c *gin.Context) {
	logger := c.MustGet(LoggerKey).(*logrus.Entry)
	if !tryLock() {
		TooManyRequestResponse(c, logger, "concurrent modification of configuration is prohibited")
		return
	}
	defer unlock()
	user := c.MustGet(UserKey).(string)
	password := c.MustGet(PasswordKey).(string)
	conn, err := syncinterface.TaosConnect("", user, password, "", 0, logger, log.IsDebug())
	//conn, err := wrapper.TaosConnect("", user, password, "", 0)
	if err != nil {
		taosErr := err.(*taoserrors.TaosError)
		ErrorResponse(c, logger, http.StatusUnauthorized, int(taosErr.Code), taosErr.ErrStr)
		return
	}
	whitelist, err := tool.GetWhitelist(conn)
	if err != nil {
		logger.Errorf("get whitelist failed, err: %s", err)
		taosErr := err.(*taoserrors.TaosError)
		InternalErrorResponse(c, logger, int(taosErr.Code), taosErr.ErrStr)
		return
	}
	valid := tool.CheckWhitelist(whitelist, iptool.GetRealIP(c.Request))
	if !valid {
		logger.Errorf("whitelist prohibits current IP access, ip:%s, whitelist:%s", iptool.GetRealIP(c.Request), tool.IpNetSliceToString(whitelist))
		ForbiddenResponse(c, logger, commonpool.ErrWhitelistForbidden.Error())
		return
	}
	body, err := c.GetRawData()
	if err != nil {
		logger.Errorf("get request body error, err:%s", err)
		BadRequestResponseWithMsg(c, logger, 0xffff, "get request body error")
		return
	}
	logger.Tracef("get modify config request, req:%s", body)
	var modifyConfig ModifyConfig
	err = json.Unmarshal(body, &modifyConfig)
	if err != nil {
		logger.Errorf("unmarshal json error, err:%s, req:%s", err, body)
		BadRequestResponseWithMsg(c, logger, 0xffff, "unmarshal json error")
		return
	}
	if modifyConfig.LogLevel != nil {
		logLevel := *modifyConfig.LogLevel
		logger.Infof("change config, log.level:%s", logLevel)
		err = log.SetLevel(logLevel)
		if err != nil {
			logger.Errorf("change log.level error, err:%s", err)
			BadRequestResponseWithMsg(c, logger, 0xffff, "change log.level error")
			return
		}
	}
	c.JSON(http.StatusOK, &Message{
		Code: 0,
		Desc: "",
	})
	logger.Debugf("change config success")
}

func init() {
	r := &ConfigController{}
	controller.AddController(r)
}

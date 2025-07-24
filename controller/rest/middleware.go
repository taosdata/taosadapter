package rest

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/db/tool"
	taoserrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
)

func prepareCtx(c *gin.Context) {
	timing := c.Query("timing")
	if timing == "true" {
		c.Set(RequireTiming, true)
	}
	c.Set(StartTimeKey, time.Now())
	var reqID int64
	var err error
	if reqIDStr := c.Query("req_id"); len(reqIDStr) != 0 {
		if reqID, err = strconv.ParseInt(reqIDStr, 10, 64); err != nil {
			logger.Errorf("illegal param, req_id must be numeric:%s, err:%s", reqIDStr, err)
			BadRequestResponseWithMsg(c, logger, 0xffff, fmt.Sprintf("illegal param, req_id must be numeric %s", err.Error()))
			return
		}
	}
	if reqID == 0 {
		reqID = generator.GetReqID()
		logger.Tracef("request:%s, client_ip:%s, req_id not set, generate new QID:0x%x", c.Request.RequestURI, c.ClientIP(), reqID)
	}
	c.Set(config.ReqIDKey, reqID)
	ctxLogger := logger.WithField(config.ReqIDKey, reqID)
	c.Set(LoggerKey, ctxLogger)
}

func checkTDengineConnection(c *gin.Context) {
	user := c.MustGet(UserKey).(string)
	password := c.MustGet(PasswordKey).(string)
	conn, err := syncinterface.TaosConnect("", user, password, "", 0, logger, log.IsDebug())
	if err != nil {
		taosErr := err.(*taoserrors.TaosError)
		ErrorResponse(c, logger, http.StatusUnauthorized, int(taosErr.Code), taosErr.ErrStr)
		return
	}
	defer func() {
		syncinterface.TaosClose(conn, logger, log.IsDebug())
	}()
	whitelist, err := tool.GetWhitelist(conn, logger, log.IsDebug())
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
}

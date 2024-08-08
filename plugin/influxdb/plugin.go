package influxdb

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/plugin"
	"github.com/taosdata/taosadapter/v3/schemaless/inserter"
	"github.com/taosdata/taosadapter/v3/tools"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
	"github.com/taosdata/taosadapter/v3/tools/web"
)

var logger = log.GetLogger("PLG").WithField("mod", "influxdb")

type Influxdb struct {
	conf Config
}

func (p *Influxdb) String() string {
	return "influxdb"
}

func (p *Influxdb) Version() string {
	return "v1"
}

func (p *Influxdb) Init(r gin.IRouter) error {
	p.conf.setValue()
	if !p.conf.Enable {
		logger.Info("influxdb disabled")
		return nil
	}
	r.Use(func(c *gin.Context) {
		if monitor.AllPaused() {
			c.Header("Retry-After", "120")
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, "memory exceeds threshold")
			return
		}
	})
	r.POST("write", getAuth, p.write)
	return nil
}

func (p *Influxdb) Start() error {
	if !p.conf.Enable {
		return nil
	}
	return nil
}

func (p *Influxdb) Stop() error {
	return nil
}

// @Tags influxdb
// @Summary influxdb write
// @Description influxdb write v1 https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/
// @Accept plain
// @Produce json
// @Param Authorization header string false "basic authorization"
// @Param u query string false "username to authenticate the request"
// @Param p query string false "username to authenticate the request"
// @Param db query string true "the database to write data to"
// @Param precision query string false "the precision of Unix timestamps in the line protocol"
// @Success 204 {string} string "no content"
// @Failure 401 {object} message
// @Failure 400 {object} badRequest
// @Failure 500 {object} message
// @Router /influxdb/v1/write [post]
func (p *Influxdb) write(c *gin.Context) {
	var reqID uint64
	var err error
	if reqIDStr := c.Query(config.ReqIDKey); len(reqIDStr) > 0 {
		if reqID, err = strconv.ParseUint(reqIDStr, 10, 64); err != nil {
			p.badRequestResponse(c, &badRequest{
				Code:    "illegal param",
				Message: "req_id must be numeric",
				Op:      "parse req_id",
				Err:     "req_id must be numeric",
			})
			return
		}
	}
	if reqID == 0 {
		reqID = uint64(generator.GetReqID())
	}
	c.Set(config.ReqIDKey, reqID)
	logger := logger.WithField(config.ReqIDKey, reqID)

	isDebug := log.IsDebug()
	precision := c.Query("precision")
	if len(precision) == 0 {
		precision = "ns"
	}
	user, password, err := plugin.GetAuth(c)
	if err != nil {
		p.commonResponse(c, http.StatusUnauthorized, &message{
			Code:    "forbidden",
			Message: err.Error(),
		})
		return
	}
	db := c.Query("db")
	if len(db) == 0 {
		logger.Error("db required")
		p.badRequestResponse(c, &badRequest{
			Code:    "not found",
			Message: "db required",
			Op:      "get db query",
			Err:     "db required",
			Line:    0,
		})
		return
	}
	var ttl int
	ttlStr := c.Query("ttl")
	if len(ttlStr) > 0 {
		ttl, err = strconv.Atoi(ttlStr)
		if err != nil {
			p.badRequestResponse(c, &badRequest{
				Code:    "illegal param",
				Message: "ttl must be numeric",
				Op:      "parse ttl",
				Err:     "ttl must be numeric",
			})
			return
		}
	}

	data, err := c.GetRawData()
	if err != nil {
		logger.WithError(err).Errorln("read line error")
		p.badRequestResponse(c, &badRequest{
			Code:    "internal error",
			Message: "read line error",
			Op:      "read line",
			Err:     err.Error(),
			Line:    0,
		})
		return
	}
	taosConn, err := commonpool.GetConnection(user, password, iptool.GetRealIP(c.Request))
	if err != nil {
		if errors.Is(err, commonpool.ErrWhitelistForbidden) {
			p.commonResponse(c, http.StatusUnauthorized, &message{
				Code:    "forbidden",
				Message: err.Error(),
			})
			return
		}
		logger.WithError(err).Errorln("connect server error")
		p.commonResponse(c, http.StatusInternalServerError, &message{Code: "internal error", Message: err.Error()})
		return
	}
	defer func() {
		putErr := taosConn.Put()
		if putErr != nil {
			logger.WithError(putErr).Errorln("connect pool put error")
		}
	}()
	conn := taosConn.TaosConnection
	start := log.GetLogNow(isDebug)
	logger.Debugf("start insert influxdb,data: %s", data)
	err = inserter.InsertInfluxdb(conn, data, db, precision, ttl, reqID, logger)
	logger.Debugf("finish insert influxdb cost:%s", log.GetLogDuration(isDebug, start))
	if err != nil {
		taosError, is := err.(*tErrors.TaosError)
		if is {
			web.SetTaosErrorCode(c, int(taosError.Code))
		}
		logger.WithError(err).Errorln("insert line error", string(data))
		p.commonResponse(c, http.StatusInternalServerError, &message{Code: "internal error", Message: err.Error()})
		return
	}

	c.Status(http.StatusNoContent)
}

type badRequest struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Op      string `json:"op"`
	Err     string `json:"err"`
	Line    int    `json:"line"`
}

type message struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (p *Influxdb) badRequestResponse(c *gin.Context, resp *badRequest) {
	c.JSON(http.StatusBadRequest, resp)
}
func (p *Influxdb) commonResponse(c *gin.Context, code int, resp *message) {
	c.JSON(code, resp)
}

func getAuth(c *gin.Context) {
	auth := c.GetHeader("Authorization")
	if len(auth) != 0 {
		auth = strings.TrimSpace(auth)
		if strings.HasPrefix(auth, "Basic") {
			user, password, err := tools.DecodeBasic(auth[6:])
			if err == nil {
				c.Set(plugin.UserKey, user)
				c.Set(plugin.PasswordKey, password)
			}
		}
	}

	user := c.Query("u")
	password := c.Query("p")
	if len(user) != 0 {
		c.Set(plugin.UserKey, user)
	}
	if len(password) != 0 {
		c.Set(plugin.PasswordKey, password)
	}
}

func init() {
	plugin.Register(&Influxdb{})
}

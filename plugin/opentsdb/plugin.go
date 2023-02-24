package opentsdb

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/taosdata/driver-go/v3/common"
	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/plugin"
	"github.com/taosdata/taosadapter/v3/schemaless/inserter"
	"github.com/taosdata/taosadapter/v3/tools/pool"
	"github.com/taosdata/taosadapter/v3/tools/web"
)

var logger = log.GetLogger("opentsdb")

type Plugin struct {
	conf Config
}

func (p *Plugin) String() string {
	return "opentsdb"
}

func (p *Plugin) Version() string {
	return "v1"
}

func (p *Plugin) Init(r gin.IRouter) error {
	p.conf.setValue()
	if !p.conf.Enable {
		logger.Info("opentsdb disabled")
		return nil
	}
	r.Use(func(c *gin.Context) {
		if monitor.AllPaused() {
			c.Header("Retry-After", "120")
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, "memory exceeds threshold")
			return
		}
	})
	r.POST("put/json/:db", plugin.Auth(p.errorResponse), p.insertJson)
	r.POST("put/telnet/:db", plugin.Auth(p.errorResponse), p.insertTelnet)
	return nil
}

func (p *Plugin) Start() error {
	if !p.conf.Enable {
		return nil
	}
	return nil
}

func (p *Plugin) Stop() error {
	return nil
}

// @Tags opentsdb
// @Summary opentsdb write
// @Description opentsdb write json message
// @Accept json
// @Produce json
// @Param Authorization header string false "basic authorization"
// @Success 204 {string} string "no content"
// @Failure 401 {object} message "unauthorized"
// @Failure 400 {string} string "badRequest"
// @Failure 500 {string} string "internal server error"
// @Router /opentsdb/v1/put/json/:db [post]
func (p *Plugin) insertJson(c *gin.Context) {
	var reqID uint64
	var err error
	if reqIDStr := c.Query(config.ReqIDKey); len(reqIDStr) > 0 {
		if reqID, err = strconv.ParseUint(reqIDStr, 10, 64); err != nil {
			p.errorResponse(c, http.StatusBadRequest,
				fmt.Errorf("illegal param, req_id must be numeric %s", err.Error()))
			return
		}
	}
	if reqID == 0 {
		reqID = uint64(common.GetReqID())
	}
	c.Set(config.ReqIDKey, reqID)

	isDebug := log.IsDebug()
	logger := logger.WithField(config.ReqIDKey, reqID)
	db := c.Param("db")
	if len(db) == 0 {
		logger.Errorln("db required")
		p.errorResponse(c, http.StatusBadRequest, errors.New("db required"))
		return
	}
	data, err := c.GetRawData()
	if err != nil {
		logger.WithError(err).Error("get request body error")
		p.errorResponse(c, http.StatusBadRequest, err)
		return
	}
	user, password, err := plugin.GetAuth(c)
	if err != nil {
		logger.WithError(err).Error("get auth error")
		p.errorResponse(c, http.StatusBadRequest, err)
		return
	}
	var ttl int
	ttlStr := c.Query("ttl")
	if len(ttlStr) > 0 {
		ttl, err = strconv.Atoi(ttlStr)
		if err != nil {
			p.errorResponse(c, http.StatusBadRequest, fmt.Errorf("illegal param, ttl must be numeric %v", err))
			return
		}
	}

	taosConn, err := commonpool.GetConnection(user, password)
	if err != nil {
		logger.WithError(err).Error("connect taosd error")
		p.errorResponse(c, http.StatusInternalServerError, err)
		return
	}
	defer func() {
		putErr := taosConn.Put()
		if putErr != nil {
			logger.WithError(putErr).Errorln("taos connect pool put error")
		}
	}()
	var start time.Time
	if isDebug {
		start = time.Now()
	}
	logger.Debug(start, "insert json payload", string(data))
	err = inserter.InsertOpentsdbJson(taosConn.TaosConnection, data, db, ttl, reqID)
	logger.Debug("insert json payload cost:", time.Since(start))
	if err != nil {
		taosError, is := err.(*tErrors.TaosError)
		if is {
			web.SetTaosErrorCode(c, int(taosError.Code))
		}
		logger.WithError(err).Error("insert json payload error", string(data))
		p.errorResponse(c, http.StatusInternalServerError, err)
		return
	}
	p.successResponse(c)
}

// @Tags opentsdb
// @Summary opentsdb write
// @Description opentsdb write telent message over http
// @Accept plain
// @Produce json
// @Param Authorization header string false "basic authorization"
// @Success 204 {string} string "no content"
// @Failure 401 {object} message "unauthorized"
// @Failure 400 {string} string "badRequest"
// @Failure 500 {string} string "internal server error"
// @Router /opentsdb/v1/put/telnet/:db [post]
func (p *Plugin) insertTelnet(c *gin.Context) {
	var ttl int
	var err error
	var reqID uint64
	if reqIDStr := c.Query(config.ReqIDKey); len(reqIDStr) > 0 {
		if reqID, err = strconv.ParseUint(reqIDStr, 10, 64); err != nil {
			p.errorResponse(c, http.StatusBadRequest,
				fmt.Errorf("illegal param, req_id must be numeric %s", err.Error()))
			return
		}
	}
	if reqID == 0 {
		reqID = uint64(common.GetReqID())
	}
	c.Set(config.ReqIDKey, reqID)

	logger := logger.WithField(config.ReqIDKey, reqID)
	isDebug := log.IsDebug()
	db := c.Param("db")
	if len(db) == 0 {
		logger.Errorln("db required")
		p.errorResponse(c, http.StatusBadRequest, errors.New("db required"))
		return
	}

	ttlStr := c.Query("ttl")
	if len(ttlStr) > 0 {
		ttl, err = strconv.Atoi(ttlStr)
		if err != nil {
			p.errorResponse(c, http.StatusBadRequest, fmt.Errorf("illegal param, ttl must be numeric %v", err))
			return
		}
	}

	rd := bufio.NewReader(c.Request.Body)
	var lines []string
	tmp := pool.BytesPoolGet()
	defer pool.BytesPoolPut(tmp)
	for {
		l, hasNext, err := rd.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				p.errorResponse(c, http.StatusBadRequest, err)
				return
			}
		}
		tmp.Write(l)
		if !hasNext {
			lines = append(lines, tmp.String())
			tmp.Reset()
		}
	}

	user, password, err := plugin.GetAuth(c)
	if err != nil {
		logger.WithError(err).Error("get auth error")
		p.errorResponse(c, http.StatusBadRequest, err)
		return
	}
	taosConn, err := commonpool.GetConnection(user, password)
	if err != nil {
		logger.WithError(err).Error("connect taosd error")
		p.errorResponse(c, http.StatusInternalServerError, err)
		return
	}
	defer func() {
		putErr := taosConn.Put()
		if putErr != nil {
			logger.WithError(putErr).Errorln("taos connect pool put error")
		}
	}()
	var start time.Time
	if isDebug {
		start = time.Now()
	}
	logger.Debug(start, "insert telnet payload", lines)
	err = inserter.InsertOpentsdbTelnetBatch(taosConn.TaosConnection, lines, db, ttl, reqID)
	logger.Debug("insert telnet payload cost:", time.Since(start))
	if err != nil {
		logger.WithError(err).Error("insert telnet payload error", lines)
		p.errorResponse(c, http.StatusInternalServerError, err)
		return
	}
	p.successResponse(c)
}

type message struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
}

func (p *Plugin) errorResponse(c *gin.Context, code int, err error) {
	c.JSON(code, message{
		Code:    code,
		Message: err.Error(),
	})
}

func (p *Plugin) successResponse(c *gin.Context) {
	c.Status(http.StatusNoContent)
}

func init() {
	plugin.Register(&Plugin{})
}

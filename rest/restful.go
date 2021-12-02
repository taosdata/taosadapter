package rest

import (
	"database/sql/driver"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v2/common"
	tErrors "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/db/async"
	"github.com/taosdata/taosadapter/db/commonpool"
	"github.com/taosdata/taosadapter/httperror"
	"github.com/taosdata/taosadapter/log"
	"github.com/taosdata/taosadapter/thread"
	"github.com/taosdata/taosadapter/tools/web"
)

const LayoutMillSecond = "2006-01-02 15:04:05.000"
const LayoutMicroSecond = "2006-01-02 15:04:05.000000"
const LayoutNanoSecond = "2006-01-02 15:04:05.000000000"

var logger = log.GetLogger("restful")

type Restful struct {
}

func (ctl *Restful) Init(r gin.IRouter) error {
	api := r.Group("rest")
	api.POST("sql", checkAuth, ctl.sql)
	api.POST("sqlt", checkAuth, ctl.sqlt)
	api.POST("sqlutc", checkAuth, ctl.sqlutc)
	api.POST("sql/:db", checkAuth, ctl.sql)
	api.POST("sqlt/:db", checkAuth, ctl.sqlt)
	api.POST("sqlutc/:db", checkAuth, ctl.sqlutc)
	api.GET("login/:user/:password", ctl.des)
	return nil
}

type TDEngineRestfulRespDoc struct {
	Status     string          `json:"status,omitempty"`
	Head       []string        `json:"head,omitempty"`
	ColumnMeta [][]interface{} `json:"column_meta,omitempty"`
	Data       [][]interface{} `json:"data,omitempty"`
	Rows       int             `json:"rows,omitempty"`
	Code       int             `json:"code,omitempty"`
	Desc       string          `json:"desc,omitempty"`
}

// @Tags rest
// @Summary execute sql
// @Description execute sql returns results in the time format "2006-01-02 15:04:05.000"
// @Accept plain
// @Produce json
// @Param Authorization header string true "authorization token"
// @Success 200 {object} TDEngineRestfulRespDoc
// @Failure 401 {string} string "unauthorized"
// @Failure 500 {string} string "internal error"
// @Router /rest/sql/:db [post]
// @Router /rest/sql [post]
func (ctl *Restful) sql(c *gin.Context) {
	db := c.Param("db")
	ctl.doQuery(c, db, func(ts int64, precision int) driver.Value {
		switch precision {
		case common.PrecisionMilliSecond:
			return common.TimestampConvertToTime(ts, precision).Local().Format(LayoutMillSecond)
		case common.PrecisionMicroSecond:
			return common.TimestampConvertToTime(ts, precision).Local().Format(LayoutMicroSecond)
		case common.PrecisionNanoSecond:
			return common.TimestampConvertToTime(ts, precision).Local().Format(LayoutNanoSecond)
		}
		panic("unsupported precision")
	})
}

// @Tags rest
// @Summary execute sqlt
// @Description execute sql to return results, time formatted as timestamp
// @Accept plain
// @Produce json
// @Param Authorization header string true "authorization token"
// @Success 200 {object} TDEngineRestfulRespDoc
// @Failure 401 {string} string "unauthorized"
// @Failure 500 {string} string "internal error"
// @Router /rest/sqlt/:db [post]
// @Router /rest/sqlt [post]
func (ctl *Restful) sqlt(c *gin.Context) {
	db := c.Param("db")
	ctl.doQuery(c, db, func(ts int64, precision int) driver.Value {
		return ts
	})
}

// @Tags rest
// @Summary execute sqlutc
// @Description execute sql to return results, time formatted as RFC3339Nano
// @Accept plain
// @Produce json
// @Param Authorization header string true "authorization token"
// @Success 200 {object} TDEngineRestfulRespDoc
// @Failure 401 {string} string "unauthorized"
// @Failure 500 {string} string "internal error"
// @Router /rest/sqlutc/:db [post]
// @Router /rest/sqlutc [post]
func (ctl *Restful) sqlutc(c *gin.Context) {
	db := c.Param("db")
	ctl.doQuery(c, db, func(ts int64, precision int) driver.Value {
		return common.TimestampConvertToTime(ts, precision).Format(time.RFC3339Nano)
	})
}

type TDEngineRestfulResp struct {
	Status     string           `json:"status"`
	Head       []string         `json:"head"`
	ColumnMeta [][]interface{}  `json:"column_meta"`
	Data       [][]driver.Value `json:"data"`
	Rows       int              `json:"rows"`
}

func (ctl *Restful) doQuery(c *gin.Context, db string, timeFunc wrapper.FormatTimeFunc) {
	var s time.Time
	isDebug := logger.Logger.IsLevelEnabled(logrus.DebugLevel)
	id := web.GetRequestID(c)
	logger := logger.WithField("sessionID", id)
	b, err := c.GetRawData()
	if err != nil {
		logger.WithError(err).Error("get request body error")
		errorResponse(c, httperror.HTTP_INVALID_CONTENT_LENGTH)
		return
	}
	if len(b) == 0 {
		logger.Errorln("no msg got")
		errorResponse(c, httperror.HTTP_NO_MSG_INPUT)
		return
	}
	sql := strings.TrimSpace(string(b))
	if len(sql) == 0 {
		logger.Errorln("no sql got")
		errorResponse(c, httperror.HTTP_NO_SQL_INPUT)
		return
	}
	user := c.MustGet(UserKey).(string)
	password := c.MustGet(PasswordKey).(string)
	if isDebug {
		s = time.Now()
	}
	taosConnect, err := commonpool.GetConnection(user, password)

	logger.Debugln("taos connect cost:", time.Now().Sub(s))
	if err != nil {
		logger.WithError(err).Error("connect taosd error")
		var tError *tErrors.TaosError
		if errors.As(err, &tError) {
			errorResponseWithMsg(c, int(tError.Code), tError.ErrStr)
			return
		} else {
			errorResponseWithMsg(c, 0xffff, err.Error())
			return
		}
	}
	defer func() {
		if isDebug {
			s = time.Now()
		}
		err := taosConnect.Put()
		if err != nil {
			panic(err)
		}
		logger.Debugln("taos put connect cost:", time.Now().Sub(s))
	}()

	if len(db) > 0 {
		if isDebug {
			s = time.Now()
		}
		var code int
		thread.Lock()
		code = wrapper.TaosSelectDB(taosConnect.TaosConnection, db)
		thread.Unlock()
		logger.Debugln("taos select db cost:", time.Now().Sub(s))
		if code != httperror.SUCCESS {
			if isDebug {
				s = time.Now()
			}
			errorMsg := tErrors.GetError(code)
			logger.Debugln("taos select db get error string cost:", time.Now().Sub(s))
			logger.Errorln("taos select db error:", sql, code&0xffff, errorMsg.Error())
			errorResponseWithMsg(c, code, errorMsg.Error())
			return
		}
	}

	startExec := time.Now()

	logger.Debugln(startExec, "start execute sql:", sql)
	result, err := async.GlobalAsync.TaosExec(taosConnect.TaosConnection, sql, timeFunc)
	logger.Debugln("execute sql cost:", time.Now().Sub(startExec))
	if err != nil {
		tError, ok := err.(*tErrors.TaosError)
		if ok {
			errorResponseWithMsg(c, int(tError.Code), tError.ErrStr)
		} else {
			errorResponseWithMsg(c, 0xffff, err.Error())
		}
		return
	}
	if isDebug {
		s = time.Now()
	}
	if result.FieldCount == 0 {
		logger.Debugln("execute sql success affected rows:", result.AffectedRows)
		c.JSON(http.StatusOK, &TDEngineRestfulResp{
			Status:     "succ",
			Head:       []string{"affected_rows"},
			Data:       [][]driver.Value{{result.AffectedRows}},
			ColumnMeta: [][]interface{}{{"affected_rows", 4, 4}},
			Rows:       1,
		})
	} else {
		if isDebug {
			s = time.Now()
		}
		if isDebug {
			s = time.Now()
		}
		logger.Debugln("execute sql success return data rows:", len(result.Data), ",cost:", time.Now().Sub(s))
		var columnMeta [][]interface{}
		for i := 0; i < len(result.Header.ColNames); i++ {
			columnMeta = append(columnMeta, []interface{}{
				result.Header.ColNames[i],
				result.Header.ColTypes[i],
				result.Header.ColLength[i],
			})
		}
		c.JSON(http.StatusOK, &TDEngineRestfulResp{
			Status:     "succ",
			Head:       result.Header.ColNames,
			Data:       result.Data,
			ColumnMeta: columnMeta,
			Rows:       len(result.Data),
		})
	}
}

// @Tags rest
// @Summary get login token
// @Description get login token
// @Accept plain
// @Produce json
// @Success 200 {object} Message
// @Failure 500 {string} string "internal error"
// @Router /rest/login/:user/:password [get]
func (ctl *Restful) des(c *gin.Context) {
	user := c.Param("user")
	password := c.Param("password")
	if len(user) < 0 || len(user) > 24 || len(password) < 0 || len(password) > 24 {
		errorResponse(c, httperror.HTTP_GEN_TAOSD_TOKEN_ERR)
		return
	}
	conn, err := commonpool.GetConnection(user, password)
	if err != nil {
		errorResponse(c, httperror.TSDB_CODE_RPC_AUTH_FAILURE)
		return
	}
	conn.Put()
	token, err := EncodeDes(user, password)
	if err != nil {
		errorResponse(c, httperror.HTTP_GEN_TAOSD_TOKEN_ERR)
		return
	}
	c.JSON(http.StatusOK, &Message{
		Status: "succ",
		Code:   0,
		Desc:   token,
	})
}

func (ctl *Restful) Close() {
	return
}

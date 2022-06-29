package rest

import (
	"database/sql/driver"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/huskar-t/melody"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v2/common"
	tErrors "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/controller"
	"github.com/taosdata/taosadapter/db/async"
	"github.com/taosdata/taosadapter/db/commonpool"
	"github.com/taosdata/taosadapter/httperror"
	"github.com/taosdata/taosadapter/log"
	"github.com/taosdata/taosadapter/monitor"
	"github.com/taosdata/taosadapter/thread"
	"github.com/taosdata/taosadapter/tools/ctools"
	"github.com/taosdata/taosadapter/tools/jsonbuilder"
	"github.com/taosdata/taosadapter/tools/web"
)

var logger = log.GetLogger("restful")

type Restful struct {
	wsM   *melody.Melody
	stmtM *melody.Melody
	tmqM  *melody.Melody
}

func (ctl *Restful) Init(r gin.IRouter) {
	ctl.InitWS()
	ctl.InitStmt()
	ctl.InitTMQ()
	api := r.Group("rest")
	api.Use(func(c *gin.Context) {
		if monitor.AllPaused() {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, "memory exceeds threshold")
			return
		}
	})
	api.POST("sql", CheckAuth, ctl.sql)
	api.POST("sql/:db", CheckAuth, ctl.sql)
	api.GET("login/:user/:password", ctl.des)
	api.GET("ws", ctl.ws)
	api.GET("stmt", ctl.stmt)
	api.GET("tmq", ctl.tmq)
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
// @Summary execute sqlutc
// @Description execute sql to return results, time formatted as RFC3339Nano
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
	timeBuffer := make([]byte, 0, 30)
	DoQuery(c, db, func(builder *jsonbuilder.Stream, ts int64, precision int) {
		timeBuffer = timeBuffer[:0]
		switch precision {
		case common.PrecisionMilliSecond: // milli-second
			timeBuffer = time.Unix(0, ts*1e6).UTC().AppendFormat(timeBuffer, time.RFC3339Nano)
		case common.PrecisionMicroSecond: // micro-second
			timeBuffer = time.Unix(0, ts*1e3).UTC().AppendFormat(timeBuffer, time.RFC3339Nano)
		case common.PrecisionNanoSecond: // nano-second
			timeBuffer = time.Unix(0, ts).UTC().AppendFormat(timeBuffer, time.RFC3339Nano)
		default:
			panic("unknown precision")
		}
		builder.WriteString(string(timeBuffer))
	})
}

type TDEngineRestfulResp struct {
	Status     string           `json:"status"`
	Head       []string         `json:"head"`
	ColumnMeta [][]interface{}  `json:"column_meta"`
	Data       [][]driver.Value `json:"data"`
	Rows       int              `json:"rows"`
}

func DoQuery(c *gin.Context, db string, timeFunc ctools.FormatTimeFunc) {
	var s time.Time
	isDebug := logger.Logger.IsLevelEnabled(logrus.DebugLevel)
	id := web.GetRequestID(c)
	logger := logger.WithField("sessionID", id)
	b, err := c.GetRawData()
	if err != nil {
		logger.WithError(err).Error("get request body error")
		ErrorResponse(c, httperror.HTTP_INVALID_CONTENT_LENGTH)
		return
	}
	if len(b) == 0 {
		logger.Errorln("no msg got")
		ErrorResponse(c, httperror.HTTP_NO_MSG_INPUT)
		return
	}
	sql := strings.TrimSpace(string(b))
	if len(sql) == 0 {
		logger.Errorln("no sql got")
		ErrorResponse(c, httperror.HTTP_NO_SQL_INPUT)
		return
	}
	c.Set("sql", sql)
	user := c.MustGet(UserKey).(string)
	password := c.MustGet(PasswordKey).(string)
	if isDebug {
		s = time.Now()
	}
	taosConnect, err := commonpool.GetConnection(user, password)
	if isDebug {
		logger.Debugln("taos connect cost:", time.Now().Sub(s))
	}
	if err != nil {
		logger.WithError(err).Error("connect taosd error")
		if tError, is := err.(*tErrors.TaosError); is {
			switch tError.Code {
			case 0x0101, 0x0214, 0x0512:
				c.AbortWithStatus(http.StatusUnauthorized)
				return
			default:
				ErrorResponseWithMsg(c, int(tError.Code), tError.ErrStr)
				return
			}
		}
		ErrorResponseWithMsg(c, 0xffff, err.Error())
		return
	}
	defer func() {
		if isDebug {
			s = time.Now()
		}
		err := taosConnect.Put()
		if err != nil {
			panic(err)
		}
		if isDebug {
			logger.Debugln("taos put connect cost:", time.Now().Sub(s))
		}
	}()

	if len(db) > 0 {
		if isDebug {
			s = time.Now()
		}
		// Attempt to select the database does not return even if there is an error
		// To avoid error reporting in the `create database` statement
		thread.Lock()
		_ = wrapper.TaosSelectDB(taosConnect.TaosConnection, db)
		thread.Unlock()
		logger.Debugln("taos select db cost:", time.Now().Sub(s))
	}
	execute(c, logger, taosConnect.TaosConnection, sql, timeFunc)
}

var (
	ExecHeader = []byte(`{"head":["affected_rows"],"column_meta":[["affected_rows","INT",4]],"rows":1,"data":[[`)
	ExecEnd    = []byte(`]]}`)
	Query2     = []byte(`{"code":0,"column_meta":[`)
	Query3     = []byte(`],"data":[`)
	Query4     = []byte(`],"rows":`)
)

func execute(c *gin.Context, logger *logrus.Entry, taosConnect unsafe.Pointer, sql string, timeFormat ctools.FormatTimeFunc) {
	isDebug := logger.Logger.IsLevelEnabled(logrus.DebugLevel)
	handler := async.GlobalAsync.HandlerPool.Get()
	defer async.GlobalAsync.HandlerPool.Put(handler)
	var s time.Time
	if isDebug {
		s = time.Now()
	}
	result, _ := async.GlobalAsync.TaosQuery(taosConnect, sql, handler)
	if isDebug {
		logger.Debugln("taos query cost:", time.Now().Sub(s))
	}
	defer func() {
		if result != nil && result.Res != nil {
			if isDebug {
				s = time.Now()
			}
			thread.Lock()
			wrapper.TaosFreeResult(result.Res)
			thread.Unlock()
			if isDebug {
				logger.Debugln("taos free result cost:", time.Now().Sub(s))
			}
		}
	}()
	res := result.Res
	code := wrapper.TaosError(res)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosErrorStr(res)
		ErrorResponseWithMsg(c, code, errStr)
		return
	}
	isUpdate := wrapper.TaosIsUpdateQuery(res)
	w := c.Writer
	c.Header("Content-Type", "application/json; charset=utf-8")
	c.Header("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)
	if isUpdate {
		affectRows := wrapper.TaosAffectedRows(res)
		_, err := w.Write(ExecHeader)
		if err != nil {
			return
		}
		_, err = w.Write([]byte(strconv.Itoa(affectRows)))
		if err != nil {
			return
		}
		_, err = w.Write(ExecEnd)
		if err != nil {
			return
		}
		w.Flush()
		return
	} else {
		if monitor.QueryPaused() {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, "query memory exceeds threshold")
			return
		}
	}
	fieldsCount := wrapper.TaosNumFields(res)
	rowsHeader, err := wrapper.ReadColumn(res, fieldsCount)
	if err != nil {
		tError, ok := err.(*tErrors.TaosError)
		if ok {
			ErrorResponseWithMsg(c, int(tError.Code), tError.ErrStr)
		} else {
			ErrorResponseWithMsg(c, 0xffff, err.Error())
		}
		return
	}
	builder := jsonbuilder.BorrowStream(w)
	defer jsonbuilder.ReturnStream(builder)
	_, err = w.Write(Query2)
	if err != nil {
		return
	}
	for i := 0; i < fieldsCount; i++ {
		builder.WriteArrayStart()
		builder.WriteString(rowsHeader.ColNames[i])
		builder.WriteMore()
		builder.WriteString(rowsHeader.TypeDatabaseName(i))
		builder.WriteMore()
		builder.WriteInt64(rowsHeader.ColLength[i])
		builder.WriteArrayEnd()
		if i != fieldsCount-1 {
			builder.WriteMore()
		}
	}
	err = builder.Flush()
	if err != nil {
		return
	}
	total := 0
	_, err = w.Write(Query3)
	if err != nil {
		return
	}
	precision := wrapper.TaosResultPrecision(res)
	fetched := false
	payloadOffset := uintptr(4 * fieldsCount)
	pHeaderList := make([]uintptr, fieldsCount)
	pStartList := make([]uintptr, fieldsCount)
	for {
		if config.Conf.RestfulRowLimit > -1 && total == config.Conf.RestfulRowLimit {
			err = builder.Flush()
			if err != nil {
				return
			}
			break
		}
		if isDebug {
			s = time.Now()
		}
		result, _ = async.GlobalAsync.TaosFetchRawBlockA(res, handler)
		if isDebug {
			logger.Debugln("taos fetch_rows_a cost:", time.Now().Sub(s))
		}
		if result.N == 0 {
			break
		} else {
			if result.N < 0 {
				break
			}
			res = result.Res
			if fetched {
				builder.WriteMore()
			} else {
				fetched = true
			}
			thread.Lock()
			block := wrapper.TaosGetRawBlock(res)
			thread.Unlock()
			blockSize := result.N
			nullBitMapOffset := uintptr(ctools.BitmapLen(blockSize))
			tmpPHeader := uintptr(block) + payloadOffset + 12 + uintptr(6*fieldsCount) // length i32, group u64
			tmpPStart := tmpPHeader
			for column := 0; column < fieldsCount; column++ {
				colLength := *((*int32)(unsafe.Pointer(uintptr(block) + 12 + uintptr(6*fieldsCount) + uintptr(column)*4)))
				if ctools.IsVarDataType(rowsHeader.ColTypes[column]) {
					pHeaderList[column] = tmpPHeader
					tmpPStart = tmpPHeader + uintptr(4*blockSize)
					pStartList[column] = tmpPStart
				} else {
					pHeaderList[column] = tmpPHeader
					tmpPStart = tmpPHeader + nullBitMapOffset
					pStartList[column] = tmpPStart
				}
				tmpPHeader = tmpPStart + uintptr(colLength)
			}

			for row := 0; row < result.N; row++ {
				builder.WriteArrayStart()
				err = builder.Flush()
				if err != nil {
					return
				}
				for column := 0; column < fieldsCount; column++ {
					ctools.JsonWriteRawBlock(builder, rowsHeader.ColTypes[column], pHeaderList[column], pStartList[column], row, precision, timeFormat)
					if column != fieldsCount-1 {
						builder.WriteMore()
						err = builder.Flush()
						if err != nil {
							return
						}
					}
				}
				builder.WriteArrayEnd()
				err = builder.Flush()
				if err != nil {
					return
				}
				if w.Size() > 16352 {
					w.Flush()
				}
				total += 1
				if config.Conf.RestfulRowLimit > -1 && total == config.Conf.RestfulRowLimit {
					break
				}
				if row != result.N-1 {
					builder.WriteMore()
				}
				err = builder.Flush()
				if err != nil {
					return
				}
			}
		}
	}
	_, err = w.Write(Query4)
	if err != nil {
		return
	}
	builder.WriteInt(total)
	builder.WriteObjectEnd()
	err = builder.Flush()
	if err != nil {
		return
	}
	w.Flush()
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
		ErrorResponse(c, httperror.HTTP_GEN_TAOSD_TOKEN_ERR)
		return
	}
	conn, err := commonpool.GetConnection(user, password)
	if err != nil {
		ErrorResponse(c, httperror.TSDB_CODE_RPC_AUTH_FAILURE)
		return
	}
	conn.Put()
	token, err := EncodeDes(user, password)
	if err != nil {
		ErrorResponse(c, httperror.HTTP_GEN_TAOSD_TOKEN_ERR)
		return
	}
	c.JSON(http.StatusOK, &Message{
		Code: 0,
		Desc: token,
	})
}

func (ctl *Restful) Close() {
	return
}

func init() {
	r := &Restful{}
	controller.AddController(r)
}

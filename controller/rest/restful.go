package rest

import (
	"database/sql/driver"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/huskar-t/melody"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v3/common"
	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/thread"
	"github.com/taosdata/taosadapter/v3/tools/csv"
	"github.com/taosdata/taosadapter/v3/tools/ctools"
	"github.com/taosdata/taosadapter/v3/tools/jsonbuilder"
	"github.com/taosdata/taosadapter/v3/tools/pool"
	"github.com/taosdata/taosadapter/v3/tools/web"
	"github.com/taosdata/taosadapter/v3/version"
)

var logger = log.GetLogger("restful")

type Restful struct {
	wsM            *melody.Melody
	stmtM          *melody.Melody
	tmqM           *melody.Melody
	uploadReplacer *strings.Replacer
	wsVersionResp  []byte
}

func (ctl *Restful) Init(r gin.IRouter) {
	resp := WSVersionResp{
		Action:  ClientVersion,
		Version: version.TaosClientVersion,
	}
	ctl.uploadReplacer = strings.NewReplacer(
		"\\", "\\\\",
		"'", "\\'",
		"(", "\\(",
		")", "\\)",
	)
	ctl.wsVersionResp, _ = json.Marshal(resp)
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
	api.POST("upload", CheckAuth, ctl.upload)
}

type TDEngineRestfulRespDoc struct {
	Code       int             `json:"code,omitempty"`
	Desc       string          `json:"desc,omitempty"`
	ColumnMeta [][]interface{} `json:"column_meta,omitempty"`
	Data       [][]interface{} `json:"data,omitempty"`
	Rows       int             `json:"rows,omitempty"`
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
	Code       int              `json:"code,omitempty"`
	Desc       string           `json:"desc,omitempty"`
	ColumnMeta [][]interface{}  `json:"column_meta,omitempty"`
	Data       [][]driver.Value `json:"data,omitempty"`
	Rows       int              `json:"rows,omitempty"`
}

func DoQuery(c *gin.Context, db string, timeFunc ctools.FormatTimeFunc) {
	var s time.Time
	isDebug := log.IsDebug()
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
			case httperror.TSDB_CODE_MND_USER_ALREADY_EXIST,
				httperror.TSDB_CODE_MND_USER_NOT_EXIST,
				httperror.TSDB_CODE_MND_INVALID_USER_FORMAT,
				httperror.TSDB_CODE_MND_INVALID_PASS_FORMAT,
				httperror.TSDB_CODE_MND_NO_USER_FROM_CONN,
				httperror.TSDB_CODE_MND_TOO_MANY_USERS,
				httperror.TSDB_CODE_MND_INVALID_ALTER_OPER,
				httperror.TSDB_CODE_MND_AUTH_FAILURE:
				ErrorResponseWithStatusMsg(c, http.StatusUnauthorized, int(tError.Code), tError.ErrStr)
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
	ExecHeader = []byte(`{"code":0,"column_meta":[["affected_rows","INT",4]],"data":[[`)
	ExecEnd    = []byte(`]],"rows":1}`)
	Query2     = []byte(`{"code":0,"column_meta":[`)
	Query3     = []byte(`],"data":[`)
	Query4     = []byte(`],"rows":`)
)

func execute(c *gin.Context, logger *logrus.Entry, taosConnect unsafe.Pointer, sql string, timeFormat ctools.FormatTimeFunc) {
	isDebug := log.IsDebug()
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
			lengthOffset := wrapper.RawBlockGetColumnLengthOffset(fieldsCount)
			tmpPHeader := uintptr(block) + wrapper.RawBlockGetColDataOffset(fieldsCount) // length i32, group u64
			tmpPStart := tmpPHeader
			for column := 0; column < fieldsCount; column++ {
				colLength := *((*int32)(unsafe.Pointer(uintptr(block) + lengthOffset + uintptr(column)*wrapper.Int32Size)))
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

const MAXSQLLength = 1024 * 1024 * 1

func (ctl *Restful) upload(c *gin.Context) {
	db := c.Query("db")
	if len(db) == 0 {
		ErrorResponseWithStatusMsg(c, http.StatusBadRequest, 0xffff, "db required")
		return
	}
	table := c.Query("table")
	if len(table) == 0 {
		ErrorResponseWithStatusMsg(c, http.StatusBadRequest, 0xffff, "table required")
		return
	}
	buffer := pool.BytesPoolGet()
	defer pool.BytesPoolPut(buffer)
	colBuffer := pool.BytesPoolGet()
	defer pool.BytesPoolPut(colBuffer)
	isDebug := log.IsDebug()
	logger = logger.WithField("uri", "upload")
	buffer.WriteString(db)
	buffer.WriteByte('.')
	buffer.WriteString(table)
	tableName := buffer.String()
	buffer.Reset()
	buffer.WriteString("describe ")
	buffer.WriteString(tableName)
	sql := buffer.String()
	buffer.Reset()
	user := c.MustGet(UserKey).(string)
	password := c.MustGet(PasswordKey).(string)
	s := log.GetLogNow(isDebug)
	taosConnect, err := commonpool.GetConnection(user, password)
	if isDebug {
		logger.Debugln("taos connect cost:", log.GetLogDuration(isDebug, s))
	}

	if err != nil {
		logger.WithError(err).Error("connect taosd error")
		if tError, is := err.(*tErrors.TaosError); is {
			switch tError.Code {
			case httperror.TSDB_CODE_MND_USER_ALREADY_EXIST,
				httperror.TSDB_CODE_MND_USER_NOT_EXIST,
				httperror.TSDB_CODE_MND_INVALID_USER_FORMAT,
				httperror.TSDB_CODE_MND_INVALID_PASS_FORMAT,
				httperror.TSDB_CODE_MND_NO_USER_FROM_CONN,
				httperror.TSDB_CODE_MND_TOO_MANY_USERS,
				httperror.TSDB_CODE_MND_INVALID_ALTER_OPER,
				httperror.TSDB_CODE_MND_AUTH_FAILURE:
				ErrorResponseWithStatusMsg(c, http.StatusUnauthorized, int(tError.Code), tError.ErrStr)
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
			logger.Debugln("taos put connect cost:", log.GetLogDuration(isDebug, s))
		}
	}()
	s = log.GetLogNow(isDebug)
	result, err := async.GlobalAsync.TaosExec(taosConnect.TaosConnection, sql, func(ts int64, precision int) driver.Value {
		return ts
	})
	logger.Debugln("describe table cost:", log.GetLogDuration(isDebug, s))
	if err != nil {
		taosError, is := err.(*tErrors.TaosError)
		if is {
			ErrorResponseWithMsg(c, int(taosError.Code), taosError.ErrStr)
			return
		}
		ErrorResponseWithMsg(c, 0xffff, err.Error())
		return
	}
	columnCount := 0
	var isStr []bool
	for _, v := range result.Data {
		if v[3].(string) == "TAG" {
			break
		}
		switch v[1].(string) {
		case common.TSDB_DATA_TYPE_TIMESTAMP_Str, common.TSDB_DATA_TYPE_BINARY_Str, common.TSDB_DATA_TYPE_NCHAR_Str:
			isStr = append(isStr, true)
		default:
			isStr = append(isStr, false)
		}
		columnCount += 1
	}
	reader, err := c.Request.MultipartReader()
	if err != nil {
		logger.WithError(err).Error("get multi part reader error")
		ErrorResponseWithMsg(c, 0xffff, err.Error())
		return
	}
	rows := 0
	buffer.WriteString("insert into ")
	buffer.WriteString(tableName)
	buffer.WriteString(" values")
	prefixLength := buffer.Len()
	for {
		part, err := reader.NextPart()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				logger.WithError(err).Error("get next part error")
				ErrorResponseWithMsg(c, 0xffff, err.Error())
				return
			}
		}
		if part.FormName() != "data" {
			continue
		}
		csvReader := csv.NewReader(part)
		csvReader.ReuseRecord = true
		for {
			record, err := csvReader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
			}
			if len(record) < columnCount {
				logger.Errorf("column count not enough got %d want %d", len(record), columnCount)
				ErrorResponseWithMsg(c, 0xffff, "column count not enough")
				return
			}
			colBuffer.WriteString("(")
			for i := 0; i < columnCount; i++ {
				if record[i] == nil {
					colBuffer.WriteString("null")
				} else {
					if isStr[i] {
						colBuffer.WriteByte('\'')
						colBuffer.WriteString(ctl.uploadReplacer.Replace(*record[i]))
						colBuffer.WriteByte('\'')
					} else {
						colBuffer.WriteString(*record[i])
					}
				}
				if i != columnCount-1 {
					colBuffer.WriteByte(',')
				}
			}
			colBuffer.WriteByte(')')
			rows += 1
			if buffer.Len()+colBuffer.Len() >= MAXSQLLength {
				s = log.GetLogNow(isDebug)
				err = async.GlobalAsync.TaosExecWithoutResult(taosConnect.TaosConnection, buffer.String())
				logger.Debugln("execute insert sql1 cost:", log.GetLogDuration(isDebug, s))
				if err != nil {
					taosError, is := err.(*tErrors.TaosError)
					if is {
						ErrorResponseWithMsg(c, int(taosError.Code), taosError.ErrStr)
						return
					}
					ErrorResponseWithMsg(c, 0xffff, err.Error())
					return
				}
				buffer.Reset()
				buffer.WriteString("insert into ")
				buffer.WriteString(tableName)
				buffer.WriteString(" values")
			}
			colBuffer.WriteTo(buffer)
		}
	}
	if buffer.Len() > prefixLength {
		s = log.GetLogNow(isDebug)
		err = async.GlobalAsync.TaosExecWithoutResult(taosConnect.TaosConnection, buffer.String())
		logger.Debugln("execute insert sql2 cost:", log.GetLogDuration(isDebug, s))
		if err != nil {
			taosError, is := err.(*tErrors.TaosError)
			if is {
				ErrorResponseWithMsg(c, int(taosError.Code), taosError.ErrStr)
				return
			}
			ErrorResponseWithMsg(c, 0xffff, err.Error())
			return
		}
	}
	buffer.Reset()
	buffer.Write(ExecHeader)
	buffer.WriteString(strconv.Itoa(rows))
	buffer.Write(ExecEnd)
	c.String(http.StatusOK, buffer.String())
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

package rest

import (
	"database/sql/driver"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/parser"
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
	"github.com/taosdata/taosadapter/v3/tools"
	"github.com/taosdata/taosadapter/v3/tools/csv"
	"github.com/taosdata/taosadapter/v3/tools/ctools"
	"github.com/taosdata/taosadapter/v3/tools/jsonbuilder"
	"github.com/taosdata/taosadapter/v3/tools/layout"
	"github.com/taosdata/taosadapter/v3/tools/pool"
	"github.com/taosdata/taosadapter/v3/tools/sqltype"
)

var logger = log.GetLogger("restful")

const StartTimeKey = "st"

type Restful struct {
	uploadReplacer *strings.Replacer
}

func (ctl *Restful) Init(r gin.IRouter) {
	ctl.uploadReplacer = strings.NewReplacer(
		"\\", "\\\\",
		"'", "\\'",
		"(", "\\(",
		")", "\\)",
	)
	api := r.Group("rest")
	api.Use(func(c *gin.Context) {
		if monitor.AllPaused() {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, "memory exceeds threshold")
			return
		}
	})
	api.POST("sql", setStartTime, CheckAuth, ctl.sql)
	api.POST("sql/:db", setStartTime, CheckAuth, ctl.sql)
	api.POST("sql/:db/vgid", CheckAuth, ctl.tableVgID)
	api.GET("login/:user/:password", ctl.des)
	api.POST("upload", CheckAuth, ctl.upload)
}

func setStartTime(c *gin.Context) {
	timing := c.Query("timing")
	if timing == "true" {
		c.Set(StartTimeKey, time.Now().UnixNano())
	}
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
	timeZone := c.Query("tz")
	location := time.UTC
	var err error
	if len(timeZone) != 0 {
		location, err = time.LoadLocation(timeZone)
		if err != nil {
			BadRequestResponseWithMsg(c, 0xffff, err.Error())
			return
		}
	}
	var reqID int64
	if reqIDStr := c.Query(config.ReqIDKey); len(reqIDStr) != 0 {
		if reqID, err = strconv.ParseInt(reqIDStr, 10, 64); err != nil {
			BadRequestResponseWithMsg(c, 0xffff, fmt.Sprintf("illegal param, req_id must be numeric %s", err.Error()))
			return
		}
	}
	if reqID == 0 {
		reqID = common.GetReqID()
	}
	c.Set(config.ReqIDKey, reqID)

	timeBuffer := make([]byte, 0, 30)
	DoQuery(c, db, func(builder *jsonbuilder.Stream, ts int64, precision int) {
		timeBuffer = timeBuffer[:0]
		switch precision {
		case common.PrecisionMilliSecond: // milli-second
			timeBuffer = time.Unix(0, ts*1e6).In(location).AppendFormat(timeBuffer, layout.LayoutMillSecond)
		case common.PrecisionMicroSecond: // micro-second
			timeBuffer = time.Unix(0, ts*1e3).In(location).AppendFormat(timeBuffer, layout.LayoutMicroSecond)
		case common.PrecisionNanoSecond: // nano-second
			timeBuffer = time.Unix(0, ts).In(location).AppendFormat(timeBuffer, layout.LayoutNanoSecond)
		default:
			panic("unknown precision")
		}
		builder.WriteString(string(timeBuffer))
	}, reqID)
}

type TDEngineRestfulResp struct {
	Code       int              `json:"code,omitempty"`
	Desc       string           `json:"desc,omitempty"`
	ColumnMeta [][]interface{}  `json:"column_meta,omitempty"`
	Data       [][]driver.Value `json:"data,omitempty"`
	Rows       int              `json:"rows,omitempty"`
}

func DoQuery(c *gin.Context, db string, timeFunc ctools.FormatTimeFunc, reqID int64) {
	var s time.Time
	isDebug := log.IsDebug()
	logger := logger.WithField(config.ReqIDKey, reqID)
	b, err := c.GetRawData()
	if err != nil {
		logger.WithError(err).Error("get request body error")
		BadRequestResponse(c, httperror.HTTP_INVALID_CONTENT_LENGTH)
		return
	}
	if len(b) == 0 {
		logger.Errorln("no msg got")
		BadRequestResponse(c, httperror.HTTP_NO_MSG_INPUT)
		return
	}
	sql := strings.TrimSpace(string(b))
	if len(sql) == 0 {
		logger.Errorln("no sql got")
		BadRequestResponse(c, httperror.HTTP_NO_SQL_INPUT)
		return
	}
	sqlType := monitor.RestRecordRequest(sql)
	c.Set("sql", sql)
	user := c.MustGet(UserKey).(string)
	password := c.MustGet(PasswordKey).(string)
	if isDebug {
		s = time.Now()
	}
	taosConnect, err := commonpool.GetConnection(user, password)
	if isDebug {
		logger.Debugln("connect server cost:", time.Since(s))
	}
	if err != nil {
		monitor.RestRecordResult(sqlType, false)
		logger.WithField("sql", sql).WithError(err).Error("connect server error")
		if tError, is := err.(*tErrors.TaosError); is {
			TaosErrorResponse(c, int(tError.Code), tError.ErrStr)
			return
		}
		CommonErrorResponse(c, err.Error())
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
			logger.Debugln("put connect cost:", time.Since(s))
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
		logger.Debugln("select db cost:", time.Since(s))
	}
	execute(c, logger, taosConnect.TaosConnection, sql, timeFunc, reqID, sqlType)
}

var (
	ExecHeader        = []byte(`{"code":0,"column_meta":[["affected_rows","INT",4]],"data":[[`)
	ExecEnd           = []byte(`]],"rows":1}`)
	ExecEndWithTiming = []byte(`]],"rows":1,"timing":`)
	Query2            = []byte(`{"code":0,"column_meta":[`)
	Query3            = []byte(`],"data":[`)
	Query4            = []byte(`],"rows":`)
	Timing            = []byte(`,"timing":`)
)

func execute(c *gin.Context, logger *logrus.Entry, taosConnect unsafe.Pointer, sql string, timeFormat ctools.FormatTimeFunc, reqID int64, sqlType sqltype.SqlType) {
	st, calculateTiming := c.Get(StartTimeKey)
	flushTiming := int64(0)
	isDebug := log.IsDebug()
	handler := async.GlobalAsync.HandlerPool.Get()
	defer async.GlobalAsync.HandlerPool.Put(handler)
	var s time.Time
	if isDebug {
		s = time.Now()
	}
	result, _ := async.GlobalAsync.TaosQuery(taosConnect, sql, handler, reqID)
	if isDebug {
		logger.Debugln("query cost:", time.Since(s))
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
				logger.Debugln("free result cost:", time.Since(s))
			}
		}
	}()
	res := result.Res
	code := wrapper.TaosError(res)
	if code != httperror.SUCCESS {
		monitor.RestRecordResult(sqlType, false)
		errStr := wrapper.TaosErrorStr(res)
		logger.WithFields(logrus.Fields{"sql": sql, "error_code": code & 0xffff, "error_msg": errStr}).Error("")
		TaosErrorResponse(c, code, errStr)
		return
	}
	monitor.RestRecordResult(sqlType, true)
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
		if calculateTiming {
			_, err = w.Write(ExecEndWithTiming)
			if err != nil {
				return
			}
			_, err = w.Write([]byte(strconv.FormatInt(time.Now().UnixNano()-st.(int64), 10)))
			if err != nil {
				return
			}
			_, err = w.Write([]byte{'}'})
			if err != nil {
				return
			}
			w.Flush()
		} else {
			_, err = w.Write(ExecEnd)
			if err != nil {
				return
			}
			w.Flush()
		}
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
		logger.WithField("sql", sql).WithError(err).Error("read column error")
		tError, ok := err.(*tErrors.TaosError)
		if ok {
			TaosErrorResponse(c, int(tError.Code), tError.ErrStr)
		} else {
			CommonErrorResponse(c, err.Error())
		}
		return
	}
	builder := jsonbuilder.BorrowStream(w)
	defer jsonbuilder.ReturnStream(builder)
	builder.WritePure(Query2)
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
	var tmpFlushTiming int64
	// // try flushing after parsing meta
	tmpFlushTiming, err = tryFlush(w, builder, calculateTiming)
	if err != nil {
		return
	}
	tmpFlushTiming += tmpFlushTiming
	total := 0
	builder.WritePure(Query3)
	precision := wrapper.TaosResultPrecision(res)
	fetched := false
	pHeaderList := make([]unsafe.Pointer, fieldsCount)
	pStartList := make([]unsafe.Pointer, fieldsCount)
	for {
		if config.Conf.RestfulRowLimit > -1 && total == config.Conf.RestfulRowLimit {
			break
		}
		if isDebug {
			s = time.Now()
		}
		result, _ = async.GlobalAsync.TaosFetchRawBlockA(res, handler)
		if isDebug {
			logger.Debugln("fetch_rows_a cost:", time.Since(s))
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
			lengthOffset := parser.RawBlockGetColumnLengthOffset(fieldsCount)
			tmpPHeader := tools.AddPointer(block, parser.RawBlockGetColDataOffset(fieldsCount))
			tmpPStart := tmpPHeader
			for column := 0; column < fieldsCount; column++ {
				colLength := *((*int32)(unsafe.Pointer(uintptr(block) + lengthOffset + uintptr(column)*parser.Int32Size)))
				if ctools.IsVarDataType(rowsHeader.ColTypes[column]) {
					pHeaderList[column] = tmpPHeader
					tmpPStart = tools.AddPointer(tmpPHeader, uintptr(4*blockSize))
					pStartList[column] = tmpPStart
				} else {
					pHeaderList[column] = tmpPHeader
					tmpPStart = tools.AddPointer(tmpPHeader, nullBitMapOffset)
					pStartList[column] = tmpPStart
				}
				tmpPHeader = tools.AddPointer(tmpPStart, uintptr(colLength))
			}

			for row := 0; row < result.N; row++ {
				builder.WriteArrayStart()
				for column := 0; column < fieldsCount; column++ {
					ctools.JsonWriteRawBlock(builder, rowsHeader.ColTypes[column], pHeaderList[column], pStartList[column], row, precision, timeFormat)
					if column != fieldsCount-1 {
						builder.WriteMore()
					}
				}
				// try flushing after parsing a row of data
				tmpFlushTiming, err = tryFlush(w, builder, calculateTiming)
				if err != nil {
					return
				}
				flushTiming += tmpFlushTiming
				builder.WriteArrayEnd()
				total += 1
				if config.Conf.RestfulRowLimit > -1 && total == config.Conf.RestfulRowLimit {
					break
				}
				if row != result.N-1 {
					builder.WriteMore()
				}
			}
		}
	}
	builder.WritePure(Query4)
	builder.WriteInt(total)
	if calculateTiming {
		builder.WritePure(Timing)
		builder.WriteInt64(time.Now().UnixNano() - st.(int64) - flushTiming)
	}
	builder.WriteObjectEnd()
	err = forceFlush(w, builder)
	if err != nil {
		logger.WithField("sql", sql).WithError(err).Error("force flush error")
	}
}

func tryFlush(w gin.ResponseWriter, builder *jsonbuilder.Stream, calculateTiming bool) (int64, error) {
	if builder.Buffered() > 16352 {
		err := builder.Flush()
		if err != nil {
			return 0, err
		}
		var s time.Time
		if calculateTiming {
			s = time.Now()
			w.Flush()
			return time.Now().Sub(s).Nanoseconds(), nil
		}
		w.Flush()
	}
	return 0, nil
}

func forceFlush(w gin.ResponseWriter, builder *jsonbuilder.Stream) error {
	err := builder.Flush()
	if err != nil {
		return err
	}
	w.Flush()
	return nil
}

const MAXSQLLength = 1024 * 1024 * 1

func (ctl *Restful) upload(c *gin.Context) {
	db := c.Query("db")
	if len(db) == 0 {
		BadRequestResponseWithMsg(c, 0xffff, "db required")
		return
	}
	table := c.Query("table")
	if len(table) == 0 {
		BadRequestResponseWithMsg(c, 0xffff, "table required")
		return
	}
	var reqID int64
	var err error
	if reqIDStr := c.Query(config.ReqIDKey); len(reqIDStr) != 0 {
		if reqID, err = strconv.ParseInt(reqIDStr, 10, 64); err != nil {
			BadRequestResponseWithMsg(c, 0xffff, fmt.Sprintf("illegal param, req_id must be numeric %s", err.Error()))
			return
		}
	}
	if reqID == 0 {
		reqID = common.GetReqID()
	}
	c.Set(config.ReqIDKey, reqID)

	buffer := pool.BytesPoolGet()
	defer pool.BytesPoolPut(buffer)
	colBuffer := pool.BytesPoolGet()
	defer pool.BytesPoolPut(colBuffer)
	isDebug := log.IsDebug()
	logger = logger.WithField("uri", "upload")
	buffer.WriteByte('`')
	buffer.WriteString(db)
	buffer.WriteByte('`')
	buffer.WriteByte('.')
	buffer.WriteByte('`')
	buffer.WriteString(table)
	buffer.WriteByte('`')
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
		logger.Debugln("connect cost:", log.GetLogDuration(isDebug, s))
	}

	if err != nil {
		logger.WithError(err).Error("connect server error")
		if tError, is := err.(*tErrors.TaosError); is {
			TaosErrorResponse(c, int(tError.Code), tError.ErrStr)
			return
		}
		CommonErrorResponse(c, err.Error())
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
			logger.Debugln("put connect cost:", log.GetLogDuration(isDebug, s))
		}
	}()
	s = log.GetLogNow(isDebug)
	result, err := async.GlobalAsync.TaosExec(taosConnect.TaosConnection, sql, func(ts int64, precision int) driver.Value {
		return ts
	}, reqID)
	logger.Debugln("describe table cost:", log.GetLogDuration(isDebug, s))
	if err != nil {
		taosError, is := err.(*tErrors.TaosError)
		if is {
			TaosErrorResponse(c, int(taosError.Code), taosError.ErrStr)
			return
		}
		CommonErrorResponse(c, err.Error())
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
		CommonErrorResponse(c, err.Error())
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
				CommonErrorResponse(c, err.Error())
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
				CommonErrorResponse(c, "column count not enough")
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
				err = async.GlobalAsync.TaosExecWithoutResult(taosConnect.TaosConnection, buffer.String(), reqID)
				logger.Debugln("execute insert sql1 cost:", log.GetLogDuration(isDebug, s))
				if err != nil {
					taosError, is := err.(*tErrors.TaosError)
					if is {
						TaosErrorResponse(c, int(taosError.Code), taosError.ErrStr)
						return
					}
					CommonErrorResponse(c, err.Error())
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
		err = async.GlobalAsync.TaosExecWithoutResult(taosConnect.TaosConnection, buffer.String(), reqID)
		logger.Debugln("execute insert sql2 cost:", log.GetLogDuration(isDebug, s))
		if err != nil {
			taosError, is := err.(*tErrors.TaosError)
			if is {
				TaosErrorResponse(c, int(taosError.Code), taosError.ErrStr)
				return
			}
			CommonErrorResponse(c, err.Error())
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
	if len(user) == 0 || len(user) > 24 || len(password) == 0 || len(password) > 24 {
		BadRequestResponse(c, httperror.HTTP_GEN_TAOSD_TOKEN_ERR)
		return
	}
	conn, err := commonpool.GetConnection(user, password)
	if err != nil {
		UnAuthResponse(c, httperror.TSDB_CODE_RPC_AUTH_FAILURE)
		return
	}
	conn.Put()
	token, err := EncodeDes(user, password)
	if err != nil {
		BadRequestResponse(c, httperror.HTTP_GEN_TAOSD_TOKEN_ERR)
		return
	}
	c.JSON(http.StatusOK, &Message{
		Code: 0,
		Desc: token,
	})
}

func (ctl *Restful) Close() {
}

func init() {
	r := &Restful{}
	controller.AddController(r)
}

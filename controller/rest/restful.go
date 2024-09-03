package rest

import (
	"database/sql/driver"
	"errors"
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
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/tools"
	"github.com/taosdata/taosadapter/v3/tools/csv"
	"github.com/taosdata/taosadapter/v3/tools/ctools"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
	"github.com/taosdata/taosadapter/v3/tools/jsonbuilder"
	"github.com/taosdata/taosadapter/v3/tools/layout"
	"github.com/taosdata/taosadapter/v3/tools/pool"
	"github.com/taosdata/taosadapter/v3/tools/sqltype"
)

var logger = log.GetLogger("RST")

const StartTimeKey = "st"
const LoggerKey = "logger"
const RequireTiming = "require_timing"

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
	api.POST("sql", prepareCtx, CheckAuth, ctl.sql)
	api.POST("sql/:db", prepareCtx, CheckAuth, ctl.sql)
	api.POST("sql/:db/vgid", prepareCtx, CheckAuth, ctl.tableVgID)
	api.GET("login/:user/:password", prepareCtx, ctl.des)
	api.POST("upload", prepareCtx, CheckAuth, ctl.upload)
}

func prepareCtx(c *gin.Context) {
	timing := c.Query("timing")
	if timing == "true" {
		c.Set(RequireTiming, true)
	}
	c.Set(StartTimeKey, time.Now().UnixNano())
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
		logger.Tracef("request:%s, client_ip:%s, req_id not set, generate new req_id: 0x%x", c.Request.RequestURI, c.ClientIP(), reqID)
	}
	c.Set(config.ReqIDKey, reqID)
	ctxLogger := logger.WithField(config.ReqIDKey, reqID)
	c.Set(LoggerKey, ctxLogger)
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
	logger := c.MustGet(LoggerKey).(*logrus.Entry)
	reqID := c.MustGet(config.ReqIDKey).(int64)
	if len(timeZone) != 0 {
		location, err = time.LoadLocation(timeZone)
		if err != nil {
			logger.Errorf("load location:%s error:%s", timeZone, err)
			BadRequestResponseWithMsg(c, logger, 0xffff, err.Error())
			return
		}
	}
	var returnObj bool
	if returnObjStr := c.Query("row_with_meta"); len(returnObjStr) != 0 {
		if returnObj, err = strconv.ParseBool(returnObjStr); err != nil {
			logger.Tracef("illegal param, row_with_meta must be boolean:%s", returnObjStr)
			BadRequestResponseWithMsg(c, logger, 0xffff, fmt.Sprintf("illegal param, row_with_meta must be boolean %s", err.Error()))
			return
		}
	}

	timeBuffer := make([]byte, 0, 30)
	DoQuery(c, db, func(builder *jsonbuilder.Stream, ts int64, precision int) {
		timeBuffer = timeBuffer[:0]
		switch precision {
		case common.PrecisionMilliSecond: // milli-second
			timeBuffer = time.Unix(ts/1e3, (ts%1e3)*1e6).In(location).AppendFormat(timeBuffer, layout.LayoutMillSecond)
		case common.PrecisionMicroSecond: // micro-second
			timeBuffer = time.Unix(ts/1e6, (ts%1e6)*1e3).In(location).AppendFormat(timeBuffer, layout.LayoutMicroSecond)
		case common.PrecisionNanoSecond: // nano-second
			timeBuffer = time.Unix(0, ts).In(location).AppendFormat(timeBuffer, layout.LayoutNanoSecond)
		default:
			logger.Errorf("unknown precision:%d", precision)
		}
		builder.WriteString(string(timeBuffer))
	}, reqID, returnObj, logger)
}

type TDEngineRestfulResp struct {
	Code       int              `json:"code,omitempty"`
	Desc       string           `json:"desc,omitempty"`
	ColumnMeta [][]interface{}  `json:"column_meta,omitempty"`
	Data       [][]driver.Value `json:"data,omitempty"`
	Rows       int              `json:"rows,omitempty"`
}

func DoQuery(c *gin.Context, db string, timeFunc ctools.FormatTimeFunc, reqID int64, returnObj bool, logger *logrus.Entry) {
	var s time.Time
	isDebug := log.IsDebug()
	b, err := c.GetRawData()
	if err != nil {
		logger.Errorf("get request body error, err:%s", err)
		BadRequestResponse(c, logger, httperror.HTTP_INVALID_CONTENT_LENGTH)
		return
	}
	if len(b) == 0 {
		logger.Error("no msg got")
		BadRequestResponse(c, logger, httperror.HTTP_NO_MSG_INPUT)
		return
	}
	sql := strings.TrimSpace(string(b))
	if len(sql) == 0 {
		logger.Error("no sql got")
		BadRequestResponse(c, logger, httperror.HTTP_NO_SQL_INPUT)
		return
	}
	logger.Debugf("request sql:%s", log.GetLogSql(sql))
	sqlType := monitor.RestRecordRequest(sql)
	c.Set("sql", sql)
	user := c.MustGet(UserKey).(string)
	password := c.MustGet(PasswordKey).(string)
	logger.Tracef("connect server, user:%s, pass:%s", user, password)
	ip := iptool.GetRealIP(c.Request)
	s = log.GetLogNow(isDebug)
	taosConnect, err := commonpool.GetConnection(user, password, ip)
	logger.Debugf("get connect, conn:%p, err:%v, cost:%s", taosConnect, err, log.GetLogDuration(isDebug, s))
	if err != nil {
		monitor.RestRecordResult(sqlType, false)
		logger.Errorf("connect server error,ip:%s, err:%s", ip, err)
		if errors.Is(err, commonpool.ErrWhitelistForbidden) {
			logger.Errorf("whitelist forbidden, ip:%s", ip)
			ForbiddenResponse(c, logger, commonpool.ErrWhitelistForbidden.Error())
			return
		}
		var tError *tErrors.TaosError
		if errors.As(err, &tError) {
			TaosErrorResponse(c, logger, int(tError.Code), tError.ErrStr)
			return
		}
		CommonErrorResponse(c, logger, err.Error())
		return
	}
	defer func() {
		s = log.GetLogNow(isDebug)
		logger.Trace("put connection")
		err := taosConnect.Put()
		if err != nil {
			panic(err)
		}
		logger.Debugf("put connect cost:%s", log.GetLogDuration(isDebug, s))
	}()

	if len(db) > 0 {
		// Attempt to select the database does not return even if there is an error
		// To avoid error reporting in the `create database` statement
		syncinterface.TaosSelectDB(taosConnect.TaosConnection, db, logger, isDebug)
	}
	execute(c, logger, isDebug, taosConnect.TaosConnection, sql, timeFunc, reqID, sqlType, returnObj)
}

var (
	ExecHeader           = []byte(`{"code":0,"column_meta":[["affected_rows","INT",4]],"data":[[`)
	ExecEnd              = []byte(`]],"rows":1}`)
	ExecEndWithTiming    = []byte(`]],"rows":1,"timing":`)
	ExecObjHeader        = []byte(`{"code":0,"column_meta":[["affected_rows","INT",4]],"data":[{"affected_rows":`)
	ExecObjEnd           = []byte(`}],"rows":1}`)
	ExecObjEndWithTiming = []byte(`}],"rows":1,"timing":`)
	Query2               = []byte(`{"code":0,"column_meta":[`)
	Query3               = []byte(`],"data":[`)
	Query4               = []byte(`],"rows":`)
	Timing               = []byte(`,"timing":`)
)

func execute(c *gin.Context, logger *logrus.Entry, isDebug bool, taosConnect unsafe.Pointer, sql string, timeFormat ctools.FormatTimeFunc, reqID int64, sqlType sqltype.SqlType, returnObj bool) {
	_, calculateTiming := c.Get(RequireTiming)
	st := c.MustGet(StartTimeKey)
	flushTiming := int64(0)
	handler := async.GlobalAsync.HandlerPool.Get()
	defer async.GlobalAsync.HandlerPool.Put(handler)
	result := async.GlobalAsync.TaosQuery(taosConnect, logger, isDebug, sql, handler, reqID)
	defer func() {
		if result != nil && result.Res != nil {
			syncinterface.FreeResult(result.Res, logger, isDebug)
		}
	}()
	res := result.Res
	code := wrapper.TaosError(res)
	if code != httperror.SUCCESS {
		monitor.RestRecordResult(sqlType, false)
		errStr := wrapper.TaosErrorStr(res)
		logger.Errorf("taos query error, qid:0x%x, code:%d, msg:%s, sql: %s", reqID, code, errStr, log.GetLogSql(sql))
		TaosErrorResponse(c, logger, code, errStr)
		return
	}
	monitor.RestRecordResult(sqlType, true)
	isUpdate := wrapper.TaosIsUpdateQuery(res)
	logger.Tracef("sql isUpdate:%t", isUpdate)
	w := c.Writer
	c.Header("Content-Type", "application/json; charset=utf-8")
	c.Header("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)
	if isUpdate {
		affectRows := wrapper.TaosAffectedRows(res)
		logger.Tracef("sql affectRows:%d", affectRows)
		var err error
		if returnObj {
			_, err = w.Write(ExecObjHeader)
		} else {
			_, err = w.Write(ExecHeader)
		}
		if err != nil {
			return
		}
		_, err = w.Write([]byte(strconv.Itoa(affectRows)))
		if err != nil {
			return
		}
		if calculateTiming {
			if returnObj {
				_, err = w.Write(ExecObjEndWithTiming)
			} else {
				_, err = w.Write(ExecEndWithTiming)
			}
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
			if returnObj {
				_, err = w.Write(ExecObjEnd)
			} else {
				_, err = w.Write(ExecEnd)
			}
			if err != nil {
				return
			}
			w.Flush()
		}
		return
	} else {
		if monitor.QueryPaused() {
			logger.Errorf("query memory exceeds threshold, qid:0x%x", reqID)
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, "query memory exceeds threshold")
			return
		}
	}
	fieldsCount := wrapper.TaosNumFields(res)
	logger.Tracef("get fieldsCount:%d", fieldsCount)
	rowsHeader, err := wrapper.ReadColumn(res, fieldsCount)
	if err != nil {
		logger.Errorf("read column error, error:%s, sql:%s", err, log.GetLogSql(sql))
		tError, ok := err.(*tErrors.TaosError)
		if ok {
			TaosErrorResponse(c, logger, int(tError.Code), tError.ErrStr)
		} else {
			CommonErrorResponse(c, logger, err.Error())
		}
		return
	}
	builder := jsonbuilder.BorrowStream(w)
	defer jsonbuilder.ReturnStream(builder)
	builder.WritePure(Query2)
	for i := 0; i < fieldsCount; i++ {
		logger.Tracef("write column meta to client, column:%d, name:%s, column_type:%s, column_len:%d", i, rowsHeader.ColNames[i], rowsHeader.TypeDatabaseName(i), rowsHeader.ColLength[i])
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
	logger.Tracef("get precision:%d", precision)
	fetched := false
	pHeaderList := make([]unsafe.Pointer, fieldsCount)
	pStartList := make([]unsafe.Pointer, fieldsCount)
	for {
		if config.Conf.RestfulRowLimit > -1 && total == config.Conf.RestfulRowLimit {
			break
		}
		result = async.GlobalAsync.TaosFetchRawBlockA(res, logger, isDebug, handler)
		if result.N == 0 {
			logger.Trace("fetch finished")
			break
		} else {
			if result.N < 0 {
				logger.Tracef("fetch error, result.N:%d", result.N)
				break
			}
			res = result.Res
			if fetched {
				builder.WriteMore()
			} else {
				fetched = true
			}
			logger.Tracef("get fetch result rows:%d", result.N)
			block := wrapper.TaosGetRawBlock(res)
			logger.Trace("start parse block")
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
				if returnObj {
					builder.WriteObjectStart()
				} else {
					builder.WriteArrayStart()
				}
				for column := 0; column < fieldsCount; column++ {
					if returnObj {
						builder.WriteObjectField(rowsHeader.ColNames[column])
					}
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
				if returnObj {
					builder.WriteObjectEnd()
				} else {
					builder.WriteArrayEnd()
				}
				total += 1
				if config.Conf.RestfulRowLimit > -1 && total == config.Conf.RestfulRowLimit {
					logger.Tracef("row limit %d reached", config.Conf.RestfulRowLimit)
					break
				}
				if row != result.N-1 {
					builder.WriteMore()
				}
			}
			logger.Trace("parse block finished")
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
		logger.Errorf("force flush error:%s", err)
	}
	logger.Trace("send response finished")
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
	logger := c.MustGet(LoggerKey).(*logrus.Entry)
	reqID := c.MustGet(config.ReqIDKey).(int64)
	db := c.Query("db")
	if len(db) == 0 {
		BadRequestResponseWithMsg(c, logger, 0xffff, "db required")
		return
	}
	table := c.Query("table")
	if len(table) == 0 {
		BadRequestResponseWithMsg(c, logger, 0xffff, "table required")
		return
	}

	buffer := pool.BytesPoolGet()
	defer pool.BytesPoolPut(buffer)
	colBuffer := pool.BytesPoolGet()
	defer pool.BytesPoolPut(colBuffer)
	isDebug := log.IsDebug()
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
	ip := iptool.GetRealIP(c.Request)
	logger.Trace("connect server")
	s := log.GetLogNow(isDebug)
	taosConnect, err := commonpool.GetConnection(user, password, ip)
	logger.Debugf("get connect, conn:%p, err:%v, cost:%s", taosConnect, err, log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.Errorf("connect server error, ip:%s, err: %s", ip, err)
		var tError *tErrors.TaosError
		if errors.Is(err, commonpool.ErrWhitelistForbidden) {
			ForbiddenResponse(c, logger, commonpool.ErrWhitelistForbidden.Error())
			return
		}
		if errors.As(err, &tError) {
			TaosErrorResponse(c, logger, int(tError.Code), tError.ErrStr)
			return
		}
		CommonErrorResponse(c, logger, err.Error())
		return
	}
	defer func() {
		s = log.GetLogNow(isDebug)
		logger.Trace("put connection")
		err := taosConnect.Put()
		if err != nil {
			panic(err)
		}
		logger.Debugf("put connect cost:%s", log.GetLogDuration(isDebug, s))
	}()
	s = log.GetLogNow(isDebug)
	logger.Tracef("exec sql: %s", sql)
	result, err := async.GlobalAsync.TaosExec(taosConnect.TaosConnection, logger, isDebug, sql, func(ts int64, precision int) driver.Value {
		return ts
	}, reqID)
	logger.Debugf("describe table cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.Errorf("exec describe sql error: %s", err)
		taosError, is := err.(*tErrors.TaosError)
		if is {
			TaosErrorResponse(c, logger, int(taosError.Code), taosError.ErrStr)
			return
		}
		CommonErrorResponse(c, logger, err.Error())
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
		logger.Errorf("get multi part reader error, err:%s", err)
		CommonErrorResponse(c, logger, err.Error())
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
				logger.Errorf("get next part error, err:%s", err)
				CommonErrorResponse(c, logger, err.Error())
				return
			}
		}
		if part.FormName() != "data" {
			continue
		}
		csvReader := csv.NewReader(part)
		csvReader.ReuseRecord = true
		logger.Trace("read csv data")
		for {
			record, err := csvReader.Read()
			if err != nil {
				if err == io.EOF {
					logger.Trace("read csv finished")
					break
				}
				logger.Errorf("read csv error,err: %s", err)
				CommonErrorResponse(c, logger, err.Error())
				return
			}
			if len(record) < columnCount {
				logger.Errorf("column count not enough got %d want %d", len(record), columnCount)
				CommonErrorResponse(c, logger, "column count not enough")
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
				insertSql := buffer.String()
				logger.Tracef("exec insert sql: %s", insertSql)
				s = log.GetLogNow(isDebug)
				err = async.GlobalAsync.TaosExecWithoutResult(taosConnect.TaosConnection, logger, isDebug, insertSql, reqID)
				logger.Debugf("execute insert sql cost:%s", log.GetLogDuration(isDebug, s))
				if err != nil {
					logger.Errorf("exec insert sql error:%s", err)
					taosError, is := err.(*tErrors.TaosError)
					if is {
						TaosErrorResponse(c, logger, int(taosError.Code), taosError.ErrStr)
						return
					}
					CommonErrorResponse(c, logger, err.Error())
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
		insertSql := buffer.String()
		logger.Tracef("exec insert sql:%s", insertSql)
		s = log.GetLogNow(isDebug)
		err = async.GlobalAsync.TaosExecWithoutResult(taosConnect.TaosConnection, logger, isDebug, insertSql, reqID)
		logger.Debugf("execute insert sql cost:%s", log.GetLogDuration(isDebug, s))
		if err != nil {
			logger.Debugf("execute insert sql cost:%s", log.GetLogDuration(isDebug, s))
			taosError, is := err.(*tErrors.TaosError)
			if is {
				TaosErrorResponse(c, logger, int(taosError.Code), taosError.ErrStr)
				return
			}
			CommonErrorResponse(c, logger, err.Error())
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
	logger := c.MustGet(LoggerKey).(*logrus.Entry)
	if len(user) == 0 || len(user) > 24 || len(password) == 0 || len(password) > 24 {
		logger.Errorf("user or password length error,user length: %d,password length: %d", len(user), len(password))
		BadRequestResponse(c, logger, httperror.HTTP_GEN_TAOSD_TOKEN_ERR)
		return
	}
	logger.Tracef("get connection")
	ip := iptool.GetRealIP(c.Request)
	s := log.GetLogNow(log.IsDebug())
	conn, err := commonpool.GetConnection(user, password, ip)
	logger.Debugf("get connect, conn:%p, err:%v, cost:%s", conn, err, log.GetLogDuration(log.IsDebug(), s))
	if err != nil {
		logger.Errorf("get connection error, ip:%s, err:%s", ip, err)
		if errors.Is(err, commonpool.ErrWhitelistForbidden) {
			ForbiddenResponse(c, logger, commonpool.ErrWhitelistForbidden.Error())
			return
		}
		UnAuthResponse(c, logger, httperror.TSDB_CODE_RPC_AUTH_FAILURE)
		return
	}
	conn.Put()
	token, err := EncodeDes(user, password)
	if err != nil {
		logger.Errorf("encode token error, err:%s", err)
		BadRequestResponse(c, logger, httperror.HTTP_GEN_TAOSD_TOKEN_ERR)
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

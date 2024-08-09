package async

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v3/common"
	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/thread"
)

var FetchRowError = errors.New("fetch row error")
var GlobalAsync *Async

type Async struct {
	HandlerPool *HandlerPool
}

func NewAsync(handlerPool *HandlerPool) *Async {
	return &Async{HandlerPool: handlerPool}
}

func (a *Async) TaosExec(taosConnect unsafe.Pointer, logger *logrus.Entry, isDebug bool, sql string, timeFormat wrapper.FormatTimeFunc, reqID int64) (*ExecResult, error) {
	handler := a.HandlerPool.Get()
	defer a.HandlerPool.Put(handler)
	result := a.TaosQuery(taosConnect, logger, isDebug, sql, handler, reqID)
	var s time.Time
	defer func() {
		if result != nil && result.Res != nil {
			tool.FreeResult(result.Res, logger, isDebug)
		}
	}()
	res := result.Res
	code := wrapper.TaosError(res)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosErrorStr(res)
		logger.Traceln("taos query error code:", code, " err:", errStr)
		return nil, tErrors.NewError(code, errStr)
	}
	isUpdate := wrapper.TaosIsUpdateQuery(res)
	logger.Traceln("get isUpdate:", isUpdate)
	execResult := &ExecResult{}
	if isUpdate {
		affectRows := wrapper.TaosAffectedRows(res)
		logger.Traceln("get affectRows:", affectRows)
		execResult.AffectedRows = affectRows
		return execResult, nil
	}
	fieldsCount := wrapper.TaosNumFields(res)
	logger.Traceln("get fieldsCount:", fieldsCount)
	execResult.FieldCount = fieldsCount
	rowsHeader, err := wrapper.ReadColumn(res, fieldsCount)
	if err != nil {
		return nil, err
	}
	execResult.Header = rowsHeader
	precision := wrapper.TaosResultPrecision(res)
	logger.Traceln("get precision:", precision)
	for {
		result = a.TaosFetchRowsA(res, logger, isDebug, handler)
		logger.Trace("get fetch result rows:", result.N)
		if result.N == 0 {
			logger.Traceln("fetch finished")
			return execResult, nil
		} else {
			res = result.Res
			for i := 0; i < result.N; i++ {
				var row unsafe.Pointer
				logger.Traceln("get thread lock for fetch row,row: ", i)
				s = log.GetLogNow(isDebug)
				thread.Lock()
				logger.Debugln("get thread lock for fetch row cost:", log.GetLogDuration(isDebug, s))
				s = log.GetLogNow(isDebug)
				logger.Traceln("start fetch row")
				row = wrapper.TaosFetchRow(res)
				logger.Debugln("taos_fetch_row cost:", log.GetLogDuration(isDebug, s))
				thread.Unlock()
				logger.Traceln("start fetch lengths")
				lengths := wrapper.FetchLengths(res, len(rowsHeader.ColNames))
				logger.Traceln("fetch lengths:", lengths)
				values := make([]driver.Value, len(rowsHeader.ColNames))
				for j := range rowsHeader.ColTypes {
					if row == nil {
						logger.Error("fetch row error, row is nil")
						return nil, FetchRowError
					}
					v := wrapper.FetchRow(row, j, rowsHeader.ColTypes[j], lengths[j], precision, timeFormat)
					logger.Tracef("get column %d value: %v\n", j, v)
					if vv, is := v.([]byte); is {
						v = json.RawMessage(vv)
					}
					values[j] = v
				}
				execResult.Data = append(execResult.Data, values)
			}
		}
	}
}

func (a *Async) TaosQuery(taosConnect unsafe.Pointer, logger *logrus.Entry, isDebug bool, sql string, handler *Handler, reqID int64) *Result {
	if reqID == 0 {
		reqID = common.GetReqID()
		logger.Tracef("reqID is 0, generate a new one: 0x%x\n", reqID)
		logger = logger.WithField(config.ReqIDKey, reqID)
	}
	logger.Tracef("get thread lock for taos_query_a")
	s := log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("get thread lock for taos_query_a cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	logger.Traceln("start taos_query_a, sql:", sql)
	wrapper.TaosQueryAWithReqID(taosConnect, sql, handler.Handler, reqID)
	logger.Debugln("taos_query_a cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	logger.Traceln("wait for query result")
	s = log.GetLogNow(isDebug)
	r := <-handler.Caller.QueryResult
	logger.Debugln("wait for query result cost:", log.GetLogDuration(isDebug, s))
	return r
}

func (a *Async) TaosFetchRowsA(res unsafe.Pointer, logger *logrus.Entry, isDebug bool, handler *Handler) *Result {
	logger.Traceln("get thread lock for fetch_rows_a")
	s := log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("get thread lock for fetch_rows_a cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	wrapper.TaosFetchRowsA(res, handler.Handler)
	logger.Debugln("taos_fetch_rows_a cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	logger.Traceln("wait for fetch rows result")
	s = log.GetLogNow(isDebug)
	r := <-handler.Caller.FetchResult
	logger.Debugln("wait for fetch rows result cost:", log.GetLogDuration(isDebug, s))
	return r
}

func (a *Async) TaosFetchRawBlockA(res unsafe.Pointer, logger *logrus.Entry, isDebug bool, handler *Handler) *Result {
	s := log.GetLogNow(isDebug)
	logger.Traceln("get thread lock for fetch_raw_block_a")
	thread.Lock()
	logger.Debugln("get thread lock for fetch_raw_block_a cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	logger.Traceln("start fetch_raw_block_a")
	wrapper.TaosFetchRawBlockA(res, handler.Handler)
	logger.Debugln("taos_fetch_raw_block_a cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	logger.Traceln("wait for fetch raw block result")
	s = log.GetLogNow(isDebug)
	r := <-handler.Caller.FetchResult
	logger.Debugln("wait for fetch raw block result cost:", log.GetLogDuration(isDebug, s))
	return r
}

type ExecResult struct {
	AffectedRows int
	FieldCount   int
	Header       *wrapper.RowsHeader
	Data         [][]driver.Value
}

func (a *Async) TaosExecWithoutResult(taosConnect unsafe.Pointer, logger *logrus.Entry, isDebug bool, sql string, reqID int64) error {
	handler := a.HandlerPool.Get()
	defer a.HandlerPool.Put(handler)
	result := a.TaosQuery(taosConnect, logger, isDebug, sql, handler, reqID)
	defer func() {
		if result != nil && result.Res != nil {
			tool.FreeResult(result.Res, logger, isDebug)
		}
	}()
	res := result.Res
	code := wrapper.TaosError(res)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosErrorStr(res)
		logger.Traceln("taos query error code:", code, " err:", errStr)
		return tErrors.NewError(code, errStr)
	}
	return nil
}

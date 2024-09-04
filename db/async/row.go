package async

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"
	"unsafe"

	"github.com/sirupsen/logrus"
	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/thread"
	"github.com/taosdata/taosadapter/v3/tools/generator"
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
			syncinterface.FreeResult(result.Res, logger, isDebug)
		}
	}()
	res := result.Res
	code := wrapper.TaosError(res)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosErrorStr(res)
		logger.Tracef("taos query error, code:%d, msg:%s", code, errStr)
		return nil, tErrors.NewError(code, errStr)
	}
	logger.Tracef("get query result, res:%p", res)
	isUpdate := wrapper.TaosIsUpdateQuery(res)
	logger.Tracef("get isUpdate:%t", isUpdate)
	execResult := &ExecResult{}
	if isUpdate {
		affectRows := wrapper.TaosAffectedRows(res)
		logger.Tracef("get affectRows:%d", affectRows)
		execResult.AffectedRows = affectRows
		return execResult, nil
	}
	fieldsCount := wrapper.TaosNumFields(res)
	logger.Tracef("get fieldsCount:%d", fieldsCount)
	execResult.FieldCount = fieldsCount
	rowsHeader, err := wrapper.ReadColumn(res, fieldsCount)
	if err != nil {
		logger.Errorf("read column error, error:%s", err)
		return nil, err
	}
	execResult.Header = rowsHeader
	precision := wrapper.TaosResultPrecision(res)
	logger.Tracef("get precision:%d", precision)
	for {
		result = a.TaosFetchRowsA(res, logger, isDebug, handler)
		logger.Tracef("get fetch result, rows:%d", result.N)
		if result.N == 0 {
			logger.Trace("fetch finished")
			return execResult, nil
		} else {
			res = result.Res
			for i := 0; i < result.N; i++ {
				var row unsafe.Pointer
				logger.Tracef("get thread lock for fetch row, row:%d", i)
				s = log.GetLogNow(isDebug)
				thread.Lock()
				logger.Debugf("get thread lock for fetch row cost:%s", log.GetLogDuration(isDebug, s))
				s = log.GetLogNow(isDebug)
				row = wrapper.TaosFetchRow(res)
				logger.Debugf("taos_fetch_row finish, row:%p, cost:%s", row, log.GetLogDuration(isDebug, s))
				thread.Unlock()
				lengths := wrapper.FetchLengths(res, len(rowsHeader.ColNames))
				logger.Tracef("fetch lengths:%d", lengths)
				values := make([]driver.Value, len(rowsHeader.ColNames))
				for j := range rowsHeader.ColTypes {
					if row == nil {
						logger.Error("fetch row error, row is nil")
						return nil, FetchRowError
					}
					v := wrapper.FetchRow(row, j, rowsHeader.ColTypes[j], lengths[j], precision, timeFormat)
					if vv, is := v.([]byte); is {
						v = json.RawMessage(vv)
					}
					values[j] = v
				}
				logger.Tracef("get data, %v", values)
				execResult.Data = append(execResult.Data, values)
			}
		}
	}
}

func (a *Async) TaosQuery(taosConnect unsafe.Pointer, logger *logrus.Entry, isDebug bool, sql string, handler *Handler, reqID int64) *Result {
	logger.Tracef("call taos_query_a, conn:%p, qid:0x%x, sql:%s", taosConnect, reqID, log.GetLogSql(sql))
	if reqID == 0 {
		reqID = generator.GetReqID()
		logger.Tracef("reqID is 0, generate a new one:0x%x", reqID)
		logger = logger.WithField(config.ReqIDKey, reqID)
	}
	s := log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugf("get thread lock for taos_query_a cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	wrapper.TaosQueryAWithReqID(taosConnect, sql, handler.Handler, reqID)
	logger.Debugf("taos_query_a finish, cost:%s", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	logger.Trace("wait for query result")
	s = log.GetLogNow(isDebug)
	r := <-handler.Caller.QueryResult
	logger.Debugf("get query result, res:%p, n:%d, cost:%s", r.Res, r.N, log.GetLogDuration(isDebug, s))
	return r
}

func (a *Async) TaosFetchRowsA(res unsafe.Pointer, logger *logrus.Entry, isDebug bool, handler *Handler) *Result {
	logger.Tracef("call taos_fetch_rows_a, res:%p", res)
	s := log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugf("get thread lock for fetch_rows_a cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	wrapper.TaosFetchRowsA(res, handler.Handler)
	logger.Debugf("taos_fetch_rows_a finish, cost:%s", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	logger.Trace("wait for fetch rows result")
	s = log.GetLogNow(isDebug)
	r := <-handler.Caller.FetchResult
	logger.Debugf("get fetch rows result finish, res:%p, n:%d, cost:%s", r.Res, r.N, log.GetLogDuration(isDebug, s))
	return r
}

func (a *Async) TaosFetchRawBlockA(res unsafe.Pointer, logger *logrus.Entry, isDebug bool, handler *Handler) *Result {
	logger.Tracef("call taos_fetch_raw_block_a, res:%p", res)
	s := log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugf("get thread lock for fetch_raw_block_a cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	logger.Trace("start fetch_raw_block_a")
	wrapper.TaosFetchRawBlockA(res, handler.Handler)
	logger.Debugf("taos_fetch_raw_block_a finish, cost:%s", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	logger.Trace("wait for fetch raw block result")
	s = log.GetLogNow(isDebug)
	r := <-handler.Caller.FetchResult
	logger.Debugf("get fetch raw block result, res:%p, n:%d, cost:%s", r.Res, r.N, log.GetLogDuration(isDebug, s))
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
			syncinterface.FreeResult(result.Res, logger, isDebug)
		}
	}()
	res := result.Res
	code := wrapper.TaosError(res)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosErrorStr(res)
		logger.Tracef("taos query error, code:%d, msg:%s", code, errStr)
		return tErrors.NewError(code, errStr)
	}
	return nil
}

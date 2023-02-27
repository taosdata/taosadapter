package async

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"unsafe"

	"github.com/taosdata/driver-go/v3/common"
	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/httperror"
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

func (a *Async) TaosExec(taosConnect unsafe.Pointer, sql string, timeFormat wrapper.FormatTimeFunc, reqID int64) (*ExecResult, error) {
	handler := a.HandlerPool.Get()
	defer a.HandlerPool.Put(handler)
	result, err := a.TaosQuery(taosConnect, sql, handler, reqID)
	defer func() {
		if result != nil && result.Res != nil {
			thread.Lock()
			wrapper.TaosFreeResult(result.Res)
			thread.Unlock()
		}
	}()
	if err != nil {
		return nil, err
	}
	res := result.Res
	code := wrapper.TaosError(res)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosErrorStr(res)
		return nil, tErrors.NewError(code, errStr)
	}
	isUpdate := wrapper.TaosIsUpdateQuery(res)
	execResult := &ExecResult{}
	if isUpdate {
		affectRows := wrapper.TaosAffectedRows(res)
		execResult.AffectedRows = affectRows
		return execResult, nil
	}
	fieldsCount := wrapper.TaosNumFields(res)
	execResult.FieldCount = fieldsCount
	var rowsHeader *wrapper.RowsHeader
	rowsHeader, err = wrapper.ReadColumn(res, fieldsCount)
	if err != nil {
		return nil, err
	}
	execResult.Header = rowsHeader
	precision := wrapper.TaosResultPrecision(res)
	for {
		result, err = a.TaosFetchRowsA(res, handler)
		if err != nil {
			return nil, err
		}
		if result.N == 0 {
			return execResult, nil
		} else {
			res = result.Res
			for i := 0; i < result.N; i++ {
				var row unsafe.Pointer
				thread.Lock()
				row = wrapper.TaosFetchRow(res)
				thread.Unlock()
				lengths := wrapper.FetchLengths(res, len(rowsHeader.ColNames))
				values := make([]driver.Value, len(rowsHeader.ColNames))
				for j := range rowsHeader.ColTypes {
					if row == nil {
						return nil, FetchRowError
					}
					v := wrapper.FetchRow(row, j, rowsHeader.ColTypes[j], lengths[j], precision, timeFormat)
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

func (a *Async) TaosQuery(taosConnect unsafe.Pointer, sql string, handler *Handler, reqID int64) (*Result, error) {
	if reqID == 0 {
		reqID = common.GetReqID()
	}
	thread.Lock()
	wrapper.TaosQueryAWithReqID(taosConnect, sql, handler.Handler, reqID)
	thread.Unlock()
	r := <-handler.Caller.QueryResult
	return r, nil
}

func (a *Async) TaosFetchRowsA(res unsafe.Pointer, handler *Handler) (*Result, error) {
	thread.Lock()
	wrapper.TaosFetchRowsA(res, handler.Handler)
	thread.Unlock()
	r := <-handler.Caller.FetchResult
	return r, nil
}

func (a *Async) TaosFetchRawBlockA(res unsafe.Pointer, handler *Handler) (*Result, error) {
	thread.Lock()
	wrapper.TaosFetchRawBlockA(res, handler.Handler)
	thread.Unlock()
	r := <-handler.Caller.FetchResult
	return r, nil
}

type ExecResult struct {
	AffectedRows int
	FieldCount   int
	Header       *wrapper.RowsHeader
	Data         [][]driver.Value
}

func (a *Async) TaosExecWithoutResult(taosConnect unsafe.Pointer, sql string, reqID int64) error {
	handler := a.HandlerPool.Get()
	defer a.HandlerPool.Put(handler)
	result, err := a.TaosQuery(taosConnect, sql, handler, reqID)
	defer func() {
		if result != nil && result.Res != nil {
			thread.Lock()
			wrapper.TaosFreeResult(result.Res)
			thread.Unlock()
		}
	}()
	if err != nil {
		return err
	}
	res := result.Res
	code := wrapper.TaosError(res)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosErrorStr(res)
		return tErrors.NewError(code, errStr)
	}
	return nil
}

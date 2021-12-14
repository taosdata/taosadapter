package async

import (
	"database/sql/driver"
	"errors"
	"unsafe"

	tErrors "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/httperror"
	"github.com/taosdata/taosadapter/thread"
)

var FetchRowError = errors.New("fetch row error")
var GlobalAsync *Async

type Async struct {
	handlerPool *HandlerPool
}

func NewAsync(handlerPool *HandlerPool) *Async {
	return &Async{handlerPool: handlerPool}
}

func (a *Async) TaosExec(taosConnect unsafe.Pointer, sql string, timeFormat wrapper.FormatTimeFunc) (*ExecResult, error) {
	handler := a.handlerPool.Get()
	defer a.handlerPool.Put(handler)
	result, err := a.TaosQuery(taosConnect, sql, handler)
	defer func() {
		if result != nil && result.res != nil {
			thread.Lock()
			wrapper.TaosFreeResult(result.res)
			thread.Unlock()
		}
	}()
	if err != nil {
		return nil, err
	}
	res := result.res
	code := wrapper.TaosError(res)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosErrorStr(res)
		return nil, tErrors.NewError(code, errStr)
	}
	var fieldsCount int
	fieldsCount = wrapper.TaosNumFields(res)
	execResult := &ExecResult{FieldCount: fieldsCount}
	if fieldsCount == 0 {
		var affectRows int
		affectRows = wrapper.TaosAffectedRows(res)
		execResult.AffectedRows = affectRows
		return execResult, nil
	}
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
		if result.n == 0 {
			return execResult, nil
		} else {
			res = result.res
			for i := 0; i < result.n; i++ {
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
					values[j] = wrapper.FetchRow(row, j, rowsHeader.ColTypes[j], lengths[j], precision, timeFormat)
				}
				execResult.Data = append(execResult.Data, values)
			}
		}
	}
}

func (a *Async) TaosQuery(taosConnect unsafe.Pointer, sql string, handler *Handler) (*Result, error) {
	thread.Lock()
	wrapper.TaosQueryA(taosConnect, sql, handler.Handler)
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

type ExecResult struct {
	AffectedRows int
	FieldCount   int
	Header       *wrapper.RowsHeader
	Data         [][]driver.Value
}

func (a *Async) TaosExecWithoutResult(taosConnect unsafe.Pointer, sql string) error {
	handler := a.handlerPool.Get()
	defer a.handlerPool.Put(handler)
	result, err := a.TaosQuery(taosConnect, sql, handler)
	defer func() {
		if result != nil && result.res != nil {
			thread.Lock()
			wrapper.TaosFreeResult(result.res)
			thread.Unlock()
		}
	}()
	if err != nil {
		return err
	}
	res := result.res
	code := wrapper.TaosError(res)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosErrorStr(res)
		return tErrors.NewError(code, errStr)
	}
	return nil
}

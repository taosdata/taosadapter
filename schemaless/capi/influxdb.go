package capi

import (
	"strings"
	"unsafe"

	tErrors "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/httperror"
)

type Result struct {
	SuccessCount int
	FailCount    int
	ErrorList    []string
}

func InsertInfluxdb(taosConnect unsafe.Pointer, data []byte, db, precision string) (*Result, error) {
	code := wrapper.TaosSelectDB(taosConnect, db)
	if code != httperror.SUCCESS {
		return nil, tErrors.GetError(code)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	result := wrapper.TaosSchemalessInsert(taosConnect, lines, wrapper.InfluxDBLineProtocol, precision)
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		return nil, &tErrors.TaosError{
			Code:   int32(code) & 0xffff,
			ErrStr: errStr,
		}
	}
	successCount := wrapper.TaosAffectedRows(result)
	failCount := len(lines) - successCount
	r := &Result{
		SuccessCount: successCount,
		FailCount:    failCount,
		ErrorList:    nil,
	}
	wrapper.TaosFreeResult(result)
	return r, nil
}

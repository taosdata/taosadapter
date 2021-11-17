package capi

import (
	"strings"
	"unsafe"

	tErrors "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
)

type Result struct {
	SuccessCount int
	FailCount    int
	ErrorList    []string
}

func InsertInfluxdb(taosConnect unsafe.Pointer, data []byte, db, precision string) (*Result, error) {
	err := selectDB(taosConnect, db)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	locker.Lock()
	result := wrapper.TaosSchemalessInsert(taosConnect, lines, wrapper.InfluxDBLineProtocol, precision)
	locker.Unlock()
	code := wrapper.TaosError(result)
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

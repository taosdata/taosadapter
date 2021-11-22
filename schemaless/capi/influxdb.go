package capi

import (
	"strings"
	"unsafe"

	tErrors "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/schemaless/proto"
)

func InsertInfluxdb(taosConnect unsafe.Pointer, data []byte, db, precision string) (*proto.InfluxResult, error) {
	err := SelectDB(taosConnect, db)
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
		return nil, tErrors.NewError(code, errStr)
	}
	successCount := wrapper.TaosAffectedRows(result)
	failCount := len(lines) - successCount
	r := &proto.InfluxResult{
		SuccessCount: successCount,
		FailCount:    failCount,
		ErrorList:    make([]string, len(lines)),
	}
	wrapper.TaosFreeResult(result)
	return r, nil
}

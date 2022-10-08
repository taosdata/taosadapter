package capi

import (
	"strings"
	"unsafe"

	tErrors "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/db/tool"
	"github.com/taosdata/taosadapter/schemaless/proto"
	"github.com/taosdata/taosadapter/thread"
)

func InsertInfluxdb(taosConnect unsafe.Pointer, data []byte, db, precision string) (*proto.InfluxResult, error) {
	err := tool.SelectDB(taosConnect, db)
	if err != nil {
		return nil, err
	}
	thread.Lock()
	lines, result := wrapper.TaosSchemalessInsertRaw(taosConnect, strings.TrimSpace(string(data)), wrapper.InfluxDBLineProtocol, precision)
	thread.Unlock()
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		thread.Lock()
		wrapper.TaosFreeResult(result)
		thread.Unlock()
		return nil, tErrors.NewError(code, errStr)
	}
	successCount := wrapper.TaosAffectedRows(result)
	failCount := int(lines) - successCount
	r := &proto.InfluxResult{
		SuccessCount: successCount,
		FailCount:    failCount,
		ErrorList:    make([]string, lines),
	}
	thread.Lock()
	wrapper.TaosFreeResult(result)
	thread.Unlock()
	return r, nil
}

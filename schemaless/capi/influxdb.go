package capi

import (
	"strings"
	"unsafe"

	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/schemaless/proto"
	"github.com/taosdata/taosadapter/v3/thread"
)

func InsertInfluxdb(conn unsafe.Pointer, data []byte, db, precision string) (*proto.InfluxResult, error) {
	err := tool.SelectDB(conn, db)
	if err != nil {
		return nil, err
	}

	d := strings.TrimSpace(string(data))

	thread.Lock()
	rows, result := wrapper.TaosSchemalessInsertRaw(conn, d, wrapper.InfluxDBLineProtocol, precision)
	thread.Unlock()

	defer wrapper.TaosFreeResult(result)

	if code := wrapper.TaosError(result); code != 0 {
		return nil, tErrors.NewError(code, wrapper.TaosErrorStr(result))
	}
	successCount := wrapper.TaosAffectedRows(result)
	failCount := int(rows) - successCount
	r := &proto.InfluxResult{
		SuccessCount: successCount,
		FailCount:    failCount,
		ErrorList:    make([]string, rows), // todo
	}

	return r, nil
}

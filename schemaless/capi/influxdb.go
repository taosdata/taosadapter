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

// InsertInfluxdb
// Deprecated
func InsertInfluxdb(taosConnect unsafe.Pointer, data []byte, db, precision string) (*proto.InfluxResult, error) {
	return insertInfluxdb(taosConnect, data, db, precision, false)
}

func InsertInfluxdbRaw(taosConnect unsafe.Pointer, data []byte, db, precision string) (*proto.InfluxResult, error) {
	return insertInfluxdb(taosConnect, data, db, precision, true)
}

func insertInfluxdb(conn unsafe.Pointer, data []byte, db, precision string, raw bool) (*proto.InfluxResult, error) {
	err := tool.SelectDB(conn, db)
	if err != nil {
		return nil, err
	}

	d := strings.TrimSpace(string(data))

	var rows int32
	var result unsafe.Pointer

	thread.Lock()
	if raw {
		rows, result = wrapper.TaosSchemalessInsertRaw(conn, d, wrapper.InfluxDBLineProtocol, precision)
	} else {
		lines := strings.Split(d, "\n")
		rows = int32(len(lines))
		result = wrapper.TaosSchemalessInsert(conn, lines, wrapper.InfluxDBLineProtocol, precision)
	}
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

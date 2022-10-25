package inserter

import (
	"unsafe"

	"github.com/taosdata/taosadapter/v3/schemaless/capi"
	"github.com/taosdata/taosadapter/v3/schemaless/proto"
)

func InsertInfluxdb(taosConnect unsafe.Pointer, data []byte, db, precision string) (*proto.InfluxResult, error) {
	return capi.InsertInfluxdb(taosConnect, data, db, precision)
}

func InsertInfluxdbRaw(conn unsafe.Pointer, data []byte, db, precision string) (*proto.InfluxResult, error) {
	return capi.InsertInfluxdbRaw(conn, data, db, precision)
}

func InsertOpentsdbJson(taosConnect unsafe.Pointer, data []byte, db string) error {
	return capi.InsertOpentsdbJson(taosConnect, data, db)
}

func InsertOpentsdbJsonRaw(conn unsafe.Pointer, data []byte, db string) error {
	return capi.InsertOpentsdbJsonRaw(conn, data, db)
}

func InsertOpentsdbTelnet(taosConnect unsafe.Pointer, data, db string) error {
	return capi.InsertOpentsdbTelnetBatch(taosConnect, []string{data}, db)
}

func InsertOpentsdbTelnetBatch(taosConnect unsafe.Pointer, data []string, db string) error {
	return capi.InsertOpentsdbTelnetBatch(taosConnect, data, db)
}

func InsertOpentsdbTelnetBatchRaw(conn unsafe.Pointer, data []string, db string) error {
	return capi.InsertOpentsdbTelnetBatchRaw(conn, data, db)
}

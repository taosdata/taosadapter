package inserter

import (
	"unsafe"

	"github.com/taosdata/taosadapter/schemaless/capi"
	"github.com/taosdata/taosadapter/schemaless/proto"
)

func InsertInfluxdb(taosConnect unsafe.Pointer, data []byte, db, precision string) (*proto.InfluxResult, error) {
	return capi.InsertInfluxdb(taosConnect, data, db, precision)
}

func InsertOpentsdbJson(taosConnect unsafe.Pointer, data []byte, db string) error {
	return capi.InsertOpentsdbJson(taosConnect, data, db)
}

func InsertOpentsdbTelnet(taosConnect unsafe.Pointer, data, db string) error {
	return capi.InsertOpentsdbTelnet(taosConnect, data, db)
}

func InsertOpentsdbTelnetBatch(taosConnect unsafe.Pointer, data []string, db string) error {
	return capi.InsertOpentsdbTelnetBatch(taosConnect, data, db)
}

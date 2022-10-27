package inserter

import (
	"unsafe"

	"github.com/taosdata/taosadapter/v3/schemaless/capi"
	"github.com/taosdata/taosadapter/v3/schemaless/proto"
)

func InsertInfluxdb(taosConnect unsafe.Pointer, data []byte, db, precision string) (*proto.InfluxResult, error) {
	return capi.InsertInfluxdb(taosConnect, data, db, precision)
}

func InsertOpentsdbJson(taosConnect unsafe.Pointer, data []byte, db string) error {
	return capi.InsertOpentsdbJson(taosConnect, data, db)
}

func InsertOpentsdbTelnetBatch(taosConnect unsafe.Pointer, data []string, db string) error {
	return capi.InsertOpentsdbTelnet(taosConnect, data, db)
}

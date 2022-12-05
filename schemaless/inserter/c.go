package inserter

import (
	"unsafe"

	"github.com/taosdata/taosadapter/v3/schemaless/capi"
	"github.com/taosdata/taosadapter/v3/schemaless/proto"
)

func InsertInfluxdb(taosConnect unsafe.Pointer, data []byte, db, precision string, ttl int) (*proto.InfluxResult, error) {
	return capi.InsertInfluxdb(taosConnect, data, db, precision, ttl)
}

func InsertOpentsdbJson(taosConnect unsafe.Pointer, data []byte, db string, ttl int) error {
	return capi.InsertOpentsdbJson(taosConnect, data, db, ttl)
}

func InsertOpentsdbTelnetBatch(taosConnect unsafe.Pointer, data []string, db string, ttl int) error {
	return capi.InsertOpentsdbTelnet(taosConnect, data, db, ttl)
}

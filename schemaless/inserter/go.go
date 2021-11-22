//go:build goInserter
// +build goInserter

package inserter

import (
	"unsafe"

	"github.com/taosdata/taosadapter/schemaless/goapi/influxdb"
	"github.com/taosdata/taosadapter/schemaless/goapi/opentsdb"
	"github.com/taosdata/taosadapter/schemaless/proto"
)

func InsertInfluxdb(taosConnect unsafe.Pointer, data []byte, db, precision string) (*proto.InfluxResult, error) {
	return influxdb.InsertInfluxdb(taosConnect, data, db, precision)
}

func InsertOpentsdbJson(taosConnect unsafe.Pointer, data []byte, db string) error {
	return opentsdb.InsertOpentsdbJson(taosConnect, data, db)
}

func InsertOpentsdbTelnet(taosConnect unsafe.Pointer, data, db string) error {
	return opentsdb.InsertOpentsdbTelnet(taosConnect, data, db)
}

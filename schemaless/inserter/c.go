package inserter

import (
	"unsafe"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/taosadapter/v3/schemaless/capi"
	"github.com/taosdata/taosadapter/v3/schemaless/proto"
)

func InsertInfluxdb(taosConnect unsafe.Pointer, data []byte, db, precision string, ttl int, reqID uint64) (*proto.InfluxResult, error) {
	return capi.InsertInfluxdb(taosConnect, data, db, precision, ttl, getReqID(reqID))
}

func InsertOpentsdbJson(taosConnect unsafe.Pointer, data []byte, db string, ttl int, reqID uint64) error {
	return capi.InsertOpentsdbJson(taosConnect, data, db, ttl, getReqID(reqID))
}

func InsertOpentsdbTelnetBatch(taosConnect unsafe.Pointer, data []string, db string, ttl int, reqID uint64) error {
	return capi.InsertOpentsdbTelnet(taosConnect, data, db, ttl, getReqID(reqID))
}

func getReqID(id uint64) int64 {
	if id == 0 {
		return common.GetReqID()
	}
	return int64(id)
}

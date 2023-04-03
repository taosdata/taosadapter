package capi

import (
	"strings"
	"unsafe"

	"github.com/taosdata/driver-go/v3/common"
	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/thread"
)

func InsertInfluxdb(conn unsafe.Pointer, data []byte, db, precision string, ttl int, reqID int64) (rows int32, err error) {
	if reqID == 0 {
		reqID = common.GetReqID()
	}
	err = tool.SelectDB(conn, db, reqID)
	if err != nil {
		return 0, err
	}

	d := strings.TrimSpace(string(data))

	var result unsafe.Pointer

	thread.Lock()
	if ttl > 0 {
		rows, result = wrapper.TaosSchemalessInsertRawTTLWithReqID(conn, d, wrapper.InfluxDBLineProtocol, precision, ttl,
			reqID)
	} else {
		rows, result = wrapper.TaosSchemalessInsertRawWithReqID(conn, d, wrapper.InfluxDBLineProtocol, precision, reqID)
	}
	thread.Unlock()

	defer func() {
		thread.Lock()
		wrapper.TaosFreeResult(result)
		thread.Unlock()
	}()

	if code := wrapper.TaosError(result); code != 0 {
		err = tErrors.NewError(code, wrapper.TaosErrorStr(result))
	}
	return
}

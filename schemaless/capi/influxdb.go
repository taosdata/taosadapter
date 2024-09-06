package capi

import (
	"strings"
	"unsafe"

	"github.com/sirupsen/logrus"
	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/generator"
)

func InsertInfluxdb(conn unsafe.Pointer, data []byte, db, precision string, ttl int, reqID int64, tableNameKey string, logger *logrus.Entry) error {
	if reqID == 0 {
		reqID = generator.GetReqID()
	}
	err := tool.SchemalessSelectDB(conn, logger, log.IsDebug(), db, reqID)
	if err != nil {
		return err
	}

	d := strings.TrimSpace(string(data))

	var result unsafe.Pointer

	_, result = syncinterface.TaosSchemalessInsertRawTTLWithReqIDTBNameKey(conn, d, wrapper.InfluxDBLineProtocol, precision, ttl, reqID, tableNameKey, logger, log.IsDebug())

	defer func() {
		syncinterface.FreeResult(result, logger, log.IsDebug())
	}()

	if code := wrapper.TaosError(result); code != 0 {
		return tErrors.NewError(code, wrapper.TaosErrorStr(result))
	}
	return nil
}

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

func InsertOpentsdbJson(conn unsafe.Pointer, data []byte, db string, ttl int, reqID int64) error {
	if len(data) == 0 {
		return nil
	}
	if err := tool.SelectDB(conn, db, reqID); err != nil {
		return err
	}

	var result unsafe.Pointer
	thread.Lock()
	if ttl > 0 {
		_, result = wrapper.TaosSchemalessInsertRawTTLWithReqID(conn, string(data), wrapper.OpenTSDBJsonFormatProtocol,
			"", ttl, getReqID(reqID))
	} else {
		_, result = wrapper.TaosSchemalessInsertRawWithReqID(conn, string(data), wrapper.OpenTSDBJsonFormatProtocol,
			"", getReqID(reqID))
	}
	thread.Unlock()

	defer func() {
		thread.Lock()
		wrapper.TaosFreeResult(result)
		thread.Unlock()
	}()
	if code := wrapper.TaosError(result); code != 0 {
		return tErrors.NewError(code, wrapper.TaosErrorStr(result))
	}
	return nil
}

func InsertOpentsdbTelnet(conn unsafe.Pointer, data []string, db string, ttl int, reqID int64) error {
	trimData := make([]string, 0, len(data))
	for i := 0; i < len(data); i++ {
		if len(data[i]) == 0 {
			continue
		}
		trimData = append(trimData, strings.TrimPrefix(strings.TrimSpace(data[i]), "put "))
	}
	if len(trimData) == 0 {
		return nil
	}
	if err := tool.SelectDB(conn, db, reqID); err != nil {
		return err
	}

	var result unsafe.Pointer
	thread.Lock()
	if ttl > 0 {
		_, result = wrapper.TaosSchemalessInsertRawTTLWithReqID(conn, strings.Join(trimData, "\n"),
			wrapper.OpenTSDBTelnetLineProtocol, "", ttl, getReqID(reqID))
	} else {
		_, result = wrapper.TaosSchemalessInsertRawWithReqID(conn, strings.Join(trimData, "\n"),
			wrapper.OpenTSDBTelnetLineProtocol, "", getReqID(reqID))
	}
	thread.Unlock()
	defer func() {
		thread.Lock()
		wrapper.TaosFreeResult(result)
		thread.Unlock()
	}()

	code := wrapper.TaosError(result)
	if code != 0 {
		return tErrors.NewError(code, wrapper.TaosErrorStr(result))
	}
	return nil
}

func getReqID(id int64) int64 {
	if id == 0 {
		return common.GetReqID()
	}
	return id
}

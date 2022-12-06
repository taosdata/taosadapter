package capi

import (
	"strings"
	"unsafe"

	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/thread"
)

func InsertOpentsdbJson(conn unsafe.Pointer, data []byte, db string, ttl int) error {
	if len(data) == 0 {
		return nil
	}
	if err := tool.SelectDB(conn, db); err != nil {
		return err
	}

	var result unsafe.Pointer
	thread.Lock()
	if ttl > 0 {
		_, result = wrapper.TaosSchemalessInsertRawTTL(conn, string(data), wrapper.OpenTSDBJsonFormatProtocol, "",
			ttl)
	} else {
		_, result = wrapper.TaosSchemalessInsertRaw(conn, string(data), wrapper.OpenTSDBJsonFormatProtocol, "")
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

func InsertOpentsdbTelnet(conn unsafe.Pointer, data []string, db string, ttl int) error {
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
	if err := tool.SelectDB(conn, db); err != nil {
		return err
	}

	var result unsafe.Pointer
	thread.Lock()
	if ttl > 0 {
		_, result = wrapper.TaosSchemalessInsertRawTTL(conn, strings.Join(trimData, "\n"),
			wrapper.OpenTSDBTelnetLineProtocol, "", ttl)
	} else {
		_, result = wrapper.TaosSchemalessInsertRaw(conn, strings.Join(trimData, "\n"),
			wrapper.OpenTSDBTelnetLineProtocol, "")
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

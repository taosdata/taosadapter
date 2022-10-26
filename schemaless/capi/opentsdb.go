package capi

import (
	"strings"
	"unsafe"

	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/thread"
)

// InsertOpentsdbJson
// Deprecated
func InsertOpentsdbJson(taosConnect unsafe.Pointer, data []byte, db string) error {
	return insertOpentsdbJson(taosConnect, data, db, false)
}

func InsertOpentsdbJsonRaw(conn unsafe.Pointer, data []byte, db string) error {
	return insertOpentsdbJson(conn, data, db, true)
}

func insertOpentsdbJson(conn unsafe.Pointer, data []byte, db string, raw bool) error {
	if len(data) == 0 {
		return nil
	}
	if err := tool.SelectDB(conn, db); err != nil {
		return err
	}

	var result unsafe.Pointer
	thread.Lock()
	if raw {
		_, result = wrapper.TaosSchemalessInsertRaw(conn, string(data), wrapper.OpenTSDBJsonFormatProtocol, "")
	} else {
		result = wrapper.TaosSchemalessInsert(conn, []string{string(data)}, wrapper.OpenTSDBJsonFormatProtocol, "")
	}
	thread.Unlock()

	defer wrapper.TaosFreeResult(result)
	if code := wrapper.TaosError(result); code != 0 {
		return tErrors.NewError(code, wrapper.TaosErrorStr(result))
	}
	return nil
}

// InsertOpentsdbTelnetBatch
// Deprecated
func InsertOpentsdbTelnetBatch(taosConnect unsafe.Pointer, data []string, db string) error {
	return insertOpentsdbTelnet(taosConnect, data, db, false)
}

func InsertOpentsdbTelnetBatchRaw(conn unsafe.Pointer, data []string, db string) error {
	return insertOpentsdbTelnet(conn, data, db, true)
}

func insertOpentsdbTelnet(conn unsafe.Pointer, data []string, db string, raw bool) error {
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
	if raw {
		_, result = wrapper.TaosSchemalessInsertRaw(conn, strings.Join(trimData, "\n"),
			wrapper.OpenTSDBTelnetLineProtocol, "")
	} else {
		result = wrapper.TaosSchemalessInsert(conn, trimData, wrapper.OpenTSDBTelnetLineProtocol, "")
	}
	thread.Unlock()
	defer wrapper.TaosFreeResult(result)

	code := wrapper.TaosError(result)
	if code != 0 {
		return tErrors.NewError(code, wrapper.TaosErrorStr(result))
	}
	return nil
}

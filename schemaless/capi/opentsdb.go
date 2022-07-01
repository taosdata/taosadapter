package capi

import (
	"strings"
	"unsafe"

	tErrors "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/db/tool"
	"github.com/taosdata/taosadapter/thread"
)

func InsertOpentsdbJson(taosConnect unsafe.Pointer, data []byte, db string) error {
	if len(data) == 0 {
		return nil
	}
	err := tool.SelectDB(taosConnect, db)
	if err != nil {
		return err
	}
	thread.Lock()
	result := wrapper.TaosSchemalessInsert(taosConnect, []string{string(data)}, wrapper.OpenTSDBJsonFormatProtocol, "")
	thread.Unlock()
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		thread.Lock()
		wrapper.TaosFreeResult(result)
		thread.Unlock()
		return tErrors.NewError(code, errStr)
	}
	thread.Lock()
	wrapper.TaosFreeResult(result)
	thread.Unlock()
	return nil
}

func InsertOpentsdbTelnet(taosConnect unsafe.Pointer, data, db string) error {
	if len(data) == 0 {
		return nil
	}
	err := tool.SelectDB(taosConnect, db)
	if err != nil {
		return err
	}
	thread.Lock()
	result := wrapper.TaosSchemalessInsert(taosConnect, []string{strings.TrimPrefix(strings.TrimSpace(data), "put ")}, wrapper.OpenTSDBTelnetLineProtocol, "")
	thread.Unlock()
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		thread.Lock()
		wrapper.TaosFreeResult(result)
		thread.Unlock()
		return tErrors.NewError(code, errStr)
	}
	thread.Lock()
	wrapper.TaosFreeResult(result)
	thread.Unlock()
	return nil
}

func InsertOpentsdbTelnetBatch(taosConnect unsafe.Pointer, data []string, db string) error {
	if len(data) == 0 {
		return nil
	}
	err := tool.SelectDB(taosConnect, db)
	if err != nil {
		return err
	}
	for i := 0; i < len(data); i++ {
		data[i] = strings.TrimPrefix(strings.TrimSpace(data[i]), "put ")
	}
	thread.Lock()
	result := wrapper.TaosSchemalessInsert(taosConnect, data, wrapper.OpenTSDBTelnetLineProtocol, "")
	thread.Unlock()
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		thread.Lock()
		wrapper.TaosFreeResult(result)
		thread.Unlock()
		return tErrors.NewError(code, errStr)
	}
	thread.Lock()
	wrapper.TaosFreeResult(result)
	thread.Unlock()
	return nil
}

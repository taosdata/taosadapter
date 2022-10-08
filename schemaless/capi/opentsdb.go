package capi

import (
	"strings"
	"unsafe"

	tErrors "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/db/tool"
	"github.com/taosdata/taosadapter/thread"
	"github.com/taosdata/taosadapter/tools/pool"
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
	_, result := wrapper.TaosSchemalessInsertRaw(taosConnect, string(data), wrapper.OpenTSDBJsonFormatProtocol, "")
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
	line := strings.TrimPrefix(strings.TrimSpace(data), "put ")
	thread.Lock()
	_, result := wrapper.TaosSchemalessInsertRaw(taosConnect, line, wrapper.OpenTSDBTelnetLineProtocol, "")
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
	buffer := pool.BytesPoolGet()
	defer pool.BytesPoolPut(buffer)
	rows := len(data)
	for i := 0; i < rows; i++ {
		buffer.WriteString(strings.TrimPrefix(strings.TrimSpace(data[i]), "put "))
		if i != rows-1 {
			buffer.WriteByte('\n')
		}
	}
	thread.Lock()
	_, result := wrapper.TaosSchemalessInsertRaw(taosConnect, buffer.String(), wrapper.OpenTSDBTelnetLineProtocol, "")
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

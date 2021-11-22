package capi

import (
	"strings"
	"unsafe"

	tErrors "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
)

func InsertOpentsdbJson(taosConnect unsafe.Pointer, data []byte, db string) error {
	if len(data) == 0 {
		return nil
	}
	err := selectDB(taosConnect, db)
	if err != nil {
		return err
	}
	locker.Lock()
	result := wrapper.TaosSchemalessInsert(taosConnect, []string{string(data)}, wrapper.OpenTSDBJsonFormatProtocol, "")
	locker.Unlock()
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		return tErrors.NewError(code, errStr)
	}
	wrapper.TaosFreeResult(result)
	return nil
}

func InsertOpentsdbTelnet(taosConnect unsafe.Pointer, data, db string) error {
	if len(data) == 0 {
		return nil
	}
	err := selectDB(taosConnect, db)
	if err != nil {
		return err
	}
	locker.Lock()
	result := wrapper.TaosSchemalessInsert(taosConnect, []string{strings.TrimPrefix(strings.TrimSpace(data), "put ")}, wrapper.OpenTSDBTelnetLineProtocol, "")
	locker.Unlock()
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		return tErrors.NewError(code, errStr)
	}
	wrapper.TaosFreeResult(result)
	return nil
}

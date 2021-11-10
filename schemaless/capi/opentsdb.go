package capi

import (
	"strings"
	"unsafe"

	tErrors "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/httperror"
)

func InsertOpentsdbJson(taosConnect unsafe.Pointer, data []byte, db string) error {
	code := wrapper.TaosSelectDB(taosConnect, db)
	if code != httperror.SUCCESS {
		return tErrors.GetError(code)
	}
	result := wrapper.TaosSchemalessInsert(taosConnect, []string{string(data)}, wrapper.OpenTSDBJsonFormatProtocol, "")
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		return &tErrors.TaosError{
			Code:   int32(code) & 0xffff,
			ErrStr: errStr,
		}
	}
	wrapper.TaosFreeResult(result)
	return nil
}

func InsertOpentsdbTelnet(taosConnect unsafe.Pointer, data, db string) error {
	code := wrapper.TaosSelectDB(taosConnect, db)
	if code != httperror.SUCCESS {
		return tErrors.GetError(code)
	}
	result := wrapper.TaosSchemalessInsert(taosConnect, []string{strings.TrimPrefix(strings.TrimSpace(data), "put ")}, wrapper.OpenTSDBTelnetLineProtocol, "")
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		return &tErrors.TaosError{
			Code:   int32(code) & 0xffff,
			ErrStr: errStr,
		}
	}
	wrapper.TaosFreeResult(result)
	return nil
}

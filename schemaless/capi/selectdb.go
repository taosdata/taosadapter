package capi

import (
	"unsafe"

	tErrors "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/db/tool"
	"github.com/taosdata/taosadapter/httperror"
)

func SelectDB(taosConnect unsafe.Pointer, db string) error {
	code := wrapper.TaosSelectDB(taosConnect, db)
	if code != httperror.SUCCESS {
		if int32(code)&0xffff == tErrors.TSC_DB_NOT_SELECTED || int32(code)&0xffff == tErrors.MND_INVALID_DB {
			err := tool.CreateDBWithConnection(taosConnect, db)
			if err != nil {
				return err
			}
			wrapper.TaosSelectDB(taosConnect, db)
		} else {
			return tErrors.GetError(code)
		}
	}
	return nil
}

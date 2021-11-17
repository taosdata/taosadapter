package capi

import (
	"unsafe"

	tErrors "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/db/tool"
	"github.com/taosdata/taosadapter/httperror"
)

func selectDB(taosConnect unsafe.Pointer, db string) error {
	code := wrapper.TaosSelectDB(taosConnect, db)
	if code != httperror.SUCCESS {
		if code == int(tErrors.TSC_DB_NOT_SELECTED) {
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

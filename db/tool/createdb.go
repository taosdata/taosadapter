package tool

import (
	"unsafe"

	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/db/async"
	"github.com/taosdata/taosadapter/httperror"
	"github.com/taosdata/taosadapter/thread"
	"github.com/taosdata/taosadapter/tools/pool"
)

func CreateDBWithConnection(connection unsafe.Pointer, db string) error {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("create database if not exists ")
	b.WriteString(db)
	b.WriteString(" precision 'ns' schemaless 1")
	err := async.GlobalAsync.TaosExecWithoutResult(connection, b.String())
	if err != nil {
		return err
	}
	return nil
}

func SelectDB(taosConnect unsafe.Pointer, db string) error {
	err := async.GlobalAsync.TaosExecWithoutResult(taosConnect, "use "+db)
	if err != nil {
		e, is := err.(*errors.TaosError)
		if is && e.Code == httperror.TSDB_CODE_MND_DB_NOT_EXIST {
			err := CreateDBWithConnection(taosConnect, db)
			if err != nil {
				return err
			}
			thread.Lock()
			wrapper.TaosSelectDB(taosConnect, db)
			thread.Unlock()
		} else {
			return err
		}
	}
	return nil
}

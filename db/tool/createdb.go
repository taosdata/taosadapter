package tool

import (
	"unsafe"

	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/thread"
	"github.com/taosdata/taosadapter/v3/tools/pool"
)

func CreateDBWithConnection(connection unsafe.Pointer, db string, reqID int64) error {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("create database if not exists ")
	b.WriteString(db)
	b.WriteString(" precision 'ns' schemaless 1")
	err := async.GlobalAsync.TaosExecWithoutResult(connection, b.String(), reqID)
	if err != nil {
		return err
	}
	return nil
}

func SelectDB(taosConnect unsafe.Pointer, db string, reqID int64) error {
	err := async.GlobalAsync.TaosExecWithoutResult(taosConnect, "use "+db, reqID)
	if err != nil {
		e, is := err.(*errors.TaosError)
		if is && e.Code == httperror.TSDB_CODE_MND_DB_NOT_EXIST && config.Conf.SMLAutoCreateDB {
			err := CreateDBWithConnection(taosConnect, db, reqID)
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

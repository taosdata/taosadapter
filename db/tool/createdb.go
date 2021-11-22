package tool

import (
	"unsafe"

	"github.com/taosdata/taosadapter/db/async"
	"github.com/taosdata/taosadapter/tools/pool"
)

func CreateDBWithConnection(connection unsafe.Pointer, db string) error {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("create database if not exists ")
	b.WriteString(db)
	b.WriteString(" precision 'ns' update 2")
	err := async.GlobalAsync.TaosExecWithoutResult(connection, b.String())
	if err != nil {
		return err
	}
	return nil
}

package tool

import (
	"unsafe"

	"github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/tools/pool"
)

func CreateDBWithConnection(connection unsafe.Pointer, db string) error {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("create database if not exists ")
	b.WriteString(db)
	b.WriteString(" precision 'ns' update 2")
	result := wrapper.TaosQuery(connection, b.String())
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		return &errors.TaosError{
			Code:   int32(code) & 0xffff,
			ErrStr: errStr,
		}
	}
	wrapper.TaosFreeResult(result)
	return nil
}

package tool

import (
	"github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/db/commonpool"
	"github.com/taosdata/taosadapter/tools/pool"
)

func CreateDB(user, password, db string) error {
	taosConn, err := commonpool.GetConnection(user, password)
	if err != nil {
		return err
	}
	defer func() {
		taosConn.Put()
	}()
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("create database if not exists ")
	b.WriteString(db)
	b.WriteString(" precision 'ns' update 2")
	result := wrapper.TaosQuery(taosConn.TaosConnection, b.String())
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

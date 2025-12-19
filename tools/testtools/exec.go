package testtools

import (
	"database/sql/driver"
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
)

func Exec(conn unsafe.Pointer, sql string) error {
	res := wrapper.TaosQuery(conn, sql)
	defer wrapper.TaosFreeResult(res)
	code := wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		return errors.NewError(code, errStr)
	}
	return nil
}

func Query(conn unsafe.Pointer, sql string) ([][]driver.Value, error) {
	res := wrapper.TaosQuery(conn, sql)
	defer wrapper.TaosFreeResult(res)
	code := wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		return nil, errors.NewError(code, errStr)
	}
	fileCount := wrapper.TaosNumFields(res)
	rh, err := wrapper.ReadColumn(res, fileCount)
	if err != nil {
		return nil, err
	}
	precision := wrapper.TaosResultPrecision(res)
	var result [][]driver.Value
	for {
		columns, errCode, block := wrapper.TaosFetchRawBlock(res)
		if errCode != 0 {
			errStr := wrapper.TaosErrorStr(res)
			return nil, errors.NewError(errCode, errStr)
		}
		if columns == 0 {
			break
		}
		r, err := parser.ReadBlock(block, columns, rh.ColTypes, precision)
		if err != nil {
			return nil, err
		}
		result = append(result, r...)
	}
	return result, nil
}

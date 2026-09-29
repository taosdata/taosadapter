// Copyright (c) 2025 TAOS Data, Inc.
//
// SPDX-License-Identifier: MIT
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package testtools

import (
	"database/sql/driver"
	"fmt"
	"time"
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
)

// TSDB_CODE_MND_TRANS_CONFLICT: "Conflict transaction not completed".
const transConflictErrCode = 0x3d3

func Exec(conn unsafe.Pointer, sql string) error {
	var lastErr error
	for i := 0; i < 120; i++ {
		res := wrapper.TaosQuery(conn, sql)
		code := wrapper.TaosError(res)
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		if code == 0 {
			return nil
		}
		lastErr = errors.NewError(code, errStr)
		if code&0xffff != transConflictErrCode {
			return lastErr
		}
		// Another transaction on the same db is still committing, retry.
		time.Sleep(500 * time.Millisecond)
	}
	return lastErr
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

func EnsureDBCreated(dbName string) error {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		return err
	}
	defer wrapper.TaosClose(conn)
	for i := 0; i < 100; i++ {
		value, err := Query(conn, fmt.Sprintf(`select * from performance_schema.perf_trans where db = '%s'`, dbName))
		if err != nil {
			return err
		}
		if len(value) == 0 {
			value, err = Query(conn, fmt.Sprintf(`select * from information_schema.ins_databases where name = '%s'`, dbName))
			if err != nil {
				return err
			}
			if len(value) > 0 {
				return nil
			}
		}
		time.Sleep(time.Millisecond * 500)
	}
	return fmt.Errorf("db %s not created after waiting", dbName)
}

func EnsureTokenCreated(tokenName string) error {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		return err
	}
	defer wrapper.TaosClose(conn)
	for i := 0; i < 100; i++ {
		value, err := Query(conn, `select * from performance_schema.perf_trans where oper = 'create-token'`)
		if err != nil {
			return err
		}
		if len(value) == 0 {
			value, err := Query(conn, fmt.Sprintf(`select * from information_schema.ins_tokens where name = '%s'`, tokenName))
			if err != nil {
				return err
			}
			if len(value) > 0 {
				return nil
			}
		}
		time.Sleep(time.Millisecond * 500)
	}
	return fmt.Errorf("token not created after waiting")
}

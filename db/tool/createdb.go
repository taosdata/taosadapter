// Copyright (c) 2021 TAOS Data, Inc.
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

package tool

import (
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/tools/pool"
)

func CreateDBWithConnection(connection unsafe.Pointer, logger *logrus.Entry, isDebug bool, db string, reqID int64) error {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("create database if not exists ")
	b.WriteString(db)
	b.WriteString(" precision 'ns' schemaless 1")
	err := async.GlobalAsync.TaosExecWithoutResult(connection, logger, isDebug, b.String(), reqID)
	if err != nil {
		return err
	}
	return nil
}

func SchemalessSelectDB(taosConnect unsafe.Pointer, logger *logrus.Entry, isDebug bool, db string, reqID int64) error {
	err := async.GlobalAsync.TaosExecWithoutResult(taosConnect, logger, isDebug, "use "+db, reqID)
	if err != nil {
		e, is := err.(*errors.TaosError)
		if is && e.Code == httperror.TSDB_CODE_MND_DB_NOT_EXIST && config.Conf.SMLAutoCreateDB {
			err := CreateDBWithConnection(taosConnect, logger, isDebug, db, reqID)
			if err != nil {
				return err
			}
			logger.Tracef("use db %s", db)
			code := syncinterface.TaosSelectDB(taosConnect, db, logger, isDebug)
			if code != 0 {
				return errors.NewError(code, syncinterface.TaosErrorStr(nil, logger, isDebug))
			}
		} else {
			return err
		}
	}
	return nil
}

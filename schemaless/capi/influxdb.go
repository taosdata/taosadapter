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

package capi

import (
	"strings"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/db/tool"
	tErrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/generator"
)

func InsertInfluxdb(conn unsafe.Pointer, data []byte, db, precision string, ttl int, reqID int64, tableNameKey string, logger *logrus.Entry) error {
	if reqID == 0 {
		reqID = generator.GetReqID()
	}
	err := tool.SchemalessSelectDB(conn, logger, log.IsDebug(), db, reqID)
	if err != nil {
		return err
	}

	d := strings.TrimSpace(string(data))

	var result unsafe.Pointer

	_, result = syncinterface.TaosSchemalessInsertRawTTLWithReqIDTBNameKey(conn, d, wrapper.InfluxDBLineProtocol, precision, ttl, reqID, tableNameKey, logger, log.IsDebug())

	defer func() {
		syncinterface.TaosSchemalessFree(result, logger, log.IsDebug())
	}()

	if code := syncinterface.TaosError(result, logger, log.IsDebug()); code != 0 {
		return tErrors.NewError(code, syncinterface.TaosErrorStr(result, logger, log.IsDebug()))
	}
	return nil
}

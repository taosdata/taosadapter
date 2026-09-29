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

package inserter

import (
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/schemaless/capi"
	"github.com/taosdata/taosadapter/v3/tools/generator"
)

func InsertInfluxdb(taosConnect unsafe.Pointer, data []byte, db, precision string, ttl int, reqID uint64, tableNameKey string, logger *logrus.Entry) error {
	return capi.InsertInfluxdb(taosConnect, data, db, precision, ttl, getReqID(reqID), tableNameKey, logger)
}

func InsertOpentsdbJson(taosConnect unsafe.Pointer, data []byte, db string, ttl int, reqID uint64, tableNameKey string, logger *logrus.Entry) error {
	return capi.InsertOpentsdbJson(taosConnect, data, db, ttl, getReqID(reqID), tableNameKey, logger)
}

func InsertOpentsdbTelnetBatch(taosConnect unsafe.Pointer, data []string, db string, ttl int, reqID uint64, tableNameKey string, logger *logrus.Entry) error {
	return capi.InsertOpentsdbTelnet(taosConnect, data, db, ttl, getReqID(reqID), tableNameKey, logger)
}

func getReqID(id uint64) int64 {
	if id == 0 {
		return generator.GetReqID()
	}
	return int64(id)
}

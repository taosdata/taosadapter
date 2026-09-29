// Copyright (c) 2022 TAOS Data, Inc.
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

package parseblock

import (
	"database/sql/driver"
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	"github.com/taosdata/taosadapter/v3/tools"
)

func ParseBlock(data []byte, colTypes []uint8, rows int, precision int) (uint64, [][]driver.Value, error) {
	p0 := unsafe.Pointer(&data[0])
	id := *(*uint64)(p0)
	columnPtr := tools.AddPointer(p0, 8)
	result, err := parser.ReadBlock(columnPtr, rows, colTypes, precision)
	if err != nil {
		return 0, nil, err
	}
	return id, result, nil
}

func ParseTmqBlock(data []byte, colTypes []uint8, rows int, precision int) (uint64, uint64, [][]driver.Value, error) {
	p0 := unsafe.Pointer(&data[0])
	id := *(*uint64)(p0)
	messageID := *(*uint64)(tools.AddPointer(p0, 8))
	columnPtr := tools.AddPointer(p0, 16)
	result, err := parser.ReadBlock(columnPtr, rows, colTypes, precision)
	return id, messageID, result, err
}

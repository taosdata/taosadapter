// Copyright (c) 2024 TAOS Data, Inc.
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

package wrapper

/*
#include <taos.h>
*/
import "C"
import (
	"database/sql/driver"
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/tools"
)

const (
	PointerSize = unsafe.Sizeof(uintptr(1))
)

type FormatTimeFunc func(ts int64, precision int) driver.Value

func FetchRow(row unsafe.Pointer, offset int, colType uint8, length int, arg ...interface{}) driver.Value {
	base := *(**C.void)(tools.AddPointer(row, uintptr(offset)*PointerSize))
	p := unsafe.Pointer(base)
	if p == nil {
		return nil
	}
	switch colType {
	case C.TSDB_DATA_TYPE_BOOL:
		if v := *((*byte)(p)); v != 0 {
			return true
		}
		return false
	case C.TSDB_DATA_TYPE_TINYINT:
		return *((*int8)(p))
	case C.TSDB_DATA_TYPE_SMALLINT:
		return *((*int16)(p))
	case C.TSDB_DATA_TYPE_INT:
		return *((*int32)(p))
	case C.TSDB_DATA_TYPE_BIGINT:
		return *((*int64)(p))
	case C.TSDB_DATA_TYPE_UTINYINT:
		return *((*uint8)(p))
	case C.TSDB_DATA_TYPE_USMALLINT:
		return *((*uint16)(p))
	case C.TSDB_DATA_TYPE_UINT:
		return *((*uint32)(p))
	case C.TSDB_DATA_TYPE_UBIGINT:
		return *((*uint64)(p))
	case C.TSDB_DATA_TYPE_FLOAT:
		return *((*float32)(p))
	case C.TSDB_DATA_TYPE_DOUBLE:
		return *((*float64)(p))
	case C.TSDB_DATA_TYPE_BINARY, C.TSDB_DATA_TYPE_NCHAR:
		data := make([]byte, length)
		for i := 0; i < length; i++ {
			data[i] = *((*byte)(tools.AddPointer(p, uintptr(i))))
		}
		return string(data)
	case C.TSDB_DATA_TYPE_DECIMAL64, C.TSDB_DATA_TYPE_DECIMAL:
		data := make([]byte, 0, length)
		var b byte
		for i := 0; i < length; i++ {
			b = *((*byte)(tools.AddPointer(p, uintptr(i))))
			if b == 0 {
				break
			}
			data = append(data, b)
		}
		return string(data)
	case C.TSDB_DATA_TYPE_TIMESTAMP:
		if len(arg) == 1 {
			return common.TimestampConvertToTime(*((*int64)(p)), arg[0].(int))
		} else if len(arg) == 2 {
			return arg[1].(FormatTimeFunc)(*((*int64)(p)), arg[0].(int))
		}
		panic("convertTime error")
	case C.TSDB_DATA_TYPE_JSON, C.TSDB_DATA_TYPE_VARBINARY, C.TSDB_DATA_TYPE_GEOMETRY, C.TSDB_DATA_TYPE_BLOB:
		data := make([]byte, length)
		for i := 0; i < length; i++ {
			data[i] = *((*byte)(tools.AddPointer(p, uintptr(i))))
		}
		return data
	default:
		return nil
	}
}

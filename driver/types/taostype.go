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

package types

import (
	"reflect"
	"time"
)

type (
	TaosBool      bool
	TaosTinyint   int8
	TaosSmallint  int16
	TaosInt       int32
	TaosBigint    int64
	TaosUTinyint  uint8
	TaosUSmallint uint16
	TaosUInt      uint32
	TaosUBigint   uint64
	TaosFloat     float32
	TaosDouble    float64
	TaosBinary    []byte
	TaosVarBinary []byte
	TaosNchar     string
	TaosTimestamp struct {
		T         time.Time
		Precision int
	}
	TaosJson     []byte
	TaosGeometry []byte
)

var (
	TaosBoolType      = reflect.TypeOf(TaosBool(false))
	TaosTinyintType   = reflect.TypeOf(TaosTinyint(0))
	TaosSmallintType  = reflect.TypeOf(TaosSmallint(0))
	TaosIntType       = reflect.TypeOf(TaosInt(0))
	TaosBigintType    = reflect.TypeOf(TaosBigint(0))
	TaosUTinyintType  = reflect.TypeOf(TaosUTinyint(0))
	TaosUSmallintType = reflect.TypeOf(TaosUSmallint(0))
	TaosUIntType      = reflect.TypeOf(TaosUInt(0))
	TaosUBigintType   = reflect.TypeOf(TaosUBigint(0))
	TaosFloatType     = reflect.TypeOf(TaosFloat(0))
	TaosDoubleType    = reflect.TypeOf(TaosDouble(0))
	TaosBinaryType    = reflect.TypeOf(TaosBinary(nil))
	TaosVarBinaryType = reflect.TypeOf(TaosVarBinary(nil))
	TaosNcharType     = reflect.TypeOf(TaosNchar(""))
	TaosTimestampType = reflect.TypeOf(TaosTimestamp{})
	TaosJsonType      = reflect.TypeOf(TaosJson(""))
	TaosGeometryType  = reflect.TypeOf(TaosGeometry(nil))
)

type ColumnType struct {
	Type   reflect.Type
	MaxLen int
}

// Copyright (c) 2026 TAOS Data, Inc.
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

package stmt

import (
	"database/sql/driver"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/driver/common"
)

func TestMarshalStmt2ColumnBinary(t *testing.T) {
	fields := []*Stmt2AllField{
		{
			FieldType: common.TSDB_DATA_TYPE_BINARY,
			BindType:  TAOS_FIELD_TBNAME,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
			BindType:  TAOS_FIELD_COL,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_INT,
			BindType:  TAOS_FIELD_COL,
		},
	}
	now := time.Unix(0, 0)
	next := now.Add(time.Second)
	got, err := MarshalStmt2ColumnBinary([][]driver.Value{
		{"ct1", "ct2"},
		{now, next},
		{int32(1), int32(2)},
	}, fields)
	assert.NoError(t, err)
	assert.Equal(t, uint32(len(got)), binary.LittleEndian.Uint32(got[Stmt2ColumnTotalLengthPosition:]))
	assert.Equal(t, uint32(2), binary.LittleEndian.Uint32(got[Stmt2ColumnRowCountPosition:]))
	assert.Equal(t, uint32(2), binary.LittleEndian.Uint32(got[Stmt2ColumnTableCountPosition:]))
	assert.Equal(t, uint32(3), binary.LittleEndian.Uint32(got[Stmt2ColumnFieldCountPosition:]))
	assert.Equal(t, uint32(Stmt2ColumnDataPosition), binary.LittleEndian.Uint32(got[Stmt2ColumnFieldOffsetPosition:]))
}

func TestMarshalStmt2ColumnBinaryQueryRequiresSingleRow(t *testing.T) {
	_, err := MarshalStmt2ColumnBinary([][]driver.Value{
		{time.Unix(0, 0), time.Unix(1, 0)},
	}, nil)
	assert.EqualError(t, err, "query only supports one row")
}

func TestMarshalStmt2ColumnBinaryQueryWithoutFields(t *testing.T) {
	got, err := MarshalStmt2ColumnBinary([][]driver.Value{
		{time.Unix(0, 0)},
		{int32(7)},
	}, nil)
	assert.NoError(t, err)
	assert.Equal(t, uint32(len(got)), binary.LittleEndian.Uint32(got[Stmt2ColumnTotalLengthPosition:]))
	assert.Equal(t, uint32(1), binary.LittleEndian.Uint32(got[Stmt2ColumnRowCountPosition:]))
	assert.Equal(t, uint32(1), binary.LittleEndian.Uint32(got[Stmt2ColumnTableCountPosition:]))
	assert.Equal(t, uint32(2), binary.LittleEndian.Uint32(got[Stmt2ColumnFieldCountPosition:]))
	assert.Equal(t, uint32(common.TSDB_DATA_TYPE_TIMESTAMP), binary.LittleEndian.Uint32(got[Stmt2ColumnDataPosition+BindDataTypeOffset:]))
}

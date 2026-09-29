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

package stmt

import (
	"database/sql/driver"
	"fmt"

	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/driver/types"
)

type StmtField struct {
	Name      string `json:"name"`
	FieldType int8   `json:"field_type"`
	Precision uint8  `json:"precision"`
	Scale     uint8  `json:"scale"`
	Bytes     int32  `json:"bytes"`
}

func (s *StmtField) GetType() (*types.ColumnType, error) {
	switch s.FieldType {
	case common.TSDB_DATA_TYPE_BOOL:
		return &types.ColumnType{Type: types.TaosBoolType}, nil
	case common.TSDB_DATA_TYPE_TINYINT:
		return &types.ColumnType{Type: types.TaosTinyintType}, nil
	case common.TSDB_DATA_TYPE_SMALLINT:
		return &types.ColumnType{Type: types.TaosSmallintType}, nil
	case common.TSDB_DATA_TYPE_INT:
		return &types.ColumnType{Type: types.TaosIntType}, nil
	case common.TSDB_DATA_TYPE_BIGINT:
		return &types.ColumnType{Type: types.TaosBigintType}, nil
	case common.TSDB_DATA_TYPE_UTINYINT:
		return &types.ColumnType{Type: types.TaosUTinyintType}, nil
	case common.TSDB_DATA_TYPE_USMALLINT:
		return &types.ColumnType{Type: types.TaosUSmallintType}, nil
	case common.TSDB_DATA_TYPE_UINT:
		return &types.ColumnType{Type: types.TaosUIntType}, nil
	case common.TSDB_DATA_TYPE_UBIGINT:
		return &types.ColumnType{Type: types.TaosUBigintType}, nil
	case common.TSDB_DATA_TYPE_FLOAT:
		return &types.ColumnType{Type: types.TaosFloatType}, nil
	case common.TSDB_DATA_TYPE_DOUBLE:
		return &types.ColumnType{Type: types.TaosDoubleType}, nil
	case common.TSDB_DATA_TYPE_BINARY:
		return &types.ColumnType{Type: types.TaosBinaryType}, nil
	case common.TSDB_DATA_TYPE_VARBINARY:
		return &types.ColumnType{Type: types.TaosVarBinaryType}, nil
	case common.TSDB_DATA_TYPE_NCHAR:
		return &types.ColumnType{Type: types.TaosNcharType}, nil
	case common.TSDB_DATA_TYPE_TIMESTAMP:
		return &types.ColumnType{Type: types.TaosTimestampType}, nil
	case common.TSDB_DATA_TYPE_JSON:
		return &types.ColumnType{Type: types.TaosJsonType}, nil
	case common.TSDB_DATA_TYPE_GEOMETRY:
		return &types.ColumnType{Type: types.TaosGeometryType}, nil
	}
	return nil, fmt.Errorf("unsupported type: %d, name %s", s.FieldType, s.Name)
}

//revive:disable
const (
	TAOS_FIELD_COL = iota + 1
	TAOS_FIELD_TAG
	TAOS_FIELD_QUERY
	TAOS_FIELD_TBNAME
)

//revive:enable

type TaosStmt2BindData struct {
	TableName string
	Tags      []driver.Value   // row format
	Cols      [][]driver.Value // column format
}

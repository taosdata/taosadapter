package rest

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/types"
	"github.com/taosdata/driver-go/v3/wrapper"
)

func Test_stmtConvert(t *testing.T) {
	type args struct {
		src        [][]driver.Value
		fields     []*wrapper.StmtField
		fieldTypes []*types.ColumnType
	}
	tests := []struct {
		name           string
		args           args
		want           [][]driver.Value
		wantFieldTypes []*types.ColumnType
		wantErr        bool
	}{
		//bool
		{
			name: "bool_null",
			args: args{
				src: [][]driver.Value{{nil}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BOOL,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBoolType,
					},
				},
			},
			want: [][]driver.Value{{nil}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBoolType,
				},
			},
			wantErr: false,
		},
		{
			name: "bool_err",
			args: args{
				src: [][]driver.Value{{[]int{123}}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BOOL,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBoolType,
					},
				},
			},
			want: [][]driver.Value{{}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBoolType,
				},
			},
			wantErr: true,
		},
		{
			name: "bool_bool_true",
			args: args{
				src: [][]driver.Value{{true}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BOOL,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBoolType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosBool(true)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBoolType,
				},
			},
			wantErr: false,
		},
		{
			name: "bool_bool_false",
			args: args{
				src: [][]driver.Value{{false}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BOOL,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBoolType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosBool(false)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBoolType,
				},
			},
			wantErr: false,
		},
		{
			name: "bool_float_false",
			args: args{
				src: [][]driver.Value{{float32(0)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BOOL,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBoolType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosBool(false)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBoolType,
				},
			},
			wantErr: false,
		},
		{
			name: "bool_float_true",
			args: args{
				src: [][]driver.Value{{float32(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BOOL,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBoolType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosBool(true)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBoolType,
				},
			},
			wantErr: false,
		},
		{
			name: "bool_int_false",
			args: args{
				src: [][]driver.Value{{int32(0)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BOOL,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBoolType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosBool(false)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBoolType,
				},
			},
			wantErr: false,
		},
		{
			name: "bool_int_true",
			args: args{
				src: [][]driver.Value{{int32(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BOOL,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBoolType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosBool(true)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBoolType,
				},
			},
			wantErr: false,
		},
		{
			name: "bool_uint_false",
			args: args{
				src: [][]driver.Value{{uint32(0)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BOOL,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBoolType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosBool(false)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBoolType,
				},
			},
			wantErr: false,
		},
		{
			name: "bool_uint_true",
			args: args{
				src: [][]driver.Value{{uint32(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BOOL,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBoolType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosBool(true)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBoolType,
				},
			},
			wantErr: false,
		},
		{
			name: "bool_string_false",
			args: args{
				src: [][]driver.Value{{"false"}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BOOL,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBoolType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosBool(false)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBoolType,
				},
			},
			wantErr: false,
		},
		{
			name: "bool_string_true",
			args: args{
				src: [][]driver.Value{{"true"}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BOOL,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBoolType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosBool(true)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBoolType,
				},
			},
			wantErr: false,
		},
		//tiny int
		{
			name: "tiny_nil",
			args: args{
				src: [][]driver.Value{{nil}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_TINYINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosTinyintType,
					},
				},
			},
			want: [][]driver.Value{{nil}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosTinyintType,
				},
			},
			wantErr: false,
		},
		{
			name: "tiny_err",
			args: args{
				src: [][]driver.Value{{[]int{123}}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_TINYINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosTinyintType,
					},
				},
			},
			want: [][]driver.Value{{}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosTinyintType,
				},
			},
			wantErr: true,
		},
		{
			name: "tiny_bool_1",
			args: args{
				src: [][]driver.Value{{true}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_TINYINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosTinyintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosTinyint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosTinyintType,
				},
			},
			wantErr: false,
		},
		{
			name: "tiny_bool_0",
			args: args{
				src: [][]driver.Value{{false}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_TINYINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosTinyintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosTinyint(0)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosTinyintType,
				},
			},
			wantErr: false,
		},
		{
			name: "tiny_float_1",
			args: args{
				src: [][]driver.Value{{float32(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_TINYINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosTinyintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosTinyint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosTinyintType,
				},
			},
			wantErr: false,
		},
		{
			name: "tiny_int_1",
			args: args{
				src: [][]driver.Value{{int(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_TINYINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosTinyintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosTinyint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosTinyintType,
				},
			},
			wantErr: false,
		},
		{
			name: "tiny_uint_1",
			args: args{
				src: [][]driver.Value{{uint(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_TINYINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosTinyintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosTinyint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosTinyintType,
				},
			},
			wantErr: false,
		},
		{
			name: "tiny_string_1",
			args: args{
				src: [][]driver.Value{{"1"}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_TINYINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosTinyintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosTinyint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosTinyintType,
				},
			},
			wantErr: false,
		},
		//small int
		{
			name: "small_nil",
			args: args{
				src: [][]driver.Value{{nil}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_SMALLINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosSmallintType,
					},
				},
			},
			want: [][]driver.Value{{nil}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosSmallintType,
				},
			},
			wantErr: false,
		},
		{
			name: "small_err",
			args: args{
				src: [][]driver.Value{{[]int{123}}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_SMALLINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosSmallintType,
					},
				},
			},
			want: [][]driver.Value{{}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosSmallintType,
				},
			},
			wantErr: true,
		},
		{
			name: "small_bool_1",
			args: args{
				src: [][]driver.Value{{true}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_SMALLINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosSmallintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosSmallint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosSmallintType,
				},
			},
			wantErr: false,
		},
		{
			name: "small_bool_0",
			args: args{
				src: [][]driver.Value{{false}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_SMALLINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosSmallintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosSmallint(0)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosSmallintType,
				},
			},
			wantErr: false,
		},
		{
			name: "small_float_1",
			args: args{
				src: [][]driver.Value{{float32(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_SMALLINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosSmallintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosSmallint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosSmallintType,
				},
			},
			wantErr: false,
		},
		{
			name: "small_int_1",
			args: args{
				src: [][]driver.Value{{int(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_SMALLINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosSmallintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosSmallint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosSmallintType,
				},
			},
			wantErr: false,
		},
		{
			name: "small_uint_1",
			args: args{
				src: [][]driver.Value{{uint(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_SMALLINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosSmallintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosSmallint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosSmallintType,
				},
			},
			wantErr: false,
		},
		{
			name: "small_string_1",
			args: args{
				src: [][]driver.Value{{"1"}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_SMALLINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosSmallintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosSmallint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosSmallintType,
				},
			},
			wantErr: false,
		},
		//int
		{
			name: "int_nil",
			args: args{
				src: [][]driver.Value{{nil}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_INT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosIntType,
					},
				},
			},
			want: [][]driver.Value{{nil}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosIntType,
				},
			},
			wantErr: false,
		},
		{
			name: "int_err",
			args: args{
				src: [][]driver.Value{{[]int{123}}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_INT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosIntType,
					},
				},
			},
			want: [][]driver.Value{{}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosIntType,
				},
			},
			wantErr: true,
		},
		{
			name: "int_bool_1",
			args: args{
				src: [][]driver.Value{{true}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_INT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosIntType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosInt(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosIntType,
				},
			},
			wantErr: false,
		},
		{
			name: "int_bool_0",
			args: args{
				src: [][]driver.Value{{false}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_INT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosIntType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosInt(0)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosIntType,
				},
			},
			wantErr: false,
		},
		{
			name: "int_float_1",
			args: args{
				src: [][]driver.Value{{float32(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_INT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosIntType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosInt(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosIntType,
				},
			},
			wantErr: false,
		},
		{
			name: "int_int_1",
			args: args{
				src: [][]driver.Value{{int(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_INT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosIntType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosInt(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosIntType,
				},
			},
			wantErr: false,
		},
		{
			name: "int_uint_1",
			args: args{
				src: [][]driver.Value{{uint(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_INT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosIntType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosInt(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosIntType,
				},
			},
			wantErr: false,
		},
		{
			name: "int_string_1",
			args: args{
				src: [][]driver.Value{{"1"}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_INT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosIntType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosInt(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosIntType,
				},
			},
			wantErr: false,
		},
		//big int
		{
			name: "big_nil",
			args: args{
				src: [][]driver.Value{{nil}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BIGINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBigintType,
					},
				},
			},
			want: [][]driver.Value{{nil}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBigintType,
				},
			},
			wantErr: false,
		},
		{
			name: "big_err",
			args: args{
				src: [][]driver.Value{{[]int{123}}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BIGINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBigintType,
					},
				},
			},
			want: [][]driver.Value{{}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBigintType,
				},
			},
			wantErr: true,
		},
		{
			name: "big_bool_1",
			args: args{
				src: [][]driver.Value{{true}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BIGINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBigintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosBigint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBigintType,
				},
			},
			wantErr: false,
		},
		{
			name: "big_bool_0",
			args: args{
				src: [][]driver.Value{{false}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BIGINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBigintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosBigint(0)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBigintType,
				},
			},
			wantErr: false,
		},
		{
			name: "big_float_1",
			args: args{
				src: [][]driver.Value{{float32(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BIGINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBigintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosBigint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBigintType,
				},
			},
			wantErr: false,
		},
		{
			name: "big_int_1",
			args: args{
				src: [][]driver.Value{{int(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BIGINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBigintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosBigint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBigintType,
				},
			},
			wantErr: false,
		},
		{
			name: "big_uint_1",
			args: args{
				src: [][]driver.Value{{uint(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BIGINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBigintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosBigint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBigintType,
				},
			},
			wantErr: false,
		},
		{
			name: "big_string_1",
			args: args{
				src: [][]driver.Value{{"1"}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BIGINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBigintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosBigint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBigintType,
				},
			},
			wantErr: false,
		},

		//tiny int unsigned
		{
			name: "utiny_nil",
			args: args{
				src: [][]driver.Value{{nil}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UTINYINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUTinyintType,
					},
				},
			},
			want: [][]driver.Value{{nil}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUTinyintType,
				},
			},
			wantErr: false,
		},
		{
			name: "utiny_err",
			args: args{
				src: [][]driver.Value{{[]int{123}}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UTINYINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUTinyintType,
					},
				},
			},
			want: [][]driver.Value{{}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUTinyintType,
				},
			},
			wantErr: true,
		},
		{
			name: "utiny_bool_1",
			args: args{
				src: [][]driver.Value{{true}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UTINYINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUTinyintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUTinyint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUTinyintType,
				},
			},
			wantErr: false,
		},
		{
			name: "utiny_bool_0",
			args: args{
				src: [][]driver.Value{{false}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UTINYINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUTinyintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUTinyint(0)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUTinyintType,
				},
			},
			wantErr: false,
		},
		{
			name: "utiny_float_1",
			args: args{
				src: [][]driver.Value{{float32(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UTINYINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUTinyintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUTinyint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUTinyintType,
				},
			},
			wantErr: false,
		},
		{
			name: "utiny_int_1",
			args: args{
				src: [][]driver.Value{{int(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UTINYINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUTinyintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUTinyint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUTinyintType,
				},
			},
			wantErr: false,
		},
		{
			name: "utiny_uint_1",
			args: args{
				src: [][]driver.Value{{uint(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UTINYINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUTinyintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUTinyint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUTinyintType,
				},
			},
			wantErr: false,
		},
		{
			name: "utiny_string_1",
			args: args{
				src: [][]driver.Value{{"1"}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UTINYINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUTinyintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUTinyint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUTinyintType,
				},
			},
			wantErr: false,
		},
		//small int unsigned
		{
			name: "usmall_nil",
			args: args{
				src: [][]driver.Value{{nil}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_USMALLINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUSmallintType,
					},
				},
			},
			want: [][]driver.Value{{nil}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUSmallintType,
				},
			},
			wantErr: false,
		},
		{
			name: "usmall_err",
			args: args{
				src: [][]driver.Value{{[]int{123}}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_USMALLINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUSmallintType,
					},
				},
			},
			want: [][]driver.Value{{}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUSmallintType,
				},
			},
			wantErr: true,
		},
		{
			name: "usmall_bool_1",
			args: args{
				src: [][]driver.Value{{true}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_USMALLINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUSmallintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUSmallint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUSmallintType,
				},
			},
			wantErr: false,
		},
		{
			name: "usmall_bool_0",
			args: args{
				src: [][]driver.Value{{false}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_USMALLINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUSmallintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUSmallint(0)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUSmallintType,
				},
			},
			wantErr: false,
		},
		{
			name: "usmall_float_1",
			args: args{
				src: [][]driver.Value{{float32(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_USMALLINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUSmallintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUSmallint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUSmallintType,
				},
			},
			wantErr: false,
		},
		{
			name: "usmall_int_1",
			args: args{
				src: [][]driver.Value{{int(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_USMALLINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUSmallintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUSmallint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUSmallintType,
				},
			},
			wantErr: false,
		},
		{
			name: "usmall_uint_1",
			args: args{
				src: [][]driver.Value{{uint(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_USMALLINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUSmallintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUSmallint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUSmallintType,
				},
			},
			wantErr: false,
		},
		{
			name: "usmall_string_1",
			args: args{
				src: [][]driver.Value{{"1"}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_USMALLINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUSmallintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUSmallint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUSmallintType,
				},
			},
			wantErr: false,
		},
		//int unsigned
		{
			name: "uint_nil",
			args: args{
				src: [][]driver.Value{{nil}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUIntType,
					},
				},
			},
			want: [][]driver.Value{{nil}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUIntType,
				},
			},
			wantErr: false,
		},
		{
			name: "uint_err",
			args: args{
				src: [][]driver.Value{{[]int{123}}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUIntType,
					},
				},
			},
			want: [][]driver.Value{{}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUIntType,
				},
			},
			wantErr: true,
		},
		{
			name: "uint_bool_1",
			args: args{
				src: [][]driver.Value{{true}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUIntType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUInt(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUIntType,
				},
			},
			wantErr: false,
		},
		{
			name: "uint_bool_0",
			args: args{
				src: [][]driver.Value{{false}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUIntType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUInt(0)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUIntType,
				},
			},
			wantErr: false,
		},
		{
			name: "uint_float_1",
			args: args{
				src: [][]driver.Value{{float32(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUIntType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUInt(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUIntType,
				},
			},
			wantErr: false,
		},
		{
			name: "uint_int_1",
			args: args{
				src: [][]driver.Value{{int(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUIntType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUInt(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUIntType,
				},
			},
			wantErr: false,
		},
		{
			name: "uint_uint_1",
			args: args{
				src: [][]driver.Value{{uint(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUIntType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUInt(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUIntType,
				},
			},
			wantErr: false,
		},
		{
			name: "uint_string_1",
			args: args{
				src: [][]driver.Value{{"1"}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUIntType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUInt(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUIntType,
				},
			},
			wantErr: false,
		},
		//big int unsigned
		{
			name: "ubig_nil",
			args: args{
				src: [][]driver.Value{{nil}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UBIGINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUBigintType,
					},
				},
			},
			want: [][]driver.Value{{nil}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUBigintType,
				},
			},
			wantErr: false,
		},
		{
			name: "ubig_err",
			args: args{
				src: [][]driver.Value{{[]int{123}}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UBIGINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUBigintType,
					},
				},
			},
			want: [][]driver.Value{{}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUBigintType,
				},
			},
			wantErr: true,
		},
		{
			name: "ubig_bool_1",
			args: args{
				src: [][]driver.Value{{true}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UBIGINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUBigintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUBigint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUBigintType,
				},
			},
			wantErr: false,
		},
		{
			name: "ubig_bool_0",
			args: args{
				src: [][]driver.Value{{false}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UBIGINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUBigintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUBigint(0)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUBigintType,
				},
			},
			wantErr: false,
		},
		{
			name: "ubig_float_1",
			args: args{
				src: [][]driver.Value{{float32(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UBIGINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUBigintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUBigint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUBigintType,
				},
			},
			wantErr: false,
		},
		{
			name: "ubig_int_1",
			args: args{
				src: [][]driver.Value{{int(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UBIGINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUBigintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUBigint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUBigintType,
				},
			},
			wantErr: false,
		},
		{
			name: "ubig_uint_1",
			args: args{
				src: [][]driver.Value{{uint(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UBIGINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUBigintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUBigint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUBigintType,
				},
			},
			wantErr: false,
		},
		{
			name: "ubig_string_1",
			args: args{
				src: [][]driver.Value{{"1"}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_UBIGINT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosUBigintType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosUBigint(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosUBigintType,
				},
			},
			wantErr: false,
		},
		//float
		{
			name: "float_nil",
			args: args{
				src: [][]driver.Value{{nil}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_FLOAT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosFloatType,
					},
				},
			},
			want: [][]driver.Value{{nil}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosFloatType,
				},
			},
			wantErr: false,
		},
		{
			name: "float_err",
			args: args{
				src: [][]driver.Value{{[]int{123}}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_FLOAT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosFloatType,
					},
				},
			},
			want: [][]driver.Value{{}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosFloatType,
				},
			},
			wantErr: true,
		},
		{
			name: "float_bool_1",
			args: args{
				src: [][]driver.Value{{true}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_FLOAT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosFloatType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosFloat(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosFloatType,
				},
			},
			wantErr: false,
		},
		{
			name: "float_bool_0",
			args: args{
				src: [][]driver.Value{{false}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_FLOAT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosFloatType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosFloat(0)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosFloatType,
				},
			},
			wantErr: false,
		},
		{
			name: "float_float_1",
			args: args{
				src: [][]driver.Value{{float32(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_FLOAT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosFloatType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosFloat(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosFloatType,
				},
			},
			wantErr: false,
		},
		{
			name: "float_int_1",
			args: args{
				src: [][]driver.Value{{int(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_FLOAT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosFloatType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosFloat(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosFloatType,
				},
			},
			wantErr: false,
		},
		{
			name: "float_uint_1",
			args: args{
				src: [][]driver.Value{{uint(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_FLOAT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosFloatType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosFloat(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosFloatType,
				},
			},
			wantErr: false,
		},
		{
			name: "float_string_1",
			args: args{
				src: [][]driver.Value{{"1"}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_FLOAT,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosFloatType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosFloat(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosFloatType,
				},
			},
			wantErr: false,
		},
		//double
		{
			name: "double_nil",
			args: args{
				src: [][]driver.Value{{nil}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_DOUBLE,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosDoubleType,
					},
				},
			},
			want: [][]driver.Value{{nil}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosDoubleType,
				},
			},
			wantErr: false,
		},
		{
			name: "double_err",
			args: args{
				src: [][]driver.Value{{[]int{123}}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_DOUBLE,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosDoubleType,
					},
				},
			},
			want: [][]driver.Value{{}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosDoubleType,
				},
			},
			wantErr: true,
		},
		{
			name: "double_bool_1",
			args: args{
				src: [][]driver.Value{{true}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_DOUBLE,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosDoubleType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosDouble(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosDoubleType,
				},
			},
			wantErr: false,
		},
		{
			name: "double_bool_0",
			args: args{
				src: [][]driver.Value{{false}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_DOUBLE,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosDoubleType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosDouble(0)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosDoubleType,
				},
			},
			wantErr: false,
		},
		{
			name: "double_float_1",
			args: args{
				src: [][]driver.Value{{float32(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_DOUBLE,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosDoubleType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosDouble(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosDoubleType,
				},
			},
			wantErr: false,
		},
		{
			name: "double_int_1",
			args: args{
				src: [][]driver.Value{{int(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_DOUBLE,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosDoubleType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosDouble(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosDoubleType,
				},
			},
			wantErr: false,
		},
		{
			name: "double_uint_1",
			args: args{
				src: [][]driver.Value{{uint(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_DOUBLE,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosDoubleType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosDouble(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosDoubleType,
				},
			},
			wantErr: false,
		},
		{
			name: "double_string_1",
			args: args{
				src: [][]driver.Value{{"1"}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_DOUBLE,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosDoubleType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosDouble(1)}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosDoubleType,
				},
			},
			wantErr: false,
		},
		//binary
		{
			name: "binary_nil",
			args: args{
				src: [][]driver.Value{{nil}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BINARY,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBinaryType,
					},
				},
			},
			want: [][]driver.Value{{nil}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBinaryType,
				},
			},
			wantErr: false,
		},
		{
			name: "double_err",
			args: args{
				src: [][]driver.Value{{[]int{123}}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BINARY,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBinaryType,
					},
				},
			},
			want: [][]driver.Value{{}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosBinaryType,
				},
			},
			wantErr: true,
		},
		{
			name: "binary_string_chinese",
			args: args{
				src: [][]driver.Value{{"中文"}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BINARY,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBinaryType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosBinary("中文")}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type:   types.TaosBinaryType,
					MaxLen: 6,
				},
			},
			wantErr: false,
		},
		{
			name: "binary_bytes_chinese",
			args: args{
				src: [][]driver.Value{{[]byte("中文")}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_BINARY,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosBinaryType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosBinary("中文")}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type:   types.TaosBinaryType,
					MaxLen: 6,
				},
			},
			wantErr: false,
		},
		//nchar
		{
			name: "nchar_nil",
			args: args{
				src: [][]driver.Value{{nil}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_NCHAR,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosNcharType,
					},
				},
			},
			want: [][]driver.Value{{nil}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosNcharType,
				},
			},
			wantErr: false,
		},
		{
			name: "double_err",
			args: args{
				src: [][]driver.Value{{[]int{123}}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_NCHAR,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosNcharType,
					},
				},
			},
			want: [][]driver.Value{{}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosNcharType,
				},
			},
			wantErr: true,
		},
		{
			name: "nchar_string_chinese",
			args: args{
				src: [][]driver.Value{{"中文"}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_NCHAR,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosNcharType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosNchar("中文")}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type:   types.TaosNcharType,
					MaxLen: 6,
				},
			},
			wantErr: false,
		},
		{
			name: "nchar_bytes_chinese",
			args: args{
				src: [][]driver.Value{{[]byte("中文")}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_NCHAR,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosNcharType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosNchar("中文")}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type:   types.TaosNcharType,
					MaxLen: 6,
				},
			},
			wantErr: false,
		},
		//timestamp
		{
			name: "ts_nil",
			args: args{
				src: [][]driver.Value{{nil}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosTimestampType,
					},
				},
			},
			want: [][]driver.Value{{nil}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosTimestampType,
				},
			},
			wantErr: false,
		},
		{
			name: "ts_err",
			args: args{
				src: [][]driver.Value{{[]int{123}}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosTimestampType,
					},
				},
			},
			want: [][]driver.Value{{}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosTimestampType,
				},
			},
			wantErr: true,
		},
		{
			name: "ts_float_1",
			args: args{
				src: [][]driver.Value{{float32(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
						Precision: common.PrecisionMilliSecond,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosTimestampType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosTimestamp{
				T:         time.Unix(0, 1e6).UTC(),
				Precision: common.PrecisionMilliSecond,
			}}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosTimestampType,
				},
			},
			wantErr: false,
		},
		{
			name: "ts_int_1",
			args: args{
				src: [][]driver.Value{{int32(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
						Precision: common.PrecisionMilliSecond,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosTimestampType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosTimestamp{
				T:         time.Unix(0, 1e6).UTC(),
				Precision: common.PrecisionMilliSecond,
			}}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosTimestampType,
				},
			},
			wantErr: false,
		},
		{
			name: "ts_uint_1",
			args: args{
				src: [][]driver.Value{{uint32(1)}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
						Precision: common.PrecisionMilliSecond,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosTimestampType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosTimestamp{
				T:         time.Unix(0, 1e6).UTC(),
				Precision: common.PrecisionMilliSecond,
			}}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosTimestampType,
				},
			},
			wantErr: false,
		},
		{
			name: "ts_string_1",
			args: args{
				src: [][]driver.Value{{"1970-01-01T00:00:00.001Z"}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
						Precision: common.PrecisionMilliSecond,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosTimestampType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosTimestamp{
				T:         time.Unix(0, 1e6).UTC(),
				Precision: common.PrecisionMilliSecond,
			}}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosTimestampType,
				},
			},
			wantErr: false,
		},
		{
			name: "ts_ts_1",
			args: args{
				src: [][]driver.Value{{time.Unix(0, 1e6).UTC()}},
				fields: []*wrapper.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
						Precision: common.PrecisionMilliSecond,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosTimestampType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosTimestamp{
				T:         time.Unix(0, 1e6).UTC(),
				Precision: common.PrecisionMilliSecond,
			}}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosTimestampType,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := stmtConvert(tt.args.src, tt.args.fields, tt.args.fieldTypes)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			if err != nil {
				t.Error(err)
				return
			}
			assert.Equal(t, tt.want, tt.args.src)
			assert.Equal(t, tt.wantFieldTypes, tt.args.fieldTypes)
		})
	}
}

func Test_stmtParseColumn(t *testing.T) {
	type args struct {
		columns    json.RawMessage
		fields     []*wrapper.StmtField
		fieldTypes []*types.ColumnType
	}
	tests := []struct {
		name    string
		args    args
		want    [][]driver.Value
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "all",
			args: args{
				columns: json.RawMessage("[[null,null],[true,false],[1,11],[2,22],[3,33],[4,44],[5,55],[6,66],[7,77],[8,88],[9,99],[10,1010],[\"b\",\"bb\"],[\"n\",\"nn\"],[1,\"2022-08-09T02:35:20Z\"]]"),
				fields: []*wrapper.StmtField{
					{FieldType: common.TSDB_DATA_TYPE_BOOL},
					{FieldType: common.TSDB_DATA_TYPE_BOOL},
					{FieldType: common.TSDB_DATA_TYPE_TINYINT},
					{FieldType: common.TSDB_DATA_TYPE_SMALLINT},
					{FieldType: common.TSDB_DATA_TYPE_INT},
					{FieldType: common.TSDB_DATA_TYPE_BIGINT},
					{FieldType: common.TSDB_DATA_TYPE_UTINYINT},
					{FieldType: common.TSDB_DATA_TYPE_USMALLINT},
					{FieldType: common.TSDB_DATA_TYPE_UINT},
					{FieldType: common.TSDB_DATA_TYPE_UBIGINT},
					{FieldType: common.TSDB_DATA_TYPE_FLOAT},
					{FieldType: common.TSDB_DATA_TYPE_DOUBLE},
					{FieldType: common.TSDB_DATA_TYPE_BINARY},
					{FieldType: common.TSDB_DATA_TYPE_NCHAR},
					{FieldType: common.TSDB_DATA_TYPE_TIMESTAMP, Precision: common.PrecisionMilliSecond},
				},
				fieldTypes: []*types.ColumnType{
					{Type: types.TaosBoolType},
					{Type: types.TaosBoolType},
					{Type: types.TaosTinyintType},
					{Type: types.TaosSmallintType},
					{Type: types.TaosIntType},
					{Type: types.TaosBigintType},
					{Type: types.TaosUTinyintType},
					{Type: types.TaosUSmallintType},
					{Type: types.TaosUIntType},
					{Type: types.TaosUBigintType},
					{Type: types.TaosFloatType},
					{Type: types.TaosDoubleType},
					{Type: types.TaosBinaryType},
					{Type: types.TaosNcharType},
					{Type: types.TaosTimestampType},
				},
			},
			want: [][]driver.Value{
				{nil, nil},
				{types.TaosBool(true), types.TaosBool(false)},
				{types.TaosTinyint(1), types.TaosTinyint(11)},
				{types.TaosSmallint(2), types.TaosSmallint(22)},
				{types.TaosInt(3), types.TaosInt(33)},
				{types.TaosBigint(4), types.TaosBigint(44)},
				{types.TaosUTinyint(5), types.TaosUTinyint(55)},
				{types.TaosUSmallint(6), types.TaosUSmallint(66)},
				{types.TaosUInt(7), types.TaosUInt(77)},
				{types.TaosUBigint(8), types.TaosUBigint(88)},
				{types.TaosFloat(9), types.TaosFloat(99)},
				{types.TaosDouble(10), types.TaosDouble(1010)},
				{types.TaosBinary("b"), types.TaosBinary("bb")},
				{types.TaosNchar("n"), types.TaosNchar("nn")},
				{types.TaosTimestamp{T: time.Unix(0, 1e6)}, types.TaosTimestamp{T: time.Unix(1660012520, 0).UTC()}},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stmtParseColumn(tt.args.columns, tt.args.fields, tt.args.fieldTypes)
			if !tt.wantErr(t, err, fmt.Sprintf("stmtParseColumn(%v, %v, %v)", tt.args.columns, tt.args.fields, tt.args.fieldTypes)) {
				return
			}
			assert.Equalf(t, tt.want, got, "stmtParseColumn(%v, %v, %v)", tt.args.columns, tt.args.fields, tt.args.fieldTypes)
		})
	}
}

func Test_stmtParseTag(t *testing.T) {
	type args struct {
		tags   json.RawMessage
		fields []*wrapper.StmtField
	}
	tests := []struct {
		name    string
		args    args
		want    []driver.Value
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "all",
			args: args{
				tags: json.RawMessage(`[null,true,1,2,3,4,5,6,7,8,9,10,"b","n","2022-08-09T02:35:20Z","{\"a\":\"b\"}"]`),
				fields: []*wrapper.StmtField{
					{FieldType: common.TSDB_DATA_TYPE_BOOL},
					{FieldType: common.TSDB_DATA_TYPE_BOOL},
					{FieldType: common.TSDB_DATA_TYPE_TINYINT},
					{FieldType: common.TSDB_DATA_TYPE_SMALLINT},
					{FieldType: common.TSDB_DATA_TYPE_INT},
					{FieldType: common.TSDB_DATA_TYPE_BIGINT},
					{FieldType: common.TSDB_DATA_TYPE_UTINYINT},
					{FieldType: common.TSDB_DATA_TYPE_USMALLINT},
					{FieldType: common.TSDB_DATA_TYPE_UINT},
					{FieldType: common.TSDB_DATA_TYPE_UBIGINT},
					{FieldType: common.TSDB_DATA_TYPE_FLOAT},
					{FieldType: common.TSDB_DATA_TYPE_DOUBLE},
					{FieldType: common.TSDB_DATA_TYPE_BINARY},
					{FieldType: common.TSDB_DATA_TYPE_NCHAR},
					{FieldType: common.TSDB_DATA_TYPE_TIMESTAMP, Precision: common.PrecisionMilliSecond},
					{FieldType: common.TSDB_DATA_TYPE_JSON},
				},
			},
			want: []driver.Value{
				nil,
				types.TaosBool(true),
				types.TaosTinyint(1),
				types.TaosSmallint(2),
				types.TaosInt(3),
				types.TaosBigint(4),
				types.TaosUTinyint(5),
				types.TaosUSmallint(6),
				types.TaosUInt(7),
				types.TaosUBigint(8),
				types.TaosFloat(9),
				types.TaosDouble(10),
				types.TaosBinary("b"),
				types.TaosNchar("n"),
				types.TaosTimestamp{T: time.Unix(1660012520, 0).UTC()},
				types.TaosJson(`{"a":"b"}`),
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stmtParseTag(tt.args.tags, tt.args.fields)
			if !tt.wantErr(t, err, fmt.Sprintf("stmtParseTag(%v, %v)", tt.args.tags, tt.args.fields)) {
				return
			}
			assert.Equalf(t, tt.want, got, "stmtParseTag(%v, %v)", tt.args.tags, tt.args.fields)
		})
	}
}

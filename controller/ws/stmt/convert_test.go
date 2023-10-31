package stmt

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	stmtCommon "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/types"
)

func Test_stmtConvert(t *testing.T) {
	type args struct {
		src        [][]driver.Value
		fields     []*stmtCommon.StmtField
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
			name: "binary_err",
			args: args{
				src: [][]driver.Value{{[]int{123}}},
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
				fields: []*stmtCommon.StmtField{
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
		// varbinary
		{
			name: "varbinary_nil",
			args: args{
				src: [][]driver.Value{{nil}},
				fields: []*stmtCommon.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_VARBINARY,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosVarBinaryType,
					},
				},
			},
			want: [][]driver.Value{{nil}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosVarBinaryType,
				},
			},
			wantErr: false,
		},
		{
			name: "varbinary_err",
			args: args{
				src: [][]driver.Value{{[]int{123}}},
				fields: []*stmtCommon.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_VARBINARY,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosVarBinaryType,
					},
				},
			},
			want: [][]driver.Value{{}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosVarBinaryType,
				},
			},
			wantErr: true,
		},
		{
			name: "varbinary_string_chinese",
			args: args{
				src: [][]driver.Value{{"中文"}},
				fields: []*stmtCommon.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_VARBINARY,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosVarBinaryType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosVarBinary("中文")}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type:   types.TaosVarBinaryType,
					MaxLen: 6,
				},
			},
			wantErr: false,
		},
		{
			name: "varbinary_bytes_chinese",
			args: args{
				src: [][]driver.Value{{[]byte("中文")}},
				fields: []*stmtCommon.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_VARBINARY,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosVarBinaryType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosVarBinary("中文")}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type:   types.TaosVarBinaryType,
					MaxLen: 6,
				},
			},
			wantErr: false,
		},
		// geometry
		{
			name: "geometry_nil",
			args: args{
				src: [][]driver.Value{{nil}},
				fields: []*stmtCommon.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_GEOMETRY,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosGeometryType,
					},
				},
			},
			want: [][]driver.Value{{nil}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosGeometryType,
				},
			},
			wantErr: false,
		},
		{
			name: "geometry_err",
			args: args{
				src: [][]driver.Value{{[]int{123}}},
				fields: []*stmtCommon.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_GEOMETRY,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosGeometryType,
					},
				},
			},
			want: [][]driver.Value{{}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type: types.TaosGeometryType,
				},
			},
			wantErr: true,
		},
		{
			name: "geometry_bytes",
			args: args{
				src: [][]driver.Value{{[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}}},
				fields: []*stmtCommon.StmtField{
					{
						FieldType: common.TSDB_DATA_TYPE_GEOMETRY,
					},
				},
				fieldTypes: []*types.ColumnType{
					{
						Type: types.TaosGeometryType,
					},
				},
			},
			want: [][]driver.Value{{types.TaosGeometry{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}}},
			wantFieldTypes: []*types.ColumnType{
				{
					Type:   types.TaosGeometryType,
					MaxLen: 21,
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
		fields     []*stmtCommon.StmtField
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
				columns: json.RawMessage(`[
    [
        null,
        null
    ],
    [
        true,
        false
    ],
    [
        1,
        11
    ],
    [
        2,
        22
    ],
    [
        3,
        33
    ],
    [
        4,
        44
    ],
    [
        5,
        55
    ],
    [
        6,
        66
    ],
    [
        7,
        77
    ],
    [
        8,
        88
    ],
    [
        9,
        99
    ],
    [
        10,
        1010
    ],
    [
        "b",
        "bb"
    ],
    [
        "n",
        "nn"
    ],
    [
        1,
        "2022-08-09T02:35:20Z"
    ],
    [
        "746573745f76617262696e617279",
        null
    ],
    [
        "010100000000000000000059400000000000005940",
        null
    ]
]`),
				fields: []*stmtCommon.StmtField{
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
					{FieldType: common.TSDB_DATA_TYPE_VARBINARY},
					{FieldType: common.TSDB_DATA_TYPE_GEOMETRY},
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
					{Type: types.TaosVarBinaryType},
					{Type: types.TaosGeometryType},
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
				{types.TaosVarBinary{0x74, 0x65, 0x73, 0x74, 0x5f, 0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79}, nil},
				{types.TaosGeometry{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, nil},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := StmtParseColumn(tt.args.columns, tt.args.fields, tt.args.fieldTypes)
			if !tt.wantErr(t, err, fmt.Sprintf("StmtParseColumn(%v, %v, %v)", tt.args.columns, tt.args.fields, tt.args.fieldTypes)) {
				return
			}
			assert.Equalf(t, tt.want, got, "StmtParseColumn(%v, %v, %v)", tt.args.columns, tt.args.fields, tt.args.fieldTypes)
		})
	}
}

func Test_stmtParseTag(t *testing.T) {
	type args struct {
		tags   json.RawMessage
		fields []*stmtCommon.StmtField
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
				tags: json.RawMessage(`[
    null,
    true,
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    "b",
    "n",
    "2022-08-09T02:35:20Z",
    "{\"a\":\"b\"}",
    "746573745f76617262696e617279",
    "010100000000000000000059400000000000005940"
]`),
				fields: []*stmtCommon.StmtField{
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
					{FieldType: common.TSDB_DATA_TYPE_VARBINARY},
					{FieldType: common.TSDB_DATA_TYPE_GEOMETRY},
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
				types.TaosVarBinary{0x74, 0x65, 0x73, 0x74, 0x5f, 0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79},
				types.TaosGeometry{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := StmtParseTag(tt.args.tags, tt.args.fields)
			if !tt.wantErr(t, err, fmt.Sprintf("StmtParseTag(%v, %v)", tt.args.tags, tt.args.fields)) {
				return
			}
			assert.Equalf(t, tt.want, got, "StmtParseTag(%v, %v)", tt.args.tags, tt.args.fields)
		})
	}
}

func TestBlockConvert(t *testing.T) {
	data := []byte{
		0x01, 0x00, 0x00, 0x00,
		0x64, 0x02, 0x00, 0x00,
		0x03, 0x00, 0x00, 0x00,
		0x11, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		//types
		0x09, 0x08, 0x00, 0x00, 0x00,
		0x01, 0x01, 0x00, 0x00, 0x00,
		0x02, 0x01, 0x00, 0x00, 0x00,
		0x03, 0x02, 0x00, 0x00, 0x00,
		0x04, 0x04, 0x00, 0x00, 0x00,
		0x05, 0x08, 0x00, 0x00, 0x00,
		0x0b, 0x01, 0x00, 0x00, 0x00,
		0x0c, 0x02, 0x00, 0x00, 0x00,
		0x0d, 0x04, 0x00, 0x00, 0x00,
		0x0e, 0x08, 0x00, 0x00, 0x00,
		0x06, 0x04, 0x00, 0x00, 0x00,
		0x07, 0x08, 0x00, 0x00, 0x00,
		0x08, 0x00, 0x00, 0x00, 0x00,
		0x0a, 0x00, 0x00, 0x00, 0x00,
		0x10, 0x00, 0x00, 0x00, 0x00,
		0x14, 0x00, 0x00, 0x00, 0x00,
		0x0f, 0x00, 0x00, 0x00, 0x00,
		//lengths
		0x18, 0x00, 0x00, 0x00,
		0x03, 0x00, 0x00, 0x00,
		0x03, 0x00, 0x00, 0x00,
		0x06, 0x00, 0x00, 0x00,
		0x0c, 0x00, 0x00, 0x00,
		0x18, 0x00, 0x00, 0x00,
		0x03, 0x00, 0x00, 0x00,
		0x06, 0x00, 0x00, 0x00,
		0x0c, 0x00, 0x00, 0x00,
		0x18, 0x00, 0x00, 0x00,
		0x0c, 0x00, 0x00, 0x00,
		0x18, 0x00, 0x00, 0x00,
		0x1a, 0x00, 0x00, 0x00,
		0x54, 0x00, 0x00, 0x00,
		0x20, 0x00, 0x00, 0x00,
		0x2e, 0x00, 0x00, 0x00,
		0x12, 0x00, 0x00, 0x00,
		// ts
		0x40,
		0xe8, 0xbf, 0x1f, 0xf4, 0x83, 0x01, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0xb8, 0xc7, 0x1f, 0xf4, 0x83, 0x01, 0x00, 0x00,

		// bool
		0x40,
		0x01,
		0x00,
		0x01,

		// i8
		0x40,
		0x01,
		0x00,
		0x01,

		//int16
		0x40,
		0x01, 0x00,
		0x00, 0x00,
		0x01, 0x00,

		//int32
		0x40,
		0x01, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00,

		//int64
		0x40,
		0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

		//uint8
		0x40,
		0x01,
		0x00,
		0x01,

		//uint16
		0x40,
		0x01, 0x00,
		0x00, 0x00,
		0x01, 0x00,

		//uint32
		0x40,
		0x01, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00,

		//uint64
		0x40,
		0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

		//float
		0x40,
		0x00, 0x00, 0x80, 0x3f,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x80, 0x3f,

		//double
		0x40,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,

		//binary
		0x00, 0x00, 0x00, 0x00,
		0xff, 0xff, 0xff, 0xff,
		0x0d, 0x00, 0x00, 0x00,
		0x0b, 0x00,
		0x74, 0x65, 0x73, 0x74, 0x5f, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,
		0x0b, 0x00,
		0x74, 0x65, 0x73, 0x74, 0x5f, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,

		//nchar
		0x00, 0x00, 0x00, 0x00,
		0xff, 0xff, 0xff, 0xff,
		0x2a, 0x00, 0x00, 0x00,
		0x28, 0x00,
		0x74, 0x00, 0x00, 0x00, 0x65, 0x00, 0x00, 0x00, 0x73, 0x00,
		0x00, 0x00, 0x74, 0x00, 0x00, 0x00, 0x5f, 0x00, 0x00, 0x00,
		0x6e, 0x00, 0x00, 0x00, 0x63, 0x00, 0x00, 0x00, 0x68, 0x00,
		0x00, 0x00, 0x61, 0x00, 0x00, 0x00, 0x72, 0x00, 0x00, 0x00,
		0x28, 0x00,
		0x74, 0x00, 0x00, 0x00, 0x65, 0x00, 0x00, 0x00, 0x73, 0x00,
		0x00, 0x00, 0x74, 0x00, 0x00, 0x00, 0x5f, 0x00, 0x00, 0x00,
		0x6e, 0x00, 0x00, 0x00, 0x63, 0x00, 0x00, 0x00, 0x68, 0x00,
		0x00, 0x00, 0x61, 0x00, 0x00, 0x00, 0x72, 0x00, 0x00, 0x00,

		//varbinary
		0x00, 0x00, 0x00, 0x00,
		0xff, 0xff, 0xff, 0xff,
		0x10, 0x00, 0x00, 0x00,
		0x0e, 0x00,
		0x74, 0x65, 0x73, 0x74, 0x5f, 0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,
		0x0e, 0x00,
		0x74, 0x65, 0x73, 0x74, 0x5f, 0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,

		//geometry
		0x00, 0x00, 0x00, 0x00,
		0xff, 0xff, 0xff, 0xff,
		0x17, 0x00, 0x00, 0x00,
		0x15, 0x00,
		0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
		0x15, 0x00,
		0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,

		//json
		0x00, 0x00, 0x00, 0x00,
		0xff, 0xff, 0xff, 0xff,
		0x09, 0x00, 0x00, 0x00,
		0x07, 0x00,
		0x7b, 0x22, 0x61, 0x22, 0x3a, 0x31, 0x7d,
		0x07, 0x00,
		0x7b, 0x22, 0x61, 0x22, 0x3a, 0x31, 0x7d,
	}
	fields := []*stmtCommon.StmtField{
		{FieldType: common.TSDB_DATA_TYPE_TIMESTAMP, Precision: common.PrecisionMilliSecond},
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
		{FieldType: common.TSDB_DATA_TYPE_VARBINARY},
		{FieldType: common.TSDB_DATA_TYPE_GEOMETRY},
		{FieldType: common.TSDB_DATA_TYPE_JSON},
	}
	fieldTypes := []*types.ColumnType{
		{Type: types.TaosTimestampType},
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
		{Type: types.TaosVarBinaryType},
		{Type: types.TaosGeometryType},
		{Type: types.TaosJsonType},
	}
	want := [][]driver.Value{
		{
			types.TaosTimestamp{T: time.Unix(1666248065, 0)},
			nil,
			types.TaosTimestamp{T: time.Unix(1666248067, 0)},
		},
		{
			types.TaosBool(true),
			nil,
			types.TaosBool(true),
		},
		{
			types.TaosTinyint(1),
			nil,
			types.TaosTinyint(1),
		},
		{
			types.TaosSmallint(1),
			nil,
			types.TaosSmallint(1),
		},
		{
			types.TaosInt(1),
			nil,
			types.TaosInt(1),
		},
		{
			types.TaosBigint(1),
			nil,
			types.TaosBigint(1),
		},
		{
			types.TaosUTinyint(1),
			nil,
			types.TaosUTinyint(1),
		},
		{
			types.TaosUSmallint(1),
			nil,
			types.TaosUSmallint(1),
		},
		{
			types.TaosUInt(1),
			nil,
			types.TaosUInt(1),
		},
		{
			types.TaosUBigint(1),
			nil,
			types.TaosUBigint(1),
		},
		{
			types.TaosFloat(1),
			nil,
			types.TaosFloat(1),
		},
		{
			types.TaosDouble(1),
			nil,
			types.TaosDouble(1),
		},
		{
			types.TaosBinary("test_binary"),
			nil,
			types.TaosBinary("test_binary"),
		},
		{
			types.TaosNchar("test_nchar"),
			nil,
			types.TaosNchar("test_nchar"),
		},
		{
			types.TaosVarBinary("test_varbinary"),
			nil,
			types.TaosVarBinary("test_varbinary"),
		},
		{
			types.TaosGeometry{
				0x01,
				0x01,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x59,
				0x40,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x59,
				0x40,
			},
			nil,
			types.TaosGeometry{
				0x01,
				0x01,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x59,
				0x40,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x59,
				0x40,
			},
		},
		{
			types.TaosJson(`{"a":1}`),
			nil,
			types.TaosJson(`{"a":1}`),
		},
	}
	got := BlockConvert(unsafe.Pointer(&data[0]), 3, fields, fieldTypes)
	assert.Equal(t, want, got)
}

func TestStrToHex(t *testing.T) {
	testCases := []struct {
		input       string
		expected    []byte
		expectError bool
	}{
		{ // Test case 1: Valid hex string
			input:       "48656C6C6F20576F726C64", // "Hello World" in hex
			expected:    []byte{0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64},
			expectError: false,
		},
		{ // Test case 2: Empty string
			input:       "",
			expected:    nil,
			expectError: false,
		},
		{ // Test case 3: Invalid hex string
			input:       "123G", // Contains an invalid character 'G'
			expected:    nil,
			expectError: true,
		},
		{
			input:       "123",
			expected:    nil,
			expectError: true,
		},
	}

	for _, tc := range testCases {
		result, err := strToHex(tc.input)
		assert.Equal(t, tc.expected, result)
		if tc.expectError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}

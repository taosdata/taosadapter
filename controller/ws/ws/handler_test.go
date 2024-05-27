package ws

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	"github.com/taosdata/driver-go/v3/common/serializer"
	stmtCommon "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/types"
)

func Test_parseRowBlockInfo(t *testing.T) {
	b, err := serializer.SerializeRawBlock(
		[]*param.Param{
			param.NewParam(1).AddBool(true),
			param.NewParam(1).AddTinyint(1),
			param.NewParam(1).AddSmallint(1),
			param.NewParam(1).AddInt(1),
			param.NewParam(1).AddBigint(1),
			param.NewParam(1).AddFloat(1.1),
			param.NewParam(1).AddDouble(1.1),
			param.NewParam(1).AddBinary([]byte("California.SanFrancisco")),
			param.NewParam(1).AddNchar("California.SanFrancisco"),
			param.NewParam(1).AddUTinyint(1),
			param.NewParam(1).AddUSmallint(1),
			param.NewParam(1).AddUInt(1),
			param.NewParam(1).AddUBigint(1),
			param.NewParam(1).AddJson([]byte(`{"name":"taos"}`)),
			param.NewParam(1).AddVarBinary([]byte("California.SanFrancisco")),
		},
		param.NewColumnType(15).
			AddBool().
			AddTinyint().
			AddSmallint().
			AddInt().
			AddBigint().
			AddFloat().
			AddDouble().
			AddBinary(100).
			AddNchar(100).
			AddUTinyint().
			AddUSmallint().
			AddUInt().
			AddUBigint().
			AddJson(100).
			AddVarBinary(100),
	)
	assert.NoError(t, err)
	fields, fieldsType, err := parseRowBlockInfo(unsafe.Pointer(&b[0]), 15)
	assert.NoError(t, err)
	expectFields := []*stmtCommon.StmtField{
		{FieldType: common.TSDB_DATA_TYPE_BOOL},
		{FieldType: common.TSDB_DATA_TYPE_TINYINT},
		{FieldType: common.TSDB_DATA_TYPE_SMALLINT},
		{FieldType: common.TSDB_DATA_TYPE_INT},
		{FieldType: common.TSDB_DATA_TYPE_BIGINT},
		{FieldType: common.TSDB_DATA_TYPE_FLOAT},
		{FieldType: common.TSDB_DATA_TYPE_DOUBLE},
		{FieldType: common.TSDB_DATA_TYPE_BINARY},
		{FieldType: common.TSDB_DATA_TYPE_NCHAR},
		{FieldType: common.TSDB_DATA_TYPE_UTINYINT},
		{FieldType: common.TSDB_DATA_TYPE_USMALLINT},
		{FieldType: common.TSDB_DATA_TYPE_UINT},
		{FieldType: common.TSDB_DATA_TYPE_UBIGINT},
		{FieldType: common.TSDB_DATA_TYPE_JSON},
		{FieldType: common.TSDB_DATA_TYPE_VARBINARY},
	}
	assert.Equal(t, expectFields, fields)
	expectFieldsType := []*types.ColumnType{
		{Type: types.TaosBoolType},
		{Type: types.TaosTinyintType},
		{Type: types.TaosSmallintType},
		{Type: types.TaosIntType},
		{Type: types.TaosBigintType},
		{Type: types.TaosFloatType},
		{Type: types.TaosDoubleType},
		{Type: types.TaosBinaryType},
		{Type: types.TaosNcharType},
		{Type: types.TaosUTinyintType},
		{Type: types.TaosUSmallintType},
		{Type: types.TaosUIntType},
		{Type: types.TaosUBigintType},
		{Type: types.TaosJsonType},
		{Type: types.TaosBinaryType},
	}
	assert.Equal(t, expectFieldsType, fieldsType)
}

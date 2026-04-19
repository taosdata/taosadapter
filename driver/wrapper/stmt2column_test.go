package wrapper

import (
	"database/sql/driver"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/driver/common"
	stmtcommon "github.com/taosdata/taosadapter/v3/driver/common/stmt"
)

func TestTaosStmt2BindColumnBinaryProtocolErrors(t *testing.T) {
	err := TaosStmt2BindColumnBinary(nil, []byte{0x01, 0x02})
	assert.EqualError(t, err, "data length is less than 20")

	fields := []*stmtcommon.Stmt2AllField{
		{
			FieldType: common.TSDB_DATA_TYPE_INT,
			BindType:  stmtcommon.TAOS_FIELD_COL,
		},
	}
	data, err := stmtcommon.MarshalStmt2ColumnBinary([][]driver.Value{
		{int32(1)},
	}, fields)
	assert.NoError(t, err)
	data[0] = 0
	err = TaosStmt2BindColumnBinary(unsafe.Pointer(uintptr(1)), data)
	assert.ErrorContains(t, err, "total length not match")
}

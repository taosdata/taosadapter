package wrapper

import (
	"database/sql/driver"
	"encoding/binary"
	"os"
	"strings"
	"testing"

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
	err = TaosStmt2BindColumnBinary(nil, data)
	assert.ErrorContains(t, err, "total length not match")
}

func TestTaosStmt2BindColumnBinaryRejectsColumnNumBeyondColumnLength(t *testing.T) {
	data := make([]byte, stmtcommon.Stmt2ColumnDataPosition+64)
	binary.LittleEndian.PutUint32(data[stmtcommon.Stmt2ColumnTotalLengthPosition:], uint32(len(data)))
	binary.LittleEndian.PutUint32(data[stmtcommon.Stmt2ColumnRowCountPosition:], 7)
	binary.LittleEndian.PutUint32(data[stmtcommon.Stmt2ColumnTableCountPosition:], 1)
	binary.LittleEndian.PutUint32(data[stmtcommon.Stmt2ColumnFieldCountPosition:], 1)
	binary.LittleEndian.PutUint32(data[stmtcommon.Stmt2ColumnFieldOffsetPosition:], stmtcommon.Stmt2ColumnDataPosition)

	column := data[stmtcommon.Stmt2ColumnDataPosition:]
	binary.LittleEndian.PutUint32(column, 18)
	binary.LittleEndian.PutUint32(column[4:], uint32(common.TSDB_DATA_TYPE_INT))
	binary.LittleEndian.PutUint32(column[8:], 7)

	err := TaosStmt2BindColumnBinary(nil, data)
	assert.ErrorContains(t, err, "column 0 is_null array out of range")
}

func TestStmt2ColumnWrapperUsesDirectTaosLinkage(t *testing.T) {
	source, err := os.ReadFile("stmt2column.go")
	assert.NoError(t, err)
	content := string(source)

	assert.NotContains(t, content, "dlsym")
	assert.NotContains(t, content, "RTLD_DEFAULT")
	assert.NotContains(t, content, "<dlfcn.h>")
	assert.Contains(t, content, "taos_stmt2_bind_param_column(stmt, &bindv)")
	assert.True(t, strings.Contains(content, "TAOS_STMT2_COLUMN_BINDV bindv"))
}

func BenchmarkTaosStmt2BindColumnBinaryRejectsLargeInvalidPayload(b *testing.B) {
	data := make([]byte, 1<<20)
	binary.LittleEndian.PutUint32(data[stmtcommon.Stmt2ColumnTotalLengthPosition:], uint32(len(data)))
	binary.LittleEndian.PutUint32(data[stmtcommon.Stmt2ColumnRowCountPosition:], 1)
	binary.LittleEndian.PutUint32(data[stmtcommon.Stmt2ColumnTableCountPosition:], 1)
	binary.LittleEndian.PutUint32(data[stmtcommon.Stmt2ColumnFieldCountPosition:], 1)
	binary.LittleEndian.PutUint32(data[stmtcommon.Stmt2ColumnFieldOffsetPosition:], stmtcommon.Stmt2ColumnDataPosition)

	column := data[stmtcommon.Stmt2ColumnDataPosition:]
	binary.LittleEndian.PutUint32(column, 18)
	binary.LittleEndian.PutUint32(column[4:], uint32(common.TSDB_DATA_TYPE_INT))
	binary.LittleEndian.PutUint32(column[8:], 1)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := TaosStmt2BindColumnBinary(nil, data)
		if err == nil {
			b.Fatal("expected invalid payload error")
		}
	}
}

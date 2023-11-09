package parseblock

import (
	"database/sql/driver"
	"unsafe"

	"github.com/taosdata/driver-go/v3/common/parser"
	"github.com/taosdata/taosadapter/v3/tools"
)

func ParseBlock(data []byte, colTypes []uint8, rows int, precision int) (uint64, [][]driver.Value) {
	p0 := unsafe.Pointer(&data[0])
	id := *(*uint64)(p0)
	columnPtr := tools.AddPointer(p0, 8)
	result := parser.ReadBlock(columnPtr, rows, colTypes, precision)
	return id, result
}

func ParseTmqBlock(data []byte, colTypes []uint8, rows int, precision int) (uint64, uint64, [][]driver.Value) {
	p0 := unsafe.Pointer(&data[0])
	id := *(*uint64)(p0)
	messageID := *(*uint64)(tools.AddPointer(p0, 8))
	columnPtr := tools.AddPointer(p0, 16)
	result := parser.ReadBlock(columnPtr, rows, colTypes, precision)
	return id, messageID, result
}

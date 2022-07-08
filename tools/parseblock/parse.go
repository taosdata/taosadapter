package parseblock

/*
#include <taos.h>
*/
import "C"
import (
	"database/sql/driver"
	"unsafe"

	"github.com/taosdata/driver-go/v3/wrapper"
)

func ParseBlock(data []byte, colTypes []uint8, rows int, precision int) (uint64, [][]driver.Value) {
	id := *(*uint64)(unsafe.Pointer(*(*uintptr)(unsafe.Pointer(&data))))
	columnPtr := *(*uintptr)(unsafe.Pointer(&data)) + uintptr(8)
	result := wrapper.ReadBlock(unsafe.Pointer(columnPtr), rows, colTypes, precision)
	return id, result
}

func ParseTmqBlock(data []byte, colTypes []uint8, rows int, precision int) (uint64, uint64, [][]driver.Value) {
	p0 := *(*uintptr)(unsafe.Pointer(&data))
	id := *(*uint64)(unsafe.Pointer(p0))
	messageID := *(*uint64)(unsafe.Pointer(p0 + 8))
	columnPtr := *(*uintptr)(unsafe.Pointer(&data)) + uintptr(16)
	result := wrapper.ReadBlock(unsafe.Pointer(columnPtr), rows, colTypes, precision)
	return id, messageID, result
}

package parseblock

/*
#include <taos.h>
*/
import "C"
import (
	"database/sql/driver"
	"encoding/json"
	"math"
	"unsafe"

	"github.com/taosdata/driver-go/v2/common"
	"github.com/taosdata/driver-go/v2/wrapper"
)

func ParseBlock(data []byte, colTypes []int, lengths []int, rows int, precision int) (uint64, [][]driver.Value) {
	id := *(*uint64)(unsafe.Pointer(*(*uintptr)(unsafe.Pointer(&data))))
	columnPtr := *(*uintptr)(unsafe.Pointer(&data)) + uintptr(8)
	columns := len(lengths)
	result := make([][]driver.Value, rows)
	for column := 0; column < columns; column++ {
		for row := 0; row < rows; row++ {
			if column == 0 {
				result[row] = make([]driver.Value, columns)
			}
			rawPointer := unsafe.Pointer(columnPtr + uintptr(lengths[column]*row))
			switch colTypes[column] {
			case int(C.TSDB_DATA_TYPE_BOOL):
				value := *((*byte)(rawPointer))
				if value == wrapper.CBoolNull {
					result[row][column] = nil
				} else if value != 0 {
					result[row][column] = true
				} else {
					result[row][column] = false
				}
			case int(C.TSDB_DATA_TYPE_TINYINT):
				value := *((*int8)(rawPointer))
				if value == wrapper.CTinyintNull {
					result[row][column] = nil
				} else {
					result[row][column] = value
				}
			case int(C.TSDB_DATA_TYPE_SMALLINT):
				value := *((*int16)(rawPointer))
				if value == wrapper.CSmallintNull {
					result[row][column] = nil
				} else {
					result[row][column] = value
				}
			case int(C.TSDB_DATA_TYPE_INT):
				value := *((*int32)(rawPointer))
				if value == wrapper.CIntNull {
					result[row][column] = nil
				} else {
					result[row][column] = value
				}
			case int(C.TSDB_DATA_TYPE_BIGINT):
				value := *((*int64)(rawPointer))
				if value == wrapper.CBigintNull {
					result[row][column] = nil
				} else {
					result[row][column] = value
				}
			case int(C.TSDB_DATA_TYPE_UTINYINT):
				value := *((*uint8)(rawPointer))
				if value == wrapper.CTinyintUnsignedNull {
					result[row][column] = nil
				} else {
					result[row][column] = value
				}
			case int(C.TSDB_DATA_TYPE_USMALLINT):
				value := *((*uint16)(rawPointer))
				if value == wrapper.CSmallintUnsignedNull {
					result[row][column] = nil
				} else {
					result[row][column] = value
				}
			case int(C.TSDB_DATA_TYPE_UINT):
				value := *((*uint32)(rawPointer))
				if value == wrapper.CIntUnsignedNull {
					result[row][column] = nil
				} else {
					result[row][column] = value
				}
			case int(C.TSDB_DATA_TYPE_UBIGINT):
				value := *((*uint64)(rawPointer))
				if value == wrapper.CBigintUnsignedNull {
					result[row][column] = nil
				} else {
					result[row][column] = value
				}
			case int(C.TSDB_DATA_TYPE_FLOAT):
				value := *((*float32)(rawPointer))
				if math.IsNaN(float64(value)) {
					result[row][column] = nil
				} else {
					result[row][column] = value
				}
			case int(C.TSDB_DATA_TYPE_DOUBLE):
				value := *((*float64)(rawPointer))
				if math.IsNaN(value) {
					result[row][column] = nil
				} else {
					result[row][column] = value
				}
			case int(C.TSDB_DATA_TYPE_BINARY):
				clen := *((*int16)(rawPointer))
				rawPointer = unsafe.Pointer(uintptr(rawPointer) + 2)
				if clen == 1 {
					value := *((*byte)(rawPointer))
					if value == wrapper.CBinaryNull {
						result[row][column] = nil
					} else {
						result[row][column] = string(value)
					}
				} else {
					//func C.GoStringN(*C.char, C.int) string
					result[row][column] = C.GoStringN((*C.char)(rawPointer), C.int(clen))
				}
			case int(C.TSDB_DATA_TYPE_NCHAR):
				clen := *((*int16)(rawPointer))
				rawPointer = unsafe.Pointer(uintptr(rawPointer) + 2)
				if clen == 4 {
					isNil := true
					for i := 0; i < 4; i++ {
						if *((*byte)(unsafe.Pointer(uintptr(rawPointer) + uintptr(i)))) != wrapper.CNcharNull {
							isNil = false
							break
						}
					}
					if isNil {
						result[row][column] = nil
						continue
					}
				}
				result[row][column] = C.GoStringN((*C.char)(rawPointer), C.int(clen))
			case int(C.TSDB_DATA_TYPE_TIMESTAMP):
				value := *((*int64)(rawPointer))
				if value == wrapper.CTimestampNull {
					result[row][column] = nil
				} else {
					result[row][column] = common.TimestampConvertToTime(value, precision)
				}
			case int(C.TSDB_DATA_TYPE_JSON):
				clen := *((*int16)(rawPointer))
				rawPointer = unsafe.Pointer(uintptr(rawPointer) + 2)
				if clen == 4 {
					isNil := true
					for i := 0; i < 4; i++ {
						if *((*byte)(unsafe.Pointer(uintptr(rawPointer) + uintptr(i)))) != wrapper.CNcharNull {
							isNil = false
							break
						}
					}
					if isNil {
						result[row][column] = nil
						continue
					}
				}
				result[row][column] = json.RawMessage(C.GoBytes(rawPointer, C.int(clen)))
			}

		}
		columnPtr = columnPtr + uintptr(lengths[column]*rows)
	}
	return id, result
}

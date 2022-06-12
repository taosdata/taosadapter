package rest

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"time"
	"unsafe"

	"github.com/taosdata/driver-go/v2/common"
	"github.com/taosdata/driver-go/v2/types"
	"github.com/taosdata/driver-go/v2/wrapper"
)

func stmtConvert(src [][]driver.Value, fields []*wrapper.StmtField, fieldTypes []*types.ColumnType) error {
	for column := 0; column < len(src); column++ {
		switch fields[column].FieldType {
		case common.TSDB_DATA_TYPE_NULL:
			for row := 0; row < len(src[column]); row++ {
				src[column][row] = nil
			}
		case common.TSDB_DATA_TYPE_BOOL:
			for row := 0; row < len(src[column]); row++ {
				if src[column][row] == nil {
					continue
				}
				rv := reflect.ValueOf(src[column][row])
				switch rv.Kind() {
				case reflect.Bool:
					src[column][row] = types.TaosBool(rv.Bool())
				case reflect.Float32, reflect.Float64:
					src[column][row] = types.TaosBool(rv.Float() == 1)
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					src[column][row] = types.TaosBool(rv.Int() == 1)
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					src[column][row] = types.TaosBool(rv.Uint() == 1)
				case reflect.String:
					vv, err := strconv.ParseBool(rv.String())
					if err != nil {
						return err
					}
					src[column][row] = vv
				default:
					return fmt.Errorf("stmtConvert:%v can not convert to bool", src[column][row])
				}
			}
		case common.TSDB_DATA_TYPE_TINYINT:
			for row := 0; row < len(src[column]); row++ {
				if src[column][row] == nil {
					continue
				}
				rv := reflect.ValueOf(src[column][row])
				switch rv.Kind() {
				case reflect.Bool:
					if rv.Bool() == true {
						src[column][row] = types.TaosTinyint(1)
					} else {
						src[column][row] = types.TaosTinyint(0)
					}
				case reflect.Float32, reflect.Float64:
					src[column][row] = types.TaosTinyint(rv.Float())
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					src[column][row] = types.TaosTinyint(rv.Int())
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					src[column][row] = types.TaosTinyint(rv.Uint())
				case reflect.String:
					vv, err := strconv.ParseInt(rv.String(), 0, 8)
					if err != nil {
						return err
					}
					src[column][row] = types.TaosTinyint(vv)
				default:
					return fmt.Errorf("stmtConvert:%v can not convert to tinyint", src[column][row])
				}
			}
		case common.TSDB_DATA_TYPE_SMALLINT:
			for row := 0; row < len(src[column]); row++ {
				if src[column][row] == nil {
					continue
				}
				rv := reflect.ValueOf(src[column][row])
				switch rv.Kind() {
				case reflect.Bool:
					if rv.Bool() == true {
						src[column][row] = types.TaosSmallint(1)
					} else {
						src[column][row] = types.TaosSmallint(0)
					}
				case reflect.Float32, reflect.Float64:
					src[column][row] = types.TaosSmallint(rv.Float())
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					src[column][row] = types.TaosSmallint(rv.Int())
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					src[column][row] = types.TaosSmallint(rv.Uint())
				case reflect.String:
					vv, err := strconv.ParseInt(rv.String(), 0, 16)
					if err != nil {
						return err
					}
					src[column][row] = types.TaosSmallint(vv)
				default:
					return fmt.Errorf("stmtConvert:%v can not convert to smallint", src[column][row])
				}
			}
		case common.TSDB_DATA_TYPE_INT:
			for row := 0; row < len(src[column]); row++ {
				if src[column][row] == nil {
					continue
				}
				rv := reflect.ValueOf(src[column][row])
				switch rv.Kind() {
				case reflect.Bool:
					if rv.Bool() == true {
						src[column][row] = types.TaosInt(1)
					} else {
						src[column][row] = types.TaosInt(0)
					}
				case reflect.Float32, reflect.Float64:
					src[column][row] = types.TaosInt(rv.Float())
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					src[column][row] = types.TaosInt(rv.Int())
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					src[column][row] = types.TaosInt(rv.Uint())
				case reflect.String:
					vv, err := strconv.ParseInt(rv.String(), 0, 32)
					if err != nil {
						return err
					}
					src[column][row] = types.TaosInt(vv)
				default:
					return fmt.Errorf("stmtConvert:%v can not convert to int", src[column][row])
				}
			}
		case common.TSDB_DATA_TYPE_BIGINT:
			for row := 0; row < len(src[column]); row++ {
				if src[column][row] == nil {
					continue
				}
				rv := reflect.ValueOf(src[column][row])
				switch rv.Kind() {
				case reflect.Bool:
					if rv.Bool() == true {
						src[column][row] = types.TaosBigint(1)
					} else {
						src[column][row] = types.TaosBigint(0)
					}
				case reflect.Float32, reflect.Float64:
					src[column][row] = types.TaosBigint(rv.Float())
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					src[column][row] = types.TaosBigint(rv.Int())
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					src[column][row] = types.TaosBigint(rv.Uint())
				case reflect.String:
					vv, err := strconv.ParseInt(rv.String(), 0, 64)
					if err != nil {
						return err
					}
					src[column][row] = types.TaosBigint(vv)
				default:
					return fmt.Errorf("stmtConvert:%v can not convert to bigint", src[column][row])
				}
			}

		case common.TSDB_DATA_TYPE_FLOAT:
			for row := 0; row < len(src[column]); row++ {
				if src[column][row] == nil {
					continue
				}
				rv := reflect.ValueOf(src[column][row])
				switch rv.Kind() {
				case reflect.Bool:
					if rv.Bool() == true {
						src[column][row] = types.TaosFloat(1)
					} else {
						src[column][row] = types.TaosFloat(0)
					}
				case reflect.Float32, reflect.Float64:
					src[column][row] = types.TaosFloat(rv.Float())
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					src[column][row] = types.TaosFloat(rv.Int())
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					src[column][row] = types.TaosFloat(rv.Uint())
				case reflect.String:
					vv, err := strconv.ParseFloat(rv.String(), 32)
					if err != nil {
						return err
					}
					src[column][row] = types.TaosFloat(vv)
				default:
					return fmt.Errorf("stmtConvert:%v can not convert to float", src[column][row])
				}
			}
		case common.TSDB_DATA_TYPE_DOUBLE:
			for row := 0; row < len(src[column]); row++ {
				if src[column][row] == nil {
					continue
				}
				rv := reflect.ValueOf(src[column][row])
				switch rv.Kind() {
				case reflect.Bool:
					if rv.Bool() == true {
						src[column][row] = types.TaosDouble(1)
					} else {
						src[column][row] = types.TaosDouble(0)
					}
				case reflect.Float32, reflect.Float64:
					src[column][row] = types.TaosDouble(rv.Float())
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					src[column][row] = types.TaosDouble(rv.Int())
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					src[column][row] = types.TaosDouble(rv.Uint())
				case reflect.String:
					vv, err := strconv.ParseFloat(rv.String(), 64)
					if err != nil {
						return err
					}
					src[column][row] = types.TaosDouble(vv)
				default:
					return fmt.Errorf("stmtConvert:%v can not convert to double", src[column][row])
				}
			}
		case common.TSDB_DATA_TYPE_BINARY:
			for row := 0; row < len(src[column]); row++ {
				if src[column][row] == nil {
					continue
				}
				switch src[column][row].(type) {
				case string:
					src[column][row] = types.TaosBinary(src[column][row].(string))
				case []byte:
					src[column][row] = types.TaosBinary(src[column][row].([]byte))
				default:
					return fmt.Errorf("stmtConvert:%v can not convert to binary", src[column][row])
				}
				if fieldTypes != nil && len(src[column][row].(types.TaosBinary)) > fieldTypes[column].MaxLen {
					fieldTypes[column].MaxLen = len(src[column][row].(types.TaosBinary))
				}
			}
		case common.TSDB_DATA_TYPE_TIMESTAMP:
			for row := 0; row < len(src[column]); row++ {
				if src[column][row] == nil {
					continue
				}
				t, is := src[column][row].(time.Time)
				if is {
					src[column][row] = types.TaosTimestamp{
						T:         t,
						Precision: int(fields[column].Precision),
					}
					return nil
				}
				rv := reflect.ValueOf(src[column][row])
				switch rv.Kind() {
				case reflect.Float32, reflect.Float64:
					t := common.TimestampConvertToTime(int64(rv.Float()), int(fields[column].Precision))
					src[column][row] = types.TaosTimestamp{
						T:         t,
						Precision: int(fields[column].Precision),
					}
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					t := common.TimestampConvertToTime(rv.Int(), int(fields[column].Precision))
					src[column][row] = types.TaosTimestamp{
						T:         t,
						Precision: int(fields[column].Precision),
					}
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					t := common.TimestampConvertToTime(int64(rv.Uint()), int(fields[column].Precision))
					src[column][row] = types.TaosTimestamp{
						T:         t,
						Precision: int(fields[column].Precision),
					}
				case reflect.String:
					t, err := time.Parse(time.RFC3339Nano, rv.String())
					if err != nil {
						return err
					}
					src[column][row] = types.TaosTimestamp{
						T:         t,
						Precision: int(fields[column].Precision),
					}
				default:
					return fmt.Errorf("stmtConvert:%v can not convert to timestamp", src[column][row])
				}
			}
		case common.TSDB_DATA_TYPE_NCHAR:
			for row := 0; row < len(src[column]); row++ {
				if src[column][row] == nil {
					continue
				}
				switch src[column][row].(type) {
				case string:
					src[column][row] = types.TaosNchar(src[column][row].(string))
				case []byte:
					src[column][row] = types.TaosNchar(src[column][row].([]byte))
				default:
					return fmt.Errorf("stmtConvert:%v can not convert to nchar", src[column][row])
				}
				if fieldTypes != nil && len(src[column][row].(types.TaosNchar)) > fieldTypes[column].MaxLen {
					fieldTypes[column].MaxLen = len(src[column][row].(types.TaosNchar))
				}
			}
		case common.TSDB_DATA_TYPE_UTINYINT:
			for row := 0; row < len(src[column]); row++ {
				if src[column][row] == nil {
					continue
				}
				rv := reflect.ValueOf(src[column][row])
				switch rv.Kind() {
				case reflect.Bool:
					if rv.Bool() == true {
						src[column][row] = types.TaosUTinyint(1)
					} else {
						src[column][row] = types.TaosUTinyint(0)
					}
				case reflect.Float32, reflect.Float64:
					src[column][row] = types.TaosUTinyint(rv.Float())
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					src[column][row] = types.TaosUTinyint(rv.Int())
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					src[column][row] = types.TaosUTinyint(rv.Uint())
				case reflect.String:
					vv, err := strconv.ParseUint(rv.String(), 0, 8)
					if err != nil {
						return err
					}
					src[column][row] = types.TaosUTinyint(vv)
				default:
					return fmt.Errorf("stmtConvert:%v can not convert to tinyint unsigned", src[column][row])
				}
			}
		case common.TSDB_DATA_TYPE_USMALLINT:
			for row := 0; row < len(src[column]); row++ {
				if src[column][row] == nil {
					continue
				}
				rv := reflect.ValueOf(src[column][row])
				switch rv.Kind() {
				case reflect.Bool:
					if rv.Bool() == true {
						src[column][row] = types.TaosUSmallint(1)
					} else {
						src[column][row] = types.TaosUSmallint(0)
					}
				case reflect.Float32, reflect.Float64:
					src[column][row] = types.TaosUSmallint(rv.Float())
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					src[column][row] = types.TaosUSmallint(rv.Int())
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					src[column][row] = types.TaosUSmallint(rv.Uint())
				case reflect.String:
					vv, err := strconv.ParseUint(rv.String(), 0, 16)
					if err != nil {
						return err
					}
					src[column][row] = types.TaosUSmallint(vv)
				default:
					return fmt.Errorf("stmtConvert:%v can not convert to smallint unsigned", src[column][row])
				}
			}
		case common.TSDB_DATA_TYPE_UINT:
			for row := 0; row < len(src[column]); row++ {
				if src[column][row] == nil {
					continue
				}
				rv := reflect.ValueOf(src[column][row])
				switch rv.Kind() {
				case reflect.Bool:
					if rv.Bool() == true {
						src[column][row] = types.TaosUInt(1)
					} else {
						src[column][row] = types.TaosUInt(0)
					}
				case reflect.Float32, reflect.Float64:
					src[column][row] = types.TaosUInt(rv.Float())
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					src[column][row] = types.TaosUInt(rv.Int())
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					src[column][row] = types.TaosUInt(rv.Uint())
				case reflect.String:
					vv, err := strconv.ParseUint(rv.String(), 0, 32)
					if err != nil {
						return err
					}
					src[column][row] = types.TaosUInt(vv)
				default:
					return fmt.Errorf("stmtConvert:%v can not convert to int unsigned", src[column][row])
				}
			}
		case common.TSDB_DATA_TYPE_UBIGINT:
			for row := 0; row < len(src[column]); row++ {
				if src[column][row] == nil {
					continue
				}
				rv := reflect.ValueOf(src[column][row])
				switch rv.Kind() {
				case reflect.Bool:
					if rv.Bool() == true {
						src[column][row] = types.TaosUBigint(1)
					} else {
						src[column][row] = types.TaosUBigint(0)
					}
				case reflect.Float32, reflect.Float64:
					src[column][row] = types.TaosUBigint(rv.Float())
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					src[column][row] = types.TaosUBigint(rv.Int())
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					src[column][row] = types.TaosUBigint(rv.Uint())
				case reflect.String:
					vv, err := strconv.ParseUint(rv.String(), 0, 64)
					if err != nil {
						return err
					}
					src[column][row] = types.TaosUBigint(vv)
				default:
					return fmt.Errorf("stmtConvert:%v can not convert to bigint unsigned", src[column][row])
				}
			}
		}
	}
	return nil
}

func blockConvert(block unsafe.Pointer, blockSize int, fields []*wrapper.StmtField) [][]driver.Value {
	colCount := len(fields)
	r := make([][]driver.Value, colCount)
	payloadOffset := uintptr(4 * colCount)
	nullBitMapOffset := uintptr(wrapper.BitmapLen(blockSize))
	pHeader := uintptr(block) + payloadOffset + 12 + uintptr(6*colCount) // length i32, group u64
	pStart := pHeader
	for column := 0; column < colCount; column++ {
		r[column] = make([]driver.Value, blockSize)
		colLength := *((*int32)(unsafe.Pointer(uintptr(block) + 12 + uintptr(6*colCount) + uintptr(column)*4)))
		if wrapper.IsVarDataType(uint8(fields[column].FieldType)) {
			convertF := rawConvertVarDataMap[fields[column].FieldType]
			pStart = pHeader + uintptr(4*blockSize)
			for row := 0; row < blockSize; row++ {
				r[column][row] = convertF(pHeader, pStart, row)
			}
		} else {
			convertF := rawConvertFuncMap[fields[column].FieldType]
			pStart = pHeader + nullBitMapOffset
			for row := 0; row < blockSize; row++ {
				if ItemIsNull(pHeader, row) {
					r[column][row] = nil
				} else {
					r[column][row] = convertF(pStart, row, int(fields[column].Precision))
				}
			}
		}
		pHeader = pStart + uintptr(colLength)
	}
	return r
}

type rawConvertFunc func(pStart uintptr, row int, arg ...interface{}) driver.Value

type rawConvertVarDataFunc func(pHeader, pStart uintptr, row int) driver.Value

var rawConvertFuncMap = map[int8]rawConvertFunc{
	int8(common.TSDB_DATA_TYPE_BOOL):      rawConvertBool,
	int8(common.TSDB_DATA_TYPE_TINYINT):   rawConvertTinyint,
	int8(common.TSDB_DATA_TYPE_SMALLINT):  rawConvertSmallint,
	int8(common.TSDB_DATA_TYPE_INT):       rawConvertInt,
	int8(common.TSDB_DATA_TYPE_BIGINT):    rawConvertBigint,
	int8(common.TSDB_DATA_TYPE_UTINYINT):  rawConvertUTinyint,
	int8(common.TSDB_DATA_TYPE_USMALLINT): rawConvertUSmallint,
	int8(common.TSDB_DATA_TYPE_UINT):      rawConvertUInt,
	int8(common.TSDB_DATA_TYPE_UBIGINT):   rawConvertUBigint,
	int8(common.TSDB_DATA_TYPE_FLOAT):     rawConvertFloat,
	int8(common.TSDB_DATA_TYPE_DOUBLE):    rawConvertDouble,
	int8(common.TSDB_DATA_TYPE_TIMESTAMP): rawConvertTime,
}

var rawConvertVarDataMap = map[int8]rawConvertVarDataFunc{
	int8(common.TSDB_DATA_TYPE_BINARY): rawConvertBinary,
	int8(common.TSDB_DATA_TYPE_NCHAR):  rawConvertNchar,
}

func ItemIsNull(pHeader uintptr, row int) bool {
	offset := wrapper.CharOffset(row)
	c := *((*byte)(unsafe.Pointer(pHeader + uintptr(offset))))
	if wrapper.BMIsNull(c, row) {
		return true
	}
	return false
}

func rawConvertBool(pStart uintptr, row int, _ ...interface{}) driver.Value {
	if (*((*byte)(unsafe.Pointer(pStart + uintptr(row)*1)))) != 0 {
		return types.TaosBool(true)
	} else {
		return types.TaosBool(false)
	}
}

func rawConvertTinyint(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return types.TaosTinyint(*((*int8)(unsafe.Pointer(pStart + uintptr(row)*1))))
}

func rawConvertSmallint(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return types.TaosSmallint(*((*int16)(unsafe.Pointer(pStart + uintptr(row)*2))))
}

func rawConvertInt(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return types.TaosInt(*((*int32)(unsafe.Pointer(pStart + uintptr(row)*4))))
}

func rawConvertBigint(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return types.TaosBigint(*((*int64)(unsafe.Pointer(pStart + uintptr(row)*8))))
}

func rawConvertUTinyint(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return types.TaosUTinyint(*((*uint8)(unsafe.Pointer(pStart + uintptr(row)*1))))
}

func rawConvertUSmallint(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return types.TaosUSmallint(*((*uint16)(unsafe.Pointer(pStart + uintptr(row)*2))))
}

func rawConvertUInt(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return types.TaosUInt(*((*uint32)(unsafe.Pointer(pStart + uintptr(row)*4))))
}

func rawConvertUBigint(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return types.TaosUBigint(*((*uint64)(unsafe.Pointer(pStart + uintptr(row)*8))))
}

func rawConvertFloat(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return types.TaosFloat(*((*float32)(unsafe.Pointer(pStart + uintptr(row)*4))))
}

func rawConvertDouble(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return types.TaosDouble(*((*float64)(unsafe.Pointer(pStart + uintptr(row)*8))))
}

func rawConvertTime(pStart uintptr, row int, arg ...interface{}) driver.Value {
	return types.TaosTimestamp{
		T:         common.TimestampConvertToTime(*((*int64)(unsafe.Pointer(pStart + uintptr(row)*8))), arg[0].(int)),
		Precision: arg[0].(int),
	}
}

func rawConvertBinary(pHeader, pStart uintptr, row int) driver.Value {
	offset := *((*int32)(unsafe.Pointer(pHeader + uintptr(row*4))))
	if offset == -1 {
		return nil
	}
	currentRow := unsafe.Pointer(pStart + uintptr(offset))
	clen := *((*int16)(currentRow))
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)

	binaryVal := make([]byte, clen)

	for index := int16(0); index < clen; index++ {
		binaryVal[index] = *((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(index))))
	}
	return types.TaosBinary(binaryVal)
}

func rawConvertNchar(pHeader, pStart uintptr, row int) driver.Value {
	offset := *((*int32)(unsafe.Pointer(pHeader + uintptr(row*4))))
	if offset == -1 {
		return nil
	}
	currentRow := unsafe.Pointer(pStart + uintptr(offset))
	clen := *((*int16)(currentRow)) / 4
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)

	binaryVal := make([]rune, clen)

	for index := int16(0); index < clen; index++ {
		binaryVal[index] = *((*rune)(unsafe.Pointer(uintptr(currentRow) + uintptr(index*4))))
	}
	return types.TaosNchar(binaryVal)
}

package ws

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/parser"
	stmtCommon "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/types"
)

var jsonI = jsoniter.ConfigCompatibleWithStandardLibrary

func stmtParseColumn(columns json.RawMessage, fields []*stmtCommon.StmtField, fieldTypes []*types.ColumnType) ([][]driver.Value, error) {
	var err error
	iter := jsonI.BorrowIterator(columns)
	defer jsonI.ReturnIterator(iter)
	data := make([][]driver.Value, len(fields))
	rowNums := 0
	rowIndex := 0
	colIndex := 0
	iter.ReadArrayCB(func(iterCol *jsoniter.Iterator) bool {
		colType := fields[colIndex].FieldType
		if colIndex == 1 {
			rowNums = rowIndex
		} else if colIndex > 1 {
			if rowNums != rowIndex {
				iterCol.ReportError("wrong row length", fmt.Sprintf("want %d rows got %d", rowNums, rowIndex))
				return false
			}
		}
		rowIndex = 0
		iterCol.ReadArrayCB(func(iterRow *jsoniter.Iterator) bool {
			if iterRow.ReadNil() {
				data[colIndex] = append(data[colIndex], nil)
			} else {
				switch colType {
				case common.TSDB_DATA_TYPE_BOOL:
					data[colIndex] = append(data[colIndex], types.TaosBool(iterRow.ReadBool()))
				case common.TSDB_DATA_TYPE_TINYINT:
					data[colIndex] = append(data[colIndex], types.TaosTinyint(iterRow.ReadInt8()))
				case common.TSDB_DATA_TYPE_SMALLINT:
					data[colIndex] = append(data[colIndex], types.TaosSmallint(iterRow.ReadInt16()))
				case common.TSDB_DATA_TYPE_INT:
					data[colIndex] = append(data[colIndex], types.TaosInt(iterRow.ReadInt32()))
				case common.TSDB_DATA_TYPE_BIGINT:
					data[colIndex] = append(data[colIndex], types.TaosBigint(iterRow.ReadInt64()))
				case common.TSDB_DATA_TYPE_FLOAT:
					data[colIndex] = append(data[colIndex], types.TaosFloat(iterRow.ReadFloat32()))
				case common.TSDB_DATA_TYPE_DOUBLE:
					data[colIndex] = append(data[colIndex], types.TaosDouble(iterRow.ReadFloat64()))
				case common.TSDB_DATA_TYPE_BINARY:
					s := iterRow.ReadString()
					data[colIndex] = append(data[colIndex], types.TaosBinary(s))
					if fieldTypes != nil && len(s) > fieldTypes[colIndex].MaxLen {
						fieldTypes[colIndex].MaxLen = len(s)
					}
				case common.TSDB_DATA_TYPE_TIMESTAMP:
					valueType := iterRow.WhatIsNext()
					ts := types.TaosTimestamp{
						Precision: int(fields[colIndex].Precision),
					}
					switch valueType {
					case jsoniter.NumberValue:
						ts.T = common.TimestampConvertToTime(iterRow.ReadInt64(), ts.Precision)
					case jsoniter.StringValue:
						ts.T, err = time.Parse(time.RFC3339Nano, iterRow.ReadString())
						if err != nil {
							iterRow.ReportError("parse time", err.Error())
						}
					}
					data[colIndex] = append(data[colIndex], ts)
				case common.TSDB_DATA_TYPE_NCHAR:
					s := iterRow.ReadString()
					if fieldTypes != nil && len(s) > fieldTypes[colIndex].MaxLen {
						fieldTypes[colIndex].MaxLen = len(s)
					}
					data[colIndex] = append(data[colIndex], types.TaosNchar(s))
				case common.TSDB_DATA_TYPE_UTINYINT:
					data[colIndex] = append(data[colIndex], types.TaosUTinyint(iterRow.ReadUint8()))
				case common.TSDB_DATA_TYPE_USMALLINT:
					data[colIndex] = append(data[colIndex], types.TaosUSmallint(iterRow.ReadUint16()))
				case common.TSDB_DATA_TYPE_UINT:
					data[colIndex] = append(data[colIndex], types.TaosUInt(iterRow.ReadUint32()))
				case common.TSDB_DATA_TYPE_UBIGINT:
					data[colIndex] = append(data[colIndex], types.TaosUBigint(iterRow.ReadUint64()))
				default:
					iterRow.ReportError("unknown column types", strconv.Itoa(int(colType)))
				}
			}
			rowIndex += 1
			return iterRow.Error == nil
		})
		colIndex += 1
		return iterCol.Error == nil
	})
	if iter.Error != nil && iter.Error != io.EOF {
		return nil, iter.Error
	}
	return data, nil
}

func stmtParseTag(tags json.RawMessage, fields []*stmtCommon.StmtField) ([]driver.Value, error) {
	var err error
	iter := jsonI.BorrowIterator(tags)
	defer jsonI.ReturnIterator(iter)
	data := make([]driver.Value, len(fields))
	colIndex := 0
	iter.ReadArrayCB(func(iterCol *jsoniter.Iterator) bool {
		colType := fields[colIndex].FieldType
		if iterCol.ReadNil() {
			data[colIndex] = nil
		} else {
			switch colType {
			case common.TSDB_DATA_TYPE_BOOL:
				data[colIndex] = types.TaosBool(iterCol.ReadBool())
			case common.TSDB_DATA_TYPE_TINYINT:
				data[colIndex] = types.TaosTinyint(iterCol.ReadInt8())
			case common.TSDB_DATA_TYPE_SMALLINT:
				data[colIndex] = types.TaosSmallint(iterCol.ReadInt16())
			case common.TSDB_DATA_TYPE_INT:
				data[colIndex] = types.TaosInt(iterCol.ReadInt32())
			case common.TSDB_DATA_TYPE_BIGINT:
				data[colIndex] = types.TaosBigint(iterCol.ReadInt64())
			case common.TSDB_DATA_TYPE_FLOAT:
				data[colIndex] = types.TaosFloat(iterCol.ReadFloat32())
			case common.TSDB_DATA_TYPE_DOUBLE:
				data[colIndex] = types.TaosDouble(iterCol.ReadFloat64())
			case common.TSDB_DATA_TYPE_BINARY:
				data[colIndex] = types.TaosBinary(iterCol.ReadString())
			case common.TSDB_DATA_TYPE_TIMESTAMP:
				valueType := iterCol.WhatIsNext()
				ts := types.TaosTimestamp{
					Precision: int(fields[colIndex].Precision),
				}
				switch valueType {
				case jsoniter.NumberValue:
					ts.T = common.TimestampConvertToTime(iterCol.ReadInt64(), ts.Precision)
				case jsoniter.StringValue:
					ts.T, err = time.Parse(time.RFC3339Nano, iterCol.ReadString())
					if err != nil {
						iterCol.ReportError("parse time", err.Error())
					}
				}
				data[colIndex] = ts
			case common.TSDB_DATA_TYPE_NCHAR:
				data[colIndex] = types.TaosNchar(iterCol.ReadString())
			case common.TSDB_DATA_TYPE_UTINYINT:
				data[colIndex] = types.TaosUTinyint(iterCol.ReadUint8())
			case common.TSDB_DATA_TYPE_USMALLINT:
				data[colIndex] = types.TaosUSmallint(iterCol.ReadUint16())
			case common.TSDB_DATA_TYPE_UINT:
				data[colIndex] = types.TaosUInt(iterCol.ReadUint32())
			case common.TSDB_DATA_TYPE_UBIGINT:
				data[colIndex] = types.TaosUBigint(iterCol.ReadUint64())
			case common.TSDB_DATA_TYPE_JSON:
				x := iterCol.ReadString()
				data[colIndex] = types.TaosJson(x)
			default:
				iterCol.ReportError("unknown column types", strconv.Itoa(int(colType)))
			}
		}
		colIndex += 1
		return iterCol.Error == nil
	})
	if iter.Error != nil && iter.Error != io.EOF {
		return nil, iter.Error
	}
	return data, nil
}

func blockConvert(block unsafe.Pointer, blockSize int, fields []*stmtCommon.StmtField, fieldTypes []*types.ColumnType) [][]driver.Value {
	colCount := len(fields)
	r := make([][]driver.Value, colCount)
	nullBitMapOffset := uintptr(parser.BitmapLen(blockSize))
	lengthOffset := parser.RawBlockGetColumnLengthOffset(colCount)
	pHeader := uintptr(block) + parser.RawBlockGetColDataOffset(colCount)
	pStart := pHeader
	length := 0
	for column := 0; column < colCount; column++ {
		r[column] = make([]driver.Value, blockSize)
		colLength := *((*int32)(unsafe.Pointer(uintptr(block) + lengthOffset + uintptr(column)*parser.Int32Size)))
		if parser.IsVarDataType(uint8(fields[column].FieldType)) {
			convertF := rawConvertVarDataMap[fields[column].FieldType]
			pStart = pHeader + uintptr(4*blockSize)
			for row := 0; row < blockSize; row++ {
				r[column][row], length = convertF(pHeader, pStart, row)
				if fieldTypes != nil {
					if length > fieldTypes[column].MaxLen {
						fieldTypes[column].MaxLen = length
					}
				}
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

type rawConvertVarDataFunc func(pHeader, pStart uintptr, row int) (driver.Value, int)

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
	int8(common.TSDB_DATA_TYPE_JSON):   rawConvertJson,
}

func ItemIsNull(pHeader uintptr, row int) bool {
	offset := parser.CharOffset(row)
	c := *((*byte)(unsafe.Pointer(pHeader + uintptr(offset))))
	return parser.BMIsNull(c, row)
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

func rawConvertBinary(pHeader, pStart uintptr, row int) (driver.Value, int) {
	offset := *((*int32)(unsafe.Pointer(pHeader + uintptr(row*4))))
	if offset == -1 {
		return nil, 0
	}
	currentRow := unsafe.Pointer(pStart + uintptr(offset))
	clen := *((*int16)(currentRow))
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)

	binaryVal := make([]byte, clen)

	for index := int16(0); index < clen; index++ {
		binaryVal[index] = *((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(index))))
	}
	return types.TaosBinary(binaryVal), int(clen)
}

func rawConvertNchar(pHeader, pStart uintptr, row int) (driver.Value, int) {
	offset := *((*int32)(unsafe.Pointer(pHeader + uintptr(row*4))))
	if offset == -1 {
		return nil, 0
	}
	currentRow := unsafe.Pointer(pStart + uintptr(offset))
	clen := *((*int16)(currentRow)) / 4
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)

	binaryVal := make([]rune, clen)

	for index := int16(0); index < clen; index++ {
		binaryVal[index] = *((*rune)(unsafe.Pointer(uintptr(currentRow) + uintptr(index*4))))
	}
	return types.TaosNchar(binaryVal), int(clen) * 4
}

func rawConvertJson(pHeader, pStart uintptr, row int) (driver.Value, int) {
	offset := *((*int32)(unsafe.Pointer(pHeader + uintptr(row*4))))
	if offset == -1 {
		return nil, 0
	}
	currentRow := unsafe.Pointer(pStart + uintptr(offset))
	clen := *((*int16)(currentRow))
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)

	binaryVal := make([]byte, clen)

	for index := int16(0); index < clen; index++ {
		binaryVal[index] = *((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(index))))
	}
	return types.TaosJson(binaryVal), int(clen)
}

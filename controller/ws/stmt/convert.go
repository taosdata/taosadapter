package stmt

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
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

func stmtConvert(src [][]driver.Value, fields []*stmtCommon.StmtField, fieldTypes []*types.ColumnType) error {
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
					src[column][row] = types.TaosBool(vv)
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
					if rv.Bool() {
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
					if rv.Bool() {
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
					if rv.Bool() {
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
					if rv.Bool() {
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
					if rv.Bool() {
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
					if rv.Bool() {
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
						T:         t.UTC(),
						Precision: int(fields[column].Precision),
					}
					return nil
				}
				rv := reflect.ValueOf(src[column][row])
				switch rv.Kind() {
				case reflect.Float32, reflect.Float64:
					t := common.TimestampConvertToTime(int64(rv.Float()), int(fields[column].Precision))
					src[column][row] = types.TaosTimestamp{
						T:         t.UTC(),
						Precision: int(fields[column].Precision),
					}
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					t := common.TimestampConvertToTime(rv.Int(), int(fields[column].Precision))
					src[column][row] = types.TaosTimestamp{
						T:         t.UTC(),
						Precision: int(fields[column].Precision),
					}
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					t := common.TimestampConvertToTime(int64(rv.Uint()), int(fields[column].Precision))
					src[column][row] = types.TaosTimestamp{
						T:         t.UTC(),
						Precision: int(fields[column].Precision),
					}
				case reflect.String:
					t, err := time.Parse(time.RFC3339Nano, rv.String())
					if err != nil {
						return err
					}
					src[column][row] = types.TaosTimestamp{
						T:         t.UTC(),
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
					if rv.Bool() {
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
					if rv.Bool() {
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
					if rv.Bool() {
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
					if rv.Bool() {
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
		case common.TSDB_DATA_TYPE_JSON:
			for row := 0; row < len(src[column]); row++ {
				if src[column][row] == nil {
					continue
				}
				switch src[column][row].(type) {
				case string:
					src[column][row] = types.TaosJson(src[column][row].(string))
				case []byte:
					src[column][row] = types.TaosJson(src[column][row].([]byte))
				default:
					return fmt.Errorf("stmtConvert:%v can not convert to json", src[column][row])
				}
				if fieldTypes != nil && len(src[column][row].(types.TaosJson)) > fieldTypes[column].MaxLen {
					fieldTypes[column].MaxLen = len(src[column][row].(types.TaosJson))
				}
			}
		}
	}
	return nil
}

func StmtParseColumn(columns json.RawMessage, fields []*stmtCommon.StmtField, fieldTypes []*types.ColumnType) ([][]driver.Value, error) {
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

func StmtParseTag(tags json.RawMessage, fields []*stmtCommon.StmtField) ([]driver.Value, error) {
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

func BlockConvert(block unsafe.Pointer, blockSize int, fields []*stmtCommon.StmtField, fieldTypes []*types.ColumnType) [][]driver.Value {
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
	clen := *((*uint16)(currentRow))
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)

	binaryVal := make([]byte, clen)

	for index := uint16(0); index < clen; index++ {
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
	clen := *((*uint16)(currentRow)) / 4
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)

	binaryVal := make([]rune, clen)

	for index := uint16(0); index < clen; index++ {
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
	clen := *((*uint16)(currentRow))
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)

	binaryVal := make([]byte, clen)

	for index := uint16(0); index < clen; index++ {
		binaryVal[index] = *((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(index))))
	}
	return types.TaosJson(binaryVal), int(clen)
}

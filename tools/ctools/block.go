package ctools

import (
	"math"
	"strconv"
	"unsafe"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/parser"
	"github.com/taosdata/taosadapter/v3/tools"
	"github.com/taosdata/taosadapter/v3/tools/jsonbuilder"
)

type FormatTimeFunc func(builder *jsonbuilder.Stream, ts int64, precision int)

func IsVarDataType(colType uint8) bool {
	return colType == common.TSDB_DATA_TYPE_BINARY || colType == common.TSDB_DATA_TYPE_NCHAR || colType == common.TSDB_DATA_TYPE_JSON || colType == common.TSDB_DATA_TYPE_VARBINARY || colType == common.TSDB_DATA_TYPE_GEOMETRY
}

func BitmapLen(n int) int {
	return ((n) + ((1 << 3) - 1)) >> 3
}

func BitPos(n int) int {
	return n & ((1 << 3) - 1)
}

func CharOffset(n int) int {
	return n >> 3
}

func BMIsNull(c byte, n int) bool {
	return c&(1<<(7-BitPos(n))) == (1 << (7 - BitPos(n)))
}

func ItemIsNull(pHeader unsafe.Pointer, row int) bool {
	offset := CharOffset(row)
	c := *((*byte)(tools.AddPointer(pHeader, uintptr(offset))))
	return BMIsNull(c, row)
}

func WriteRawJsonBool(builder *jsonbuilder.Stream, pStart unsafe.Pointer, row int) {
	if (*((*byte)(tools.AddPointer(pStart, uintptr(row)*1)))) != 0 {
		builder.WriteTrue()
	} else {
		builder.WriteFalse()
	}
}

func WriteRawJsonTinyint(builder *jsonbuilder.Stream, pStart unsafe.Pointer, row int) {
	builder.WriteInt8(*((*int8)(tools.AddPointer(pStart, uintptr(row)*parser.Int8Size))))
}

func WriteRawJsonSmallint(builder *jsonbuilder.Stream, pStart unsafe.Pointer, row int) {
	builder.WriteInt16(*((*int16)(tools.AddPointer(pStart, uintptr(row)*parser.Int16Size))))
}

func WriteRawJsonInt(builder *jsonbuilder.Stream, pStart unsafe.Pointer, row int) {
	builder.WriteInt32(*((*int32)(tools.AddPointer(pStart, uintptr(row)*parser.Int32Size))))
}

func WriteRawJsonBigint(builder *jsonbuilder.Stream, pStart unsafe.Pointer, row int) {
	builder.WriteInt64(*((*int64)(tools.AddPointer(pStart, uintptr(row)*parser.Int64Size))))
}

func WriteRawJsonUTinyint(builder *jsonbuilder.Stream, pStart unsafe.Pointer, row int) {
	builder.WriteUint8(*((*uint8)(tools.AddPointer(pStart, uintptr(row)*parser.UInt8Size))))
}

func WriteRawJsonUSmallint(builder *jsonbuilder.Stream, pStart unsafe.Pointer, row int) {
	builder.WriteUint16(*((*uint16)(tools.AddPointer(pStart, uintptr(row)*parser.UInt16Size))))
}

func WriteRawJsonUInt(builder *jsonbuilder.Stream, pStart unsafe.Pointer, row int) {
	builder.WriteUint32(*((*uint32)(tools.AddPointer(pStart, uintptr(row)*parser.UInt32Size))))
}

func WriteRawJsonUBigint(builder *jsonbuilder.Stream, pStart unsafe.Pointer, row int) {
	builder.WriteUint64(*((*uint64)(tools.AddPointer(pStart, uintptr(row)*parser.UInt64Size))))
}

func WriteRawJsonFloat(builder *jsonbuilder.Stream, pStart unsafe.Pointer, row int) {
	builder.WriteFloat32(math.Float32frombits(*((*uint32)(tools.AddPointer(pStart, uintptr(row)*parser.Float32Size)))))
}

func WriteRawJsonDouble(builder *jsonbuilder.Stream, pStart unsafe.Pointer, row int) {
	builder.WriteFloat64(math.Float64frombits(*((*uint64)(tools.AddPointer(pStart, uintptr(row)*parser.Float64Size)))))
}

func WriteRawJsonTime(builder *jsonbuilder.Stream, pStart unsafe.Pointer, row int, precision int, timeFormat FormatTimeFunc) {
	value := *((*int64)(tools.AddPointer(pStart, uintptr(row)*parser.Int64Size)))
	timeFormat(builder, value, precision)
}

func WriteRawJsonBinary(builder *jsonbuilder.Stream, pHeader, pStart unsafe.Pointer, row int) {
	offset := *((*int32)(tools.AddPointer(pHeader, uintptr(row*4))))
	if offset == -1 {
		builder.WriteNil()
		return
	}
	currentRow := tools.AddPointer(pStart, uintptr(offset))
	clen := *((*int16)(currentRow))
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)

	builder.WriteByte('"')
	for index := int16(0); index < clen; index++ {
		builder.WriteStringByte(*((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(index)))))
	}
	builder.WriteByte('"')
}

func WriteRawJsonVarBinary(builder *jsonbuilder.Stream, pHeader, pStart unsafe.Pointer, row int) {
	offset := *((*int32)(tools.AddPointer(pHeader, uintptr(row*4))))
	if offset == -1 {
		builder.WriteNil()
		return
	}
	currentRow := tools.AddPointer(pStart, uintptr(offset))
	clen := *((*int16)(currentRow))
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)

	builder.WriteByte('"')
	var b byte
	for index := int16(0); index < clen; index++ {
		b = *((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(index))))
		s := strconv.FormatInt(int64(b), 16)
		if len(s) == 1 {
			builder.WriteByte('0')
		}
		builder.WriteRaw(s)
	}
	builder.WriteByte('"')
}

func WriteRawJsonGeometry(builder *jsonbuilder.Stream, pHeader, pStart unsafe.Pointer, row int) {
	offset := *((*int32)(tools.AddPointer(pHeader, uintptr(row*4))))
	if offset == -1 {
		builder.WriteNil()
		return
	}
	currentRow := tools.AddPointer(pStart, uintptr(offset))
	clen := *((*int16)(currentRow))
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)

	builder.WriteByte('"')
	var b byte
	for index := int16(0); index < clen; index++ {
		b = *((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(index))))
		s := strconv.FormatInt(int64(b), 16)
		if len(s) == 1 {
			builder.WriteByte('0')
		}
		builder.WriteRaw(s)
	}
	builder.WriteByte('"')
}

func WriteRawJsonNchar(builder *jsonbuilder.Stream, pHeader, pStart unsafe.Pointer, row int) {
	offset := *((*int32)(tools.AddPointer(pHeader, uintptr(row*4))))
	if offset == -1 {
		builder.WriteNil()
		return
	}
	currentRow := tools.AddPointer(pStart, uintptr(offset))
	clen := *((*int16)(currentRow)) / 4
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)
	builder.WriteByte('"')
	for index := int16(0); index < clen; index++ {
		builder.WriteRuneString(*((*rune)(unsafe.Pointer(uintptr(currentRow) + uintptr(index*4)))))
	}
	builder.WriteByte('"')
}

func WriteRawJsonJson(builder *jsonbuilder.Stream, pHeader, pStart unsafe.Pointer, row int) {
	offset := *((*int32)(tools.AddPointer(pHeader, uintptr(row*4))))
	if offset == -1 {
		builder.WriteNil()
		return
	}
	currentRow := tools.AddPointer(pStart, uintptr(offset))
	clen := *((*int16)(currentRow))
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)

	for index := int16(0); index < clen; index++ {
		builder.WriteByte(*((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(index)))))
	}
}

func JsonWriteRawBlock(builder *jsonbuilder.Stream, colType uint8, pHeader, pStart unsafe.Pointer, row int, precision int, timeFormat FormatTimeFunc) {
	if IsVarDataType(colType) {
		switch colType {
		case uint8(common.TSDB_DATA_TYPE_BINARY):
			WriteRawJsonBinary(builder, pHeader, pStart, row)
		case uint8(common.TSDB_DATA_TYPE_NCHAR):
			WriteRawJsonNchar(builder, pHeader, pStart, row)
		case uint8(common.TSDB_DATA_TYPE_JSON):
			WriteRawJsonJson(builder, pHeader, pStart, row)
		case uint8(common.TSDB_DATA_TYPE_VARBINARY):
			WriteRawJsonVarBinary(builder, pHeader, pStart, row)
		case uint8(common.TSDB_DATA_TYPE_GEOMETRY):
			WriteRawJsonGeometry(builder, pHeader, pStart, row)
		}
	} else {
		if ItemIsNull(pHeader, row) {
			builder.WriteNil()
		} else {
			switch colType {
			case uint8(common.TSDB_DATA_TYPE_BOOL):
				WriteRawJsonBool(builder, pStart, row)
			case uint8(common.TSDB_DATA_TYPE_TINYINT):
				WriteRawJsonTinyint(builder, pStart, row)
			case uint8(common.TSDB_DATA_TYPE_SMALLINT):
				WriteRawJsonSmallint(builder, pStart, row)
			case uint8(common.TSDB_DATA_TYPE_INT):
				WriteRawJsonInt(builder, pStart, row)
			case uint8(common.TSDB_DATA_TYPE_BIGINT):
				WriteRawJsonBigint(builder, pStart, row)
			case uint8(common.TSDB_DATA_TYPE_UTINYINT):
				WriteRawJsonUTinyint(builder, pStart, row)
			case uint8(common.TSDB_DATA_TYPE_USMALLINT):
				WriteRawJsonUSmallint(builder, pStart, row)
			case uint8(common.TSDB_DATA_TYPE_UINT):
				WriteRawJsonUInt(builder, pStart, row)
			case uint8(common.TSDB_DATA_TYPE_UBIGINT):
				WriteRawJsonUBigint(builder, pStart, row)
			case uint8(common.TSDB_DATA_TYPE_FLOAT):
				WriteRawJsonFloat(builder, pStart, row)
			case uint8(common.TSDB_DATA_TYPE_DOUBLE):
				WriteRawJsonDouble(builder, pStart, row)
			case uint8(common.TSDB_DATA_TYPE_TIMESTAMP):
				WriteRawJsonTime(builder, pStart, row, precision, timeFormat)
			}
		}
	}
}

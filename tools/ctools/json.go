package ctools

/*
#include <taos.h>
*/
import "C"
import (
	"math"
	"unsafe"

	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/tools/jsonbuilder"
)

func WriteBool(buffer *jsonbuilder.Stream, p unsafe.Pointer) {
	value := *((*byte)(p))
	if value == wrapper.CBoolNull {
		buffer.WriteNil()
	} else if value != 0 {
		buffer.WriteTrue()
	} else {
		buffer.WriteFalse()
	}
}

func WriteTinyint(buffer *jsonbuilder.Stream, p unsafe.Pointer) {
	value := *((*int8)(p))
	if value == wrapper.CTinyintNull {
		buffer.WriteNil()
	} else {
		buffer.WriteInt8(value)
	}
}

func WriteSmallint(buffer *jsonbuilder.Stream, p unsafe.Pointer) {
	value := *((*int16)(p))
	if value == wrapper.CSmallintNull {
		buffer.WriteNil()
	} else {
		buffer.WriteInt16(value)
	}
}

func WriteInt(buffer *jsonbuilder.Stream, p unsafe.Pointer) {
	value := *((*int32)(p))
	if value == wrapper.CIntNull {
		buffer.WriteNil()
	} else {
		buffer.WriteInt32(value)
	}
}

func WriteBigint(buffer *jsonbuilder.Stream, p unsafe.Pointer) {
	value := *((*int64)(p))
	if value == wrapper.CBigintNull {
		buffer.WriteNil()
	} else {
		buffer.WriteInt64(value)
	}
}

func WriteUTinyint(buffer *jsonbuilder.Stream, p unsafe.Pointer) {
	value := *((*uint8)(p))
	if value == wrapper.CTinyintUnsignedNull {
		buffer.WriteNil()
	} else {
		buffer.WriteUint8(value)
	}
}

func WriteUSmallint(buffer *jsonbuilder.Stream, p unsafe.Pointer) {
	value := *((*uint16)(p))
	if value == wrapper.CSmallintUnsignedNull {
		buffer.WriteNil()
	} else {
		buffer.WriteUint16(value)
	}
}

func WriteUInt(buffer *jsonbuilder.Stream, p unsafe.Pointer) {
	value := *((*uint32)(p))
	if value == wrapper.CIntUnsignedNull {
		buffer.WriteNil()
	} else {
		buffer.WriteUint32(value)
	}
}

func WriteUBigint(buffer *jsonbuilder.Stream, p unsafe.Pointer) {
	value := *((*uint64)(p))
	if value == wrapper.CBigintUnsignedNull {
		buffer.WriteNil()
	} else {
		buffer.WriteUint64(value)
	}
}

func WriteFloat(buffer *jsonbuilder.Stream, p unsafe.Pointer) {
	value := *((*float32)(p))
	if math.IsNaN(float64(value)) {
		buffer.WriteNil()
	} else {
		buffer.WriteFloat32(value)
	}
}

func WriteDouble(buffer *jsonbuilder.Stream, p unsafe.Pointer) {
	value := *((*float64)(p))
	if math.IsNaN(value) {
		buffer.WriteNil()
	} else {
		buffer.WriteFloat64(value)
	}
}

func WriteBinary(buffer *jsonbuilder.Stream, p unsafe.Pointer) {
	currentRow := p
	clen := *((*int16)(currentRow))
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)
	if clen == 1 {
		value := *((*byte)(currentRow))
		if value == wrapper.CBinaryNull {
			buffer.WriteNil()
		} else {
			buffer.WriteByte('"')
			buffer.WriteStringByte(value)
			buffer.WriteByte('"')
		}
	} else {
		buffer.WriteByte('"')
		for index := int16(0); index < clen; index++ {
			buffer.WriteStringByte(*((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(index)))))
		}
		buffer.WriteByte('"')
	}
}

func WriteNchar(buffer *jsonbuilder.Stream, p unsafe.Pointer) {
	currentRow := p
	clen := *((*int16)(currentRow))
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)
	if clen == 4 {
		isNil := true
		for i := 0; i < 4; i++ {
			if *((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(i)))) != wrapper.CNcharNull {
				isNil = false
				break
			}
		}
		if isNil {
			buffer.WriteNil()
			return
		}
	}
	buffer.WriteByte('"')
	for index := int16(0); index < clen; index++ {
		buffer.WriteStringByte(*((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(index)))))
	}
	buffer.WriteByte('"')
}

func WriteTimeStamp(buffer *jsonbuilder.Stream, p unsafe.Pointer, precision int, timeFormat FormatTimeFunc) {
	value := *((*int64)(p))
	if value == wrapper.CTimestampNull {
		buffer.WriteNil()
	} else {
		timeFormat(buffer, value, precision)
	}
}

func WriteJson(buffer *jsonbuilder.Stream, p unsafe.Pointer) {
	currentRow := p
	clen := *((*int16)(currentRow))
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)
	if clen == 4 {
		isNil := true
		for i := 0; i < 4; i++ {
			if *((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(i)))) != wrapper.CNcharNull {
				isNil = false
				break
			}
		}
		if isNil {
			buffer.WriteNil()
			return
		}
	}
	for index := int16(0); index < clen; index++ {
		buffer.WriteByte(*((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(index)))))
	}
}

type FormatTimeFunc func(builder *jsonbuilder.Stream, ts int64, precision int)

func JsonWriteRowValue(builder *jsonbuilder.Stream, row unsafe.Pointer, offset int, colType uint8, length int, precision int, timeFormat FormatTimeFunc) {
	p := unsafe.Pointer(*(*uintptr)(unsafe.Pointer(uintptr(row) + uintptr(offset)*wrapper.Step)))
	if p == nil {
		builder.WriteNil()
		return
	}
	switch colType {
	case C.TSDB_DATA_TYPE_BOOL:
		if v := *((*byte)(p)); v != 0 {
			builder.WriteTrue()
		} else {
			builder.WriteFalse()
		}
	case C.TSDB_DATA_TYPE_TINYINT:
		builder.WriteInt8(*((*int8)(p)))
	case C.TSDB_DATA_TYPE_SMALLINT:
		builder.WriteInt16(*((*int16)(p)))
	case C.TSDB_DATA_TYPE_INT:
		builder.WriteInt32(*((*int32)(p)))
	case C.TSDB_DATA_TYPE_BIGINT:
		builder.WriteInt64(*((*int64)(p)))
	case C.TSDB_DATA_TYPE_UTINYINT:
		builder.WriteUint8(*((*uint8)(p)))
	case C.TSDB_DATA_TYPE_USMALLINT:
		builder.WriteUint16(*((*uint16)(p)))
	case C.TSDB_DATA_TYPE_UINT:
		builder.WriteUint32(*((*uint32)(p)))
	case C.TSDB_DATA_TYPE_UBIGINT:
		builder.WriteUint64(*((*uint64)(p)))
	case C.TSDB_DATA_TYPE_FLOAT:
		builder.WriteFloat32(*((*float32)(p)))
	case C.TSDB_DATA_TYPE_DOUBLE:
		builder.WriteFloat64(*((*float64)(p)))
	case C.TSDB_DATA_TYPE_BINARY, C.TSDB_DATA_TYPE_NCHAR:
		builder.WriteByte('"')
		for i := 0; i < length; i++ {
			builder.WriteStringByte(*((*byte)(unsafe.Pointer(uintptr(p) + uintptr(i)))))
		}
		builder.WriteByte('"')
	case C.TSDB_DATA_TYPE_TIMESTAMP:
		timeFormat(builder, *((*int64)(p)), precision)
	case C.TSDB_DATA_TYPE_JSON:
		for i := 0; i < length; i++ {
			builder.WriteByte(*((*byte)(unsafe.Pointer(uintptr(p) + uintptr(i)))))
		}
	default:
		builder.WriteNil()
		return
	}
}

func JsonWriteBlockValue(builder *jsonbuilder.Stream, block unsafe.Pointer, columnType uint8, column int, row int, length int, precision int, timeFormat FormatTimeFunc) {
	pointer := unsafe.Pointer((*(*uintptr)(unsafe.Pointer(uintptr(unsafe.Pointer(*(*C.TAOS_ROW)(block))) + uintptr(column)*wrapper.PointerSize))) + uintptr(row)*uintptr(length))
	switch columnType {
	case C.TSDB_DATA_TYPE_BOOL:
		WriteBool(builder, pointer)
	case C.TSDB_DATA_TYPE_TINYINT:
		WriteTinyint(builder, pointer)
	case C.TSDB_DATA_TYPE_SMALLINT:
		WriteSmallint(builder, pointer)
	case C.TSDB_DATA_TYPE_INT:
		WriteInt(builder, pointer)
	case C.TSDB_DATA_TYPE_BIGINT:
		WriteBigint(builder, pointer)
	case C.TSDB_DATA_TYPE_UTINYINT:
		WriteUTinyint(builder, pointer)
	case C.TSDB_DATA_TYPE_USMALLINT:
		WriteUSmallint(builder, pointer)
	case C.TSDB_DATA_TYPE_UINT:
		WriteUInt(builder, pointer)
	case C.TSDB_DATA_TYPE_UBIGINT:
		WriteUBigint(builder, pointer)
	case C.TSDB_DATA_TYPE_FLOAT:
		WriteFloat(builder, pointer)
	case C.TSDB_DATA_TYPE_DOUBLE:
		WriteDouble(builder, pointer)
	case C.TSDB_DATA_TYPE_BINARY:
		WriteBinary(builder, pointer)
	case C.TSDB_DATA_TYPE_NCHAR:
		WriteNchar(builder, pointer)
	case C.TSDB_DATA_TYPE_TIMESTAMP:
		WriteTimeStamp(builder, pointer, precision, timeFormat)
	case C.TSDB_DATA_TYPE_JSON:
		WriteJson(builder, pointer)
	default:
		builder.WriteNil()
	}
}

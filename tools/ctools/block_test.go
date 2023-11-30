package ctools

import (
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/parser"
	"github.com/taosdata/taosadapter/v3/tools"
	"github.com/taosdata/taosadapter/v3/tools/jsonbuilder"
)

func TestJsonWriteRawBlock(t *testing.T) {
	raw := []byte{
		0x01, 0x00, 0x00, 0x00,
		0xa6, 0x01, 0x00, 0x00,
		0x02, 0x00, 0x00, 0x00,
		0x11, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x80,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

		0x09, 0x08, 0x00, 0x00, 0x00,
		0x01, 0x01, 0x00, 0x00, 0x00,
		0x02, 0x01, 0x00, 0x00, 0x00,
		0x03, 0x02, 0x00, 0x00, 0x00,
		0x04, 0x04, 0x00, 0x00, 0x00,
		0x05, 0x08, 0x00, 0x00, 0x00,
		0x0b, 0x01, 0x00, 0x00, 0x00,
		0x0c, 0x02, 0x00, 0x00, 0x00,
		0x0d, 0x04, 0x00, 0x00, 0x00,
		0x0e, 0x08, 0x00, 0x00, 0x00,
		0x06, 0x04, 0x00, 0x00, 0x00,
		0x07, 0x08, 0x00, 0x00, 0x00,
		0x08, 0x16, 0x00, 0x00, 0x00,
		0x0a, 0x52, 0x00, 0x00, 0x00,
		0x10, 0x20, 0x00, 0x00, 0x00,
		0x14, 0x20, 0x00, 0x00, 0x00,
		0x0f, 0x00, 0x40, 0x00, 0x00,

		0x10, 0x00, 0x00, 0x00,
		0x02, 0x00, 0x00, 0x00,
		0x02, 0x00, 0x00, 0x00,
		0x04, 0x00, 0x00, 0x00,
		0x08, 0x00, 0x00, 0x00,
		0x10, 0x00, 0x00, 0x00,
		0x02, 0x00, 0x00, 0x00,
		0x04, 0x00, 0x00, 0x00,
		0x08, 0x00, 0x00, 0x00,
		0x10, 0x00, 0x00, 0x00,
		0x08, 0x00, 0x00, 0x00,
		0x10, 0x00, 0x00, 0x00,
		0x08, 0x00, 0x00, 0x00,
		0x16, 0x00, 0x00, 0x00,
		0x10, 0x00, 0x00, 0x00,
		0x17, 0x00, 0x00, 0x00,
		0x12, 0x00, 0x00, 0x00,

		0x00,
		0x74, 0x00, 0x90, 0x86, 0x82, 0x01, 0x00, 0x00,
		0x5c, 0x04, 0x90, 0x86, 0x82, 0x01, 0x00, 0x00,

		0x40,
		0x01,
		0x00,

		0x40,
		0x02,
		0x00,

		0x40,
		0x03, 0x00,
		0x00, 0x00,

		0x40,
		0x04, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,

		0x40,
		0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

		0x40,
		0x06,
		0x00,

		0x40,
		0x07, 0x00,
		0x00, 0x00,

		0x40,
		0x08, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,

		0x40,
		0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

		0x40,
		0x00, 0x00, 0x20, 0x41,
		0x00, 0x00, 0x00, 0x00,

		0x40,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x26, 0x40,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

		0x00, 0x00, 0x00, 0x00,
		0xff, 0xff, 0xff, 0xff,
		0x06, 0x00,
		0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,

		0x00, 0x00, 0x00, 0x00,
		0xff, 0xff, 0xff, 0xff,
		0x14, 0x00,
		0x6e, 0x00, 0x00, 0x00, 0x63, 0x00, 0x00, 0x00, 0x68, 0x00, 0x00, 0x00, 0x61, 0x00, 0x00, 0x00, 0x72,
		0x00, 0x00, 0x00,

		0x00, 0x00, 0x00, 0x00,
		0xff, 0xff, 0xff, 0xff,
		0x0e, 0x00,
		0x74, 0x65, 0x73, 0x74, 0x5f, 0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,

		0x00, 0x00, 0x00, 0x00,
		0xff, 0xff, 0xff, 0xff,
		0x15, 0x00,
		0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,

		0x00, 0x00, 0x00, 0x00,
		0x09, 0x00, 0x00, 0x00,
		0x07, 0x00,
		0x7b, 0x22, 0x61, 0x22, 0x3a, 0x31, 0x7d,
		0x07, 0x00,
		0x7b, 0x22, 0x61, 0x22, 0x3a, 0x31, 0x7d,
	}
	w := &strings.Builder{}
	builder := jsonbuilder.BorrowStream(w)
	defer jsonbuilder.ReturnStream(builder)
	fieldsCount := 17
	fieldTypes := []uint8{9, 1, 2, 3, 4, 5, 11, 12, 13, 14, 6, 7, 8, 10, 16, 20, 15}
	blockSize := 2
	precision := 0
	pHeaderList := make([]unsafe.Pointer, fieldsCount)
	pStartList := make([]unsafe.Pointer, fieldsCount)
	nullBitMapOffset := uintptr(BitmapLen(blockSize))
	block := unsafe.Pointer(&raw[0])
	lengthOffset := parser.RawBlockGetColumnLengthOffset(fieldsCount)
	tmpPHeader := tools.AddPointer(block, parser.RawBlockGetColDataOffset(fieldsCount))
	tmpPStart := tmpPHeader
	for column := 0; column < fieldsCount; column++ {
		colLength := *((*int32)(unsafe.Pointer(uintptr(block) + lengthOffset + uintptr(column)*parser.Int32Size)))
		if IsVarDataType(fieldTypes[column]) {
			pHeaderList[column] = tmpPHeader
			tmpPStart = tools.AddPointer(tmpPHeader, uintptr(4*blockSize))
			pStartList[column] = tmpPStart
		} else {
			pHeaderList[column] = tmpPHeader
			tmpPStart = tools.AddPointer(tmpPHeader, nullBitMapOffset)
			pStartList[column] = tmpPStart
		}
		tmpPHeader = tools.AddPointer(tmpPStart, uintptr(colLength))
	}
	timeBuffer := make([]byte, 0, 30)
	builder.WriteObjectStart()
	for row := 0; row < blockSize; row++ {
		builder.WriteArrayStart()
		for column := 0; column < fieldsCount; column++ {
			JsonWriteRawBlock(builder, fieldTypes[column], pHeaderList[column], pStartList[column], row, precision, func(builder *jsonbuilder.Stream, ts int64, precision int) {
				timeBuffer = timeBuffer[:0]
				switch precision {
				case common.PrecisionMilliSecond: // milli-second
					timeBuffer = time.Unix(0, ts*1e6).UTC().AppendFormat(timeBuffer, time.RFC3339Nano)
				case common.PrecisionMicroSecond: // micro-second
					timeBuffer = time.Unix(0, ts*1e3).UTC().AppendFormat(timeBuffer, time.RFC3339Nano)
				case common.PrecisionNanoSecond: // nano-second
					timeBuffer = time.Unix(0, ts).UTC().AppendFormat(timeBuffer, time.RFC3339Nano)
				default:
					panic("unknown precision")
				}
				builder.WriteString(string(timeBuffer))
			})
			if column != fieldsCount-1 {
				builder.WriteMore()
				err := builder.Flush()
				assert.NoError(t, err)
			}
		}
		builder.WriteArrayEnd()
		if row != blockSize-1 {
			builder.WriteMore()
		}
	}
	builder.WriteObjectEnd()
	err := builder.Flush()
	assert.NoError(t, err)
	assert.Equal(t, `{["2022-08-10T07:02:40.5Z",true,2,3,4,5,6,7,8,9,10,11,"binary","nchar","746573745f76617262696e617279","010100000000000000000059400000000000005940",{"a":1}],["2022-08-10T07:02:41.5Z",null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,{"a":1}]}`, w.String())
}

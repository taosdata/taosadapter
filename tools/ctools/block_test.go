package ctools

import (
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/taosadapter/tools/jsonbuilder"
)

func TestJsonWriteRawBlock(t *testing.T) {
	raw := []byte{143, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 0, 8, 0, 0, 0, 1, 0, 1, 0, 0, 0, 2, 0, 1, 0, 0, 0, 3, 0, 2, 0, 0, 0, 4, 0, 4, 0, 0, 0, 5, 0, 8, 0, 0, 0, 11, 0, 1, 0, 0, 0, 12, 0, 2, 0, 0, 0, 13, 0, 4, 0, 0, 0, 14, 0, 8, 0, 0, 0, 6, 0, 4, 0, 0, 0, 7, 0, 8, 0, 0, 0, 8, 0, 22, 0, 0, 0, 10, 0, 82, 0, 0, 0, 15, 0, 0, 64, 0, 0, 16, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 0, 8, 0, 0, 0, 16, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 0, 8, 0, 0, 0, 16, 0, 0, 0, 8, 0, 0, 0, 16, 0, 0, 0, 13, 0, 0, 0, 42, 0, 0, 0, 18, 0, 0, 0, 0, 214, 138, 67, 209, 129, 1, 0, 0, 190, 142, 67, 209, 129, 1, 0, 0, 64, 1, 0, 64, 1, 0, 64, 1, 0, 0, 0, 64, 1, 0, 0, 0, 0, 0, 0, 0, 64, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 1, 0, 64, 1, 0, 0, 0, 64, 1, 0, 0, 0, 0, 0, 0, 0, 64, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 128, 63, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 11, 0, 116, 101, 115, 116, 95, 98, 105, 110, 97, 114, 121, 0, 0, 0, 0, 255, 255, 255, 255, 40, 0, 116, 0, 0, 0, 101, 0, 0, 0, 115, 0, 0, 0, 116, 0, 0, 0, 95, 0, 0, 0, 110, 0, 0, 0, 99, 0, 0, 0, 104, 0, 0, 0, 97, 0, 0, 0, 114, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 7, 0, 123, 34, 97, 34, 58, 49, 125, 7, 0, 123, 34, 97, 34, 58, 49, 125, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	w := &strings.Builder{}
	builder := jsonbuilder.BorrowStream(w)
	defer jsonbuilder.ReturnStream(builder)
	fieldsCount := 15
	fieldTypes := []uint8{9, 1, 2, 3, 4, 5, 11, 12, 13, 14, 6, 7, 8, 10, 15}
	blockSize := 2
	precision := 0
	payloadOffset := uintptr(4 * fieldsCount)
	pHeaderList := make([]uintptr, fieldsCount)
	pStartList := make([]uintptr, fieldsCount)
	nullBitMapOffset := uintptr(BitmapLen(blockSize))
	block := unsafe.Pointer(*(*uintptr)(unsafe.Pointer(&raw)))
	tmpPHeader := uintptr(block) + payloadOffset + 12 + uintptr(6*fieldsCount) // length i32, group u64
	tmpPStart := tmpPHeader
	for column := 0; column < fieldsCount; column++ {
		colLength := *((*int32)(unsafe.Pointer(uintptr(block) + 12 + uintptr(6*fieldsCount) + uintptr(column)*4)))
		if IsVarDataType(fieldTypes[column]) {
			pHeaderList[column] = tmpPHeader
			tmpPStart = tmpPHeader + uintptr(4*blockSize)
			pStartList[column] = tmpPStart
		} else {
			pHeaderList[column] = tmpPHeader
			tmpPStart = tmpPHeader + nullBitMapOffset
			pStartList[column] = tmpPStart
		}
		tmpPHeader = tmpPStart + uintptr(colLength)
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
	assert.Equal(t, "{[\"2022-07-06T02:07:53.558Z\",true,1,1,1,1,1,1,1,1,1,1,\"test_binary\",\"test_nchar\",{\"a\":1}],[\"2022-07-06T02:07:54.558Z\",null,null,null,null,null,null,null,null,null,null,null,null,null,{\"a\":1}]}", w.String())
}

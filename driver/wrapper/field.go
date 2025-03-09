package wrapper

/*
#include <taos.h>
*/
import "C"
import (
	"bytes"
	"reflect"
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/driver/errors"
)

type RowsHeader struct {
	ColNames  []string
	ColTypes  []uint8
	ColLength []int64
}

func ReadColumn(result unsafe.Pointer, count int) (*RowsHeader, error) {
	if result == nil {
		return nil, &errors.TaosError{Code: 0xffff, ErrStr: "invalid result"}
	}
	rowsHeader := &RowsHeader{
		ColNames:  make([]string, count),
		ColTypes:  make([]uint8, count),
		ColLength: make([]int64, count),
	}
	pFields := TaosFetchFields(result)
	for i := 0; i < count; i++ {
		field := *(*C.struct_taosField)(unsafe.Pointer(uintptr(pFields) + uintptr(C.sizeof_struct_taosField*C.int(i))))
		buf := bytes.NewBufferString("")
		for _, c := range field.name {
			if c == 0 {
				break
			}
			buf.WriteByte(byte(c))
		}
		rowsHeader.ColNames[i] = buf.String()
		rowsHeader.ColTypes[i] = (uint8)(field._type)
		rowsHeader.ColLength[i] = int64((uint32)(field.bytes))
	}
	return rowsHeader, nil
}

func ReadColumnV2(pFields unsafe.Pointer, count int32) (*RowsHeader, error) {
	rowsHeader := &RowsHeader{
		ColNames:  make([]string, count),
		ColTypes:  make([]uint8, count),
		ColLength: make([]int64, count),
	}
	for i := int32(0); i < count; i++ {
		field := *(*C.struct_taosField)(unsafe.Pointer(uintptr(pFields) + uintptr(C.sizeof_struct_taosField*C.int(i))))
		buf := bytes.NewBufferString("")
		for _, c := range field.name {
			if c == 0 {
				break
			}
			buf.WriteByte(byte(c))
		}
		rowsHeader.ColNames[i] = buf.String()
		rowsHeader.ColTypes[i] = (uint8)(field._type)
		rowsHeader.ColLength[i] = int64((uint32)(field.bytes))
	}
	return rowsHeader, nil
}

func (rh *RowsHeader) TypeDatabaseName(i int) string {
	return common.TypeNameMap[int(rh.ColTypes[i])]
}

func (rh *RowsHeader) ScanType(i int) reflect.Type {
	t, exist := common.ColumnTypeMap[int(rh.ColTypes[i])]
	if !exist {
		return common.UnknownType
	}
	return t
}

func FetchLengths(res unsafe.Pointer, count int) []int {
	lengths := TaosFetchLengths(res)
	result := make([]int, count)
	for i := 0; i < count; i++ {
		result[i] = int(*(*C.int)(unsafe.Pointer(uintptr(lengths) + uintptr(C.sizeof_int*C.int(i)))))
	}
	return result
}

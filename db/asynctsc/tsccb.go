package asynctsc

/*
#include "taos.h"
#include <stdbool.h>
typedef struct taosa_query_result {
    int code;
    const char *err;
    TAOS_RES *res;
    bool is_update_query;
    int64_t affected_rows;
    int32_t field_count;
    TAOS_FIELD *field;
    int precision;
} taosa_query_result;
*/
import "C"
import (
	"fmt"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
	"unsafe"
)

type Caller interface {
	QueryCall(result *QueryResult)
	FreeCall()
}

type QueryResult struct {
	Code          int
	Err           string
	Res           unsafe.Pointer
	IsUpdateQuery bool
	AffectedRows  int64
	FieldCount    int32
	Field         unsafe.Pointer
	Precision     int
}

//export AdapterQueryCallback
func AdapterQueryCallback(p unsafe.Pointer, result *C.taosa_query_result) {
	caller := (*(*cgo.Handle)(p)).Value().(Caller)
	fmt.Println(unsafe.Pointer(result.err))
	err := C.GoString(result.err)
	fmt.Println(err)
	r := &QueryResult{
		Code: int(result.code),
		Err:  C.GoString(result.err),
	}
	if r.Code != 0 {
		caller.QueryCall(r)
		return
	}
	r.IsUpdateQuery = bool(result.is_update_query)
	if r.IsUpdateQuery {
		r.AffectedRows = int64(result.affected_rows)
		caller.QueryCall(r)
	}
	r.FieldCount = int32(result.field_count)
	r.Field = unsafe.Pointer(result.field)
	r.Precision = int(result.precision)
	caller.QueryCall(r)
}

//export AdapterFreeCallback
func AdapterFreeCallback(p unsafe.Pointer) {
	caller := (*(*cgo.Handle)(p)).Value().(Caller)
	caller.FreeCall()
}

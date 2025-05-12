package wrapper

/*
#cgo CFLAGS: -IC:/TDengine/include -I/usr/include
#cgo linux LDFLAGS: -L/usr/lib -ltaosnative
#cgo windows LDFLAGS: -LC:/TDengine/driver -ltaosnative
#cgo darwin LDFLAGS: -L/usr/local/lib -ltaosnative
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
extern void WhitelistCallback(void *param, int code, TAOS *taos, int numOfWhiteLists, char ** pWhiteLists);
void taos_fetch_whitelist_dual_stack_a_wrapper(TAOS *taos, void *param){
    taos_fetch_whitelist_dual_stack_a(taos, WhitelistCallback, param);
};
*/
import "C"
import (
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
)

//typedef void (*__taos_async_whitelist_dual_stack_fn_t)(void *param, int code, TAOS *taos, int numOfWhiteLists,char **pWhiteLists);

// TaosFetchWhitelistDualStackA DLL_EXPORT void taos_fetch_whitelist_dual_stack_a(TAOS *taos, __taos_async_whitelist_dual_stack_fn_t fp, void *param);
func TaosFetchWhitelistDualStackA(taosConnect unsafe.Pointer, caller cgo.Handle) {
	C.taos_fetch_whitelist_dual_stack_a_wrapper(taosConnect, caller.Pointer())
}

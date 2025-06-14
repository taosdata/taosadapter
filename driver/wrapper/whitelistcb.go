package wrapper

import "C"
import (
	"fmt"
	"net"
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
)

type WhitelistResult struct {
	Err    error
	IPNets []*net.IPNet
}

//typedef void (*__taos_async_whitelist_dual_stack_fn_t)(void *param, int code, TAOS *taos, int numOfWhiteLists,char **pWhiteLists);

//export WhitelistCallback
func WhitelistCallback(param unsafe.Pointer, code int, taosConnect unsafe.Pointer, numOfWhiteLists int, pWhiteLists unsafe.Pointer) {
	c := (*(*cgo.Handle)(param)).Value().(chan *WhitelistResult)
	if code != 0 {
		errStr := TaosErrorStr(nil)
		c <- &WhitelistResult{Err: errors.NewError(code, errStr)}
		return
	}
	ips := make([]*net.IPNet, 0, numOfWhiteLists)
	for i := 0; i < numOfWhiteLists; i++ {
		cStrPtr := *(**C.char)(unsafe.Pointer(uintptr(pWhiteLists) + uintptr(i)*unsafe.Sizeof(uintptr(0))))
		s := C.GoString(cStrPtr)
		_, ipNet, err := net.ParseCIDR(s)
		if err != nil {
			c <- &WhitelistResult{Err: errors.NewError(0xffff, fmt.Sprintf("ParseCIDR error: %s", err.Error()))}
			return
		}
		ips = append(ips, ipNet)
	}
	c <- &WhitelistResult{IPNets: ips}
}

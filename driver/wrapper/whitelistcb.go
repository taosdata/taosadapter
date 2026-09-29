// Copyright (c) 2024 TAOS Data, Inc.
//
// SPDX-License-Identifier: MIT
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

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
	Err         error
	AllowIPNets []*net.IPNet
	BlockIPNets []*net.IPNet
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
	var allowlist, blocklist []*net.IPNet
	for i := 0; i < numOfWhiteLists; i++ {
		cStrPtr := *(**C.char)(unsafe.Pointer(uintptr(pWhiteLists) + uintptr(i)*unsafe.Sizeof(uintptr(0))))
		s := C.GoString(cStrPtr)
		if len(s) < 2 {
			c <- &WhitelistResult{Err: errors.NewError(0xffff, fmt.Sprintf("Unexpected whitelist format: %s", string(s)))}
			return
		}
		_, ipNet, err := net.ParseCIDR(s[2:])
		if err != nil {
			c <- &WhitelistResult{Err: errors.NewError(0xffff, fmt.Sprintf("ParseCIDR error: %s", err.Error()))}
			return
		}
		switch s[0] {
		case '+':
			allowlist = append(allowlist, ipNet)
		case '-':
			blocklist = append(blocklist, ipNet)
		default:
			c <- &WhitelistResult{Err: errors.NewError(0xffff, fmt.Sprintf("Unexpected whitelist prefix: %s", string(s)))}
			return
		}
	}
	c <- &WhitelistResult{AllowIPNets: allowlist, BlockIPNets: blocklist}
}

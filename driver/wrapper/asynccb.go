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

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>

*/
import "C"
import (
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
)

type Caller interface {
	QueryCall(res unsafe.Pointer, code int)
	FetchCall(res unsafe.Pointer, numOfRows int)
}

//export QueryCallback
func QueryCallback(p unsafe.Pointer, res *C.TAOS_RES, code C.int) {
	caller := (*(*cgo.Handle)(p)).Value().(Caller)
	caller.QueryCall(unsafe.Pointer(res), int(code))
}

//export FetchRowsCallback
func FetchRowsCallback(p unsafe.Pointer, res *C.TAOS_RES, numOfRows C.int) {
	caller := (*(*cgo.Handle)(p)).Value().(Caller)
	caller.FetchCall(unsafe.Pointer(res), int(numOfRows))
}

//export FetchRawBlockCallback
func FetchRawBlockCallback(p unsafe.Pointer, res *C.TAOS_RES, numOfRows C.int) {
	caller := (*(*cgo.Handle)(p)).Value().(Caller)
	caller.FetchCall(unsafe.Pointer(res), int(numOfRows))
}

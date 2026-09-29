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

//typedef void(tmq_commit_cb(tmq_t *, int32_t code, void *param));

//export TMQCommitCB
func TMQCommitCB(consumer unsafe.Pointer, resp C.int32_t, param unsafe.Pointer) {
	c := (*(*cgo.Handle)(param)).Value().(chan *TMQCommitCallbackResult)
	r := GetTMQCommitCallbackResult(int32(resp), consumer)
	defer func() {
		// Avoid panic due to channel closed
		_ = recover()
	}()
	c <- r
}

//export TMQAutoCommitCB
func TMQAutoCommitCB(consumer unsafe.Pointer, resp C.int32_t, param unsafe.Pointer) {
	c := (*(*cgo.Handle)(param)).Value().(chan *TMQCommitCallbackResult)
	r := GetTMQCommitCallbackResult(int32(resp), consumer)
	defer func() {
		// Avoid panic due to channel closed
		_ = recover()
	}()
	c <- r
}

//export TMQCommitOffsetCB
func TMQCommitOffsetCB(consumer unsafe.Pointer, resp C.int32_t, param unsafe.Pointer) {
	c := (*(*cgo.Handle)(param)).Value().(chan *TMQCommitCallbackResult)
	r := GetTMQCommitCallbackResult(int32(resp), consumer)
	defer func() {
		// Avoid panic due to channel closed
		_ = recover()
	}()
	c <- r
}

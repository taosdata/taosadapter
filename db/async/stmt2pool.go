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

package async

import (
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
)

type Stmt2Result struct {
	Res      unsafe.Pointer
	Affected int
	N        int
}

type Stmt2CallBackCaller struct {
	ExecResult chan *Stmt2Result
}

type Stmt2CallBackCallerPool struct {
	pool chan cgo.Handle
}

const Stmt2CBPoolSize = 10000

func NewStmt2CallBackCallerPool(size int) *Stmt2CallBackCallerPool {
	return &Stmt2CallBackCallerPool{
		pool: make(chan cgo.Handle, size),
	}
}

func (p *Stmt2CallBackCallerPool) Get() (cgo.Handle, *Stmt2CallBackCaller) {
	select {
	case handle := <-p.pool:
		c := handle.Value().(*Stmt2CallBackCaller)
		// cleanup channel
		for {
			select {
			case <-c.ExecResult:
			default:
				return handle, c
			}
		}
	default:
		c := &Stmt2CallBackCaller{
			ExecResult: make(chan *Stmt2Result, 1),
		}
		return cgo.NewHandle(c), c
	}
}

func (p *Stmt2CallBackCallerPool) Put(h cgo.Handle) {
	select {
	case p.pool <- h:
	default:
		h.Delete()
	}
}

func (s *Stmt2CallBackCaller) ExecCall(res unsafe.Pointer, affected int, code int) {
	s.ExecResult <- &Stmt2Result{
		Res:      res,
		Affected: affected,
		N:        code,
	}
}

var GlobalStmt2CallBackCallerPool = NewStmt2CallBackCallerPool(Stmt2CBPoolSize)

// Copyright (c) 2021 TAOS Data, Inc.
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
	"container/list"
	"sync"
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
)

type Result struct {
	Res unsafe.Pointer
	N   int
}
type Caller struct {
	QueryResult chan *Result
	FetchResult chan *Result
}

func NewCaller() *Caller {
	return &Caller{
		QueryResult: make(chan *Result, 1),
		FetchResult: make(chan *Result, 1),
	}
}

func (c *Caller) QueryCall(res unsafe.Pointer, code int) {
	c.QueryResult <- &Result{
		Res: res,
		N:   code,
	}
}

func (c *Caller) FetchCall(res unsafe.Pointer, numOfRows int) {
	c.FetchResult <- &Result{
		Res: res,
		N:   numOfRows,
	}
}

type poolReq struct {
	idleHandler *Handler
}

type HandlerPool struct {
	mu       sync.RWMutex
	count    int
	handlers chan *Handler
	reqList  *list.List
}

type Handler struct {
	Handler cgo.Handle
	Caller  *Caller
}

func NewHandlerPool(count int) *HandlerPool {
	c := &HandlerPool{
		count:    count,
		handlers: make(chan *Handler, count),
		reqList:  list.New(),
	}
	for i := 0; i < count; i++ {
		caller := NewCaller()
		c.handlers <- &Handler{
			Handler: cgo.NewHandle(caller),
			Caller:  caller,
		}
	}
	return c
}

func (c *HandlerPool) Get() *Handler {
	for {
		select {
		case wrapConn := <-c.handlers:
			return wrapConn
		default:
			c.mu.Lock()
			req := make(chan poolReq, 1)
			c.reqList.PushBack(req)
			c.mu.Unlock()
			ret := <-req
			return ret.idleHandler
		}
	}
}

func (c *HandlerPool) Put(handler *Handler) {
	c.mu.Lock()
	e := c.reqList.Front()
	if e != nil {
		req := e.Value.(chan poolReq)
		c.reqList.Remove(e)
		req <- poolReq{
			idleHandler: handler,
		}
		c.mu.Unlock()
		return
	}
	c.handlers <- handler
	c.mu.Unlock()
}

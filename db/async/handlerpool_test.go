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
	"testing"
	"time"
)

func BenchmarkName(b *testing.B) {
	pool := NewHandlerPool(1)
	for i := 0; i < b.N; i++ {
		h := pool.Get()
		pool.Put(h)
	}
}

// @author: xftan
// @date: 2021/12/14 15:00
// @description: test func NewHandlerPool
func TestNewHandlerPool(t *testing.T) {
	type args struct {
		count int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test",
			args: args{
				count: 100,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(_ *testing.T) {
			got := NewHandlerPool(tt.args.count)
			l := make([]*Handler, tt.args.count)
			for i := 0; i < tt.args.count; i++ {
				l[i] = got.Get()
			}
			for _, handler := range l {
				got.Put(handler)
			}
		})
	}
}

// @author: xftan
// @date: 2021/12/14 15:01
// @description: test func HandlerPool.Get
func TestHandlerPool_Get(t *testing.T) {
	pool := NewHandlerPool(1)
	h := pool.Get()
	go func() {
		time.Sleep(time.Millisecond)
		pool.Put(h)
	}()
	h2 := pool.Get()
	pool.Put(h2)
}

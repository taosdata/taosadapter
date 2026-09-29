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
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestStmt2CallBackCallerPool(t *testing.T) {
	pool := NewStmt2CallBackCallerPool(2)

	// Test Get method
	handle1, caller1 := pool.Get()
	if handle1 == 0 || caller1 == nil {
		t.Errorf("Expected handle and caller to be not nil")
	}

	handle2, caller2 := pool.Get()
	if handle2 == 0 || caller2 == nil {
		t.Errorf("Expected handle and caller to be not nil")
	}

	// Test Put method
	pool.Put(handle1)
	pool.Put(handle2)

	// Test Get method after Put
	handle3, caller3 := pool.Get()
	if handle3 == 0 || caller3 == nil {
		t.Errorf("Expected handle and caller to be not nil after Put")
	}
	assert.Equal(t, handle1, handle3)

	handle4, caller4 := pool.Get()
	if handle4 == 0 || caller4 == nil {
		t.Errorf("Expected handle and caller to be not nil after Put")
	}
	assert.Equal(t, handle2, handle4)

	handle5, caller5 := pool.Get()
	if handle5 == 0 || caller5 == nil {
		t.Errorf("Expected handle and caller to be not nil after Put")
	}

	pool.Put(handle5)
	pool.Put(handle4)
	pool.Put(handle3)

	handle6, caller6 := pool.Get()
	if handle6 == 0 || caller6 == nil {
		t.Errorf("Expected handle and caller to be not nil after Put")
	}
	assert.Equal(t, handle5, handle6)
}

func TestExecCall(t *testing.T) {
	handle1, caller1 := GlobalStmt2CallBackCallerPool.Get()
	assert.NotNil(t, caller1)
	defer GlobalStmt2CallBackCallerPool.Put(handle1)
	x := 1
	p := unsafe.Pointer(&x)
	caller1.ExecCall(p, 100, 1000)
	res := <-caller1.ExecResult
	assert.NotNil(t, res)
	assert.Equal(t, p, res.Res)
	assert.Equal(t, 100, res.Affected)
	assert.Equal(t, 1000, res.N)
}

func TestCleanChannel(t *testing.T) {
	handle1, caller1 := GlobalStmt2CallBackCallerPool.Get()
	assert.NotNil(t, caller1)
	GlobalStmt2CallBackCallerPool.Put(handle1)
	caller1.ExecResult <- &Stmt2Result{}
	handle2, caller2 := GlobalStmt2CallBackCallerPool.Get()
	assert.Equal(t, handle1, handle2)
	assert.Equal(t, 0, len(caller2.ExecResult))
}

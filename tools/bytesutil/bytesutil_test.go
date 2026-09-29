// Copyright (c) 2022 TAOS Data, Inc.
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

package bytesutil

import (
	"bytes"
	"testing"
)

func TestRoundToNearestPow2(t *testing.T) {
	f := func(n, resultExpected int) {
		t.Helper()
		result := roundToNearestPow2(n)
		if result != resultExpected {
			t.Fatalf("unexpected roundtoNearestPow2(%d); got %d; want %d", n, result, resultExpected)
		}
	}
	f(1, 1)
	f(2, 2)
	f(3, 4)
	f(4, 4)
	f(5, 8)
	f(6, 8)
	f(7, 8)
	f(8, 8)
	f(9, 16)
	f(10, 16)
	f(16, 16)
	f(17, 32)
	f(32, 32)
	f(33, 64)
	f(64, 64)
}

func TestResizeNoCopyNoOverallocate(t *testing.T) {
	for i := 0; i < 1000; i++ {
		b := ResizeNoCopyNoOverallocate(nil, i)
		if len(b) != i {
			t.Fatalf("invalid b size; got %d; want %d", len(b), i)
		}
		if cap(b) != i {
			t.Fatalf("invalid cap(b); got %d; want %d", cap(b), i)
		}
		b1 := ResizeNoCopyNoOverallocate(b, i)
		if len(b1) != len(b) || (len(b) > 0 && &b1[0] != &b[0]) {
			t.Fatalf("invalid b1; got %x; want %x", &b1[0], &b[0])
		}
		if cap(b1) != i {
			t.Fatalf("invalid cap(b1); got %d; want %d", cap(b1), i)
		}
		b2 := ResizeNoCopyNoOverallocate(b[:0], i)
		if len(b2) != len(b) || (len(b) > 0 && &b2[0] != &b[0]) {
			t.Fatalf("invalid b2; got %x; want %x", &b2[0], &b[0])
		}
		if cap(b2) != i {
			t.Fatalf("invalid cap(b2); got %d; want %d", cap(b2), i)
		}
		if i > 0 {
			b[0] = 123
			b3 := ResizeNoCopyNoOverallocate(b, i+1)
			if len(b3) != i+1 {
				t.Fatalf("invalid b3 len; got %d; want %d", len(b3), i+1)
			}
			if cap(b3) != i+1 {
				t.Fatalf("invalid cap(b3); got %d; want %d", cap(b3), i+1)
			}
			if &b3[0] == &b[0] {
				t.Fatalf("b3 must be newly allocated")
			}
			if b3[0] != 0 {
				t.Fatalf("b3[0] must be zeroed; got %d", b3[0])
			}
		}
	}
}

func TestResizeNoCopyMayOverallocate(t *testing.T) {
	for i := 0; i < 1000; i++ {
		b := ResizeNoCopyMayOverallocate(nil, i)
		if len(b) != i {
			t.Fatalf("invalid b size; got %d; want %d", len(b), i)
		}
		capExpected := roundToNearestPow2(i)
		if cap(b) != capExpected {
			t.Fatalf("invalid cap(b); got %d; want %d", cap(b), capExpected)
		}
		b1 := ResizeNoCopyMayOverallocate(b, i)
		if len(b1) != len(b) || (len(b) > 0 && &b1[0] != &b[0]) {
			t.Fatalf("invalid b1; got %x; want %x", &b1[0], &b[0])
		}
		if cap(b1) != capExpected {
			t.Fatalf("invalid cap(b1); got %d; want %d", cap(b1), capExpected)
		}
		b2 := ResizeNoCopyMayOverallocate(b[:0], i)
		if len(b2) != len(b) || (len(b) > 0 && &b2[0] != &b[0]) {
			t.Fatalf("invalid b2; got %x; want %x", &b2[0], &b[0])
		}
		if cap(b2) != capExpected {
			t.Fatalf("invalid cap(b2); got %d; want %d", cap(b2), capExpected)
		}
		if i > 0 {
			b3 := ResizeNoCopyMayOverallocate(b, i+1)
			if len(b3) != i+1 {
				t.Fatalf("invalid b3 len; got %d; want %d", len(b3), i+1)
			}
			capExpected = roundToNearestPow2(i + 1)
			if cap(b3) != capExpected {
				t.Fatalf("invalid cap(b3); got %d; want %d", cap(b3), capExpected)
			}
		}
	}
}

func TestResizeWithCopyNoOverallocate(t *testing.T) {
	for i := 0; i < 1000; i++ {
		b := ResizeWithCopyNoOverallocate(nil, i)
		if len(b) != i {
			t.Fatalf("invalid b size; got %d; want %d", len(b), i)
		}
		if cap(b) != i {
			t.Fatalf("invalid cap(b); got %d; want %d", cap(b), i)
		}
		b1 := ResizeWithCopyNoOverallocate(b, i)
		if len(b1) != len(b) || (len(b) > 0 && &b1[0] != &b[0]) {
			t.Fatalf("invalid b1; got %x; want %x", &b1[0], &b[0])
		}
		if cap(b1) != i {
			t.Fatalf("invalid cap(b1); got %d; want %d", cap(b1), i)
		}
		b2 := ResizeWithCopyNoOverallocate(b[:0], i)
		if len(b2) != len(b) || (len(b) > 0 && &b2[0] != &b[0]) {
			t.Fatalf("invalid b2; got %x; want %x", &b2[0], &b[0])
		}
		if cap(b2) != i {
			t.Fatalf("invalid cap(b2); got %d; want %d", cap(b2), i)
		}
		if i > 0 {
			b[0] = 123
			b3 := ResizeWithCopyNoOverallocate(b, i+1)
			if len(b3) != i+1 {
				t.Fatalf("invalid b3 len; got %d; want %d", len(b3), i+1)
			}
			if cap(b3) != i+1 {
				t.Fatalf("invalid cap(b3); got %d; want %d", cap(b3), i+1)
			}
			if &b3[0] == &b[0] {
				t.Fatalf("b3 must be newly allocated for i=%d", i)
			}
			if b3[0] != b[0] || b3[0] != 123 {
				t.Fatalf("b3[0] must equal b[0]; got %d; want %d", b3[0], b[0])
			}
		}
	}
}

func TestResizeWithCopyMayOverallocate(t *testing.T) {
	for i := 0; i < 1000; i++ {
		b := ResizeWithCopyMayOverallocate(nil, i)
		if len(b) != i {
			t.Fatalf("invalid b size; got %d; want %d", len(b), i)
		}
		capExpected := roundToNearestPow2(i)
		if cap(b) != capExpected {
			t.Fatalf("invalid cap(b); got %d; want %d", cap(b), capExpected)
		}
		b1 := ResizeWithCopyMayOverallocate(b, i)
		if len(b1) != len(b) || (len(b) > 0 && &b1[0] != &b[0]) {
			t.Fatalf("invalid b1; got %x; want %x", &b1[0], &b[0])
		}
		if cap(b1) != capExpected {
			t.Fatalf("invalid cap(b1); got %d; want %d", cap(b1), capExpected)
		}
		b2 := ResizeWithCopyMayOverallocate(b[:0], i)
		if len(b2) != len(b) || (len(b) > 0 && &b2[0] != &b[0]) {
			t.Fatalf("invalid b2; got %x; want %x", &b2[0], &b[0])
		}
		if cap(b2) != capExpected {
			t.Fatalf("invalid cap(b2); got %d; want %d", cap(b2), capExpected)
		}
		if i > 0 {
			b[0] = 123
			b3 := ResizeWithCopyMayOverallocate(b, i+1)
			if len(b3) != i+1 {
				t.Fatalf("invalid b3 len; got %d; want %d", len(b3), i+1)
			}
			capExpected = roundToNearestPow2(i + 1)
			if cap(b3) != capExpected {
				t.Fatalf("invalid cap(b3); got %d; want %d", cap(b3), capExpected)
			}
			if b3[0] != b[0] || b3[0] != 123 {
				t.Fatalf("b3[0] must equal b[0]; got %d; want %d", b3[0], b[0])
			}
		}
	}
}

func TestToUnsafeString(t *testing.T) {
	s := "str"
	if !bytes.Equal([]byte("str"), ToUnsafeBytes(s)) {
		t.Fatalf(`[]bytes(%s) doesnt equal to %s `, s, s)
	}
	s = ""
	if !bytes.Equal([]byte(""), ToUnsafeBytes(s)) {
		t.Fatalf(`[]bytes(%s) doesnt equal to %s `, s, s)
	}
}

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
	"math/bits"
	"unsafe"
)

// ResizeWithCopyMayOverallocate resizes b to minimum n bytes and returns the resized buffer (which may be newly allocated).
//
// If newly allocated buffer is returned then b contents is copied to it.
func ResizeWithCopyMayOverallocate(b []byte, n int) []byte {
	if n <= cap(b) {
		return b[:n]
	}
	nNew := roundToNearestPow2(n)
	bNew := make([]byte, nNew)
	copy(bNew, b)
	return bNew[:n]
}

// ResizeWithCopyNoOverallocate resizes b to exactly n bytes and returns the resized buffer (which may be newly allocated).
//
// If newly allocated buffer is returned then b contents is copied to it.
func ResizeWithCopyNoOverallocate(b []byte, n int) []byte {
	if n <= cap(b) {
		return b[:n]
	}
	bNew := make([]byte, n)
	copy(bNew, b)
	return bNew
}

// ResizeNoCopyMayOverallocate resizes b to minimum n bytes and returns the resized buffer (which may be newly allocated).
//
// If newly allocated buffer is returned then b contents isn't copied to it.
func ResizeNoCopyMayOverallocate(b []byte, n int) []byte {
	if n <= cap(b) {
		return b[:n]
	}
	nNew := roundToNearestPow2(n)
	bNew := make([]byte, nNew)
	return bNew[:n]
}

// ResizeNoCopyNoOverallocate resizes b to exactly n bytes and returns the resized buffer (which may be newly allocated).
//
// If newly allocated buffer is returned then b contents isn't copied to it.
func ResizeNoCopyNoOverallocate(b []byte, n int) []byte {
	if n <= cap(b) {
		return b[:n]
	}
	return make([]byte, n)
}

// roundToNearestPow2 rounds n to the nearest power of 2
//
// It is expected that n > 0
func roundToNearestPow2(n int) int {
	pow2 := uint8(bits.Len(uint(n - 1)))
	return 1 << pow2
}

// ToUnsafeString converts b to string without memory allocations.
//
// The returned string is valid only until b is reachable and unmodified.
func ToUnsafeString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// stringHeader instead of reflect.StringHeader
type stringHeader struct {
	data unsafe.Pointer
	len  int
}

// sliceHeader instead of reflect.SliceHeader
type sliceHeader struct {
	data unsafe.Pointer
	len  int
	cap  int
}

// ToUnsafeBytes converts s to a byte slice without memory allocations.
//
// The returned byte slice is valid only until s is reachable and unmodified.
func ToUnsafeBytes(s string) (b []byte) {
	if len(s) == 0 {
		return []byte{}
	}
	hdr := (*sliceHeader)(unsafe.Pointer(&b))
	hdr.data = (*stringHeader)(unsafe.Pointer(&s)).data
	hdr.cap = len(s)
	hdr.len = len(s)
	return b
}

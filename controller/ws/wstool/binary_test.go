// Copyright (c) 2023 TAOS Data, Inc.
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

package wstool

import (
	"bytes"
	"testing"
)

func TestWriteUint64(t *testing.T) {
	var buf bytes.Buffer
	var expected = []byte{0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01}
	WriteUint64(&buf, 0x0123456789ABCDEF)
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("WriteUint64 produces incorrect output, got %v, expected %v", buf.Bytes(), expected)
	}
}

func TestWriteUint32(t *testing.T) {
	var buf bytes.Buffer
	var expected = []byte{0x67, 0x45, 0x23, 0x01}
	WriteUint32(&buf, 0x01234567)
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("WriteUint32 produces incorrect output, got %v, expected %v", buf.Bytes(), expected)
	}
}

func TestWriteUint16(t *testing.T) {
	var buf bytes.Buffer
	var expected = []byte{0x23, 0x01}
	WriteUint16(&buf, 0x0123)
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("WriteUint16 produces incorrect output, got %v, expected %v", buf.Bytes(), expected)
	}
}

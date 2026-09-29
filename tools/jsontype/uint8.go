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

package jsontype

// JsonUint8 is a wrapper for []uint8 to implement json.Marshaler interface.
type JsonUint8 []uint8

var digits []uint32

func init() {
	digits = make([]uint32, 1000)
	for i := uint32(0); i < 1000; i++ {
		digits[i] = (((i / 100) + '0') << 16) + ((((i / 10) % 10) + '0') << 8) + i%10 + '0'
		if i < 10 {
			digits[i] += 2 << 24
		} else if i < 100 {
			digits[i] += 1 << 24
		}
	}
}
func writeFirstBuf(space []byte, v uint32) []byte {
	start := v >> 24
	switch start {
	case 0:
		space = append(space, byte(v>>16), byte(v>>8))
	case 1:
		space = append(space, byte(v>>8))
	}
	space = append(space, byte(v))
	return space
}

// MarshalJSON implements the json.Marshaler interface.
func (m JsonUint8) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	if len(m) == 0 {
		return []byte{'[', ']'}, nil
	}
	var b []byte
	b = append(b, '[')
	b = writeFirstBuf(b, digits[m[0]])
	for i := 1; i < len(m); i++ {
		b = append(b, ',')
		b = writeFirstBuf(b, digits[m[i]])
	}
	b = append(b, ']')
	return b, nil
}

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

package jsonbuilder

import (
	"math"
	"strconv"
)

var pow10 []uint64

func init() {
	pow10 = []uint64{
		1,
		10,
		100,
		1000,
		10000,
		100000,
		1000000,
		10000000,
		100000000,
		1000000000,
		10000000000,
		100000000000,
		1000000000000,
		10000000000000,
		100000000000000,
		1000000000000000,
		10000000000000000,
	}
}

// WriteFloat32 write float32 to stream
func (stream *Stream) WriteFloat32(val float32) {
	if math.IsInf(float64(val), 0) || math.IsNaN(float64(val)) {
		stream.WriteNil()
		return
	}
	abs := math.Abs(float64(val))
	fmt := byte('f')
	// Note: Must use float32 comparisons for underlying float32 value to get precise cutoffs right.
	if abs != 0 {
		if float32(abs) < 1e-6 || float32(abs) >= 1e21 {
			fmt = 'e'
		}
	}
	stream.buf = strconv.AppendFloat(stream.buf, float64(val), fmt, -1, 32)
}

// WriteFloat32Lossy write float32 to stream with ONLY 6 digits precision although much much faster
func (stream *Stream) WriteFloat32Lossy(val float32) {
	if math.IsInf(float64(val), 0) || math.IsNaN(float64(val)) {
		stream.WriteNil()
		return
	}
	if val < 0 {
		stream.writeByte('-')
		val = -val
	}
	if val > 0x4ffffff {
		stream.WriteFloat32(val)
		return
	}
	precision := 6
	exp := uint64(1000000) // 6
	lval := uint64(float64(val)*float64(exp) + 0.5)
	stream.WriteUint64(lval / exp)
	fval := lval % exp
	if fval == 0 {
		return
	}
	stream.writeByte('.')
	for p := precision - 1; p > 0 && fval < pow10[p]; p-- {
		stream.writeByte('0')
	}
	stream.WriteUint64(fval)
	for stream.buf[len(stream.buf)-1] == '0' {
		stream.buf = stream.buf[:len(stream.buf)-1]
	}
}

// WriteFloat64 write float64 to stream
func (stream *Stream) WriteFloat64(val float64) {
	if math.IsInf(val, 0) || math.IsNaN(val) {
		stream.WriteNil()
		return
	}
	abs := math.Abs(val)
	fmt := byte('f')
	// Note: Must use float32 comparisons for underlying float32 value to get precise cutoffs right.
	if abs != 0 {
		if abs < 1e-6 || abs >= 1e21 {
			fmt = 'e'
		}
	}
	stream.buf = strconv.AppendFloat(stream.buf, float64(val), fmt, -1, 64)
}

// WriteFloat64Lossy write float64 to stream with ONLY 17 digits precision although much much faster
func (stream *Stream) WriteFloat64Lossy(val float64) {
	if math.IsInf(val, 0) || math.IsNaN(val) {
		stream.WriteNil()
		return
	}
	if val < 0 {
		stream.writeByte('-')
		val = -val
	}
	if val > 0x4ffffff {
		stream.WriteFloat64(val)
		return
	}
	precision := 16
	exp := uint64(10000000000000000) // 17
	lval := uint64(val*float64(exp) + 0.5)
	stream.WriteUint64(lval / exp)
	fval := lval % exp
	if fval == 0 {
		return
	}
	stream.writeByte('.')
	for p := precision - 1; p > 0 && fval < pow10[p]; p-- {
		stream.writeByte('0')
	}
	stream.WriteUint64(fval)
	for stream.buf[len(stream.buf)-1] == '0' {
		stream.buf = stream.buf[:len(stream.buf)-1]
	}
}

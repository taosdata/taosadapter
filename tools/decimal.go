// Copyright (c) 2025 TAOS Data, Inc.
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

package tools

import (
	"math/big"
	"strings"
)

func FormatI128(hi int64, lo uint64) string {
	num := new(big.Int).SetInt64(hi)
	num.Lsh(num, 64)
	num.Or(num, new(big.Int).SetUint64(lo))
	return num.String()
}

func FormatDecimal(str string, scale int) string {
	if scale == 0 {
		return str
	}
	builder := strings.Builder{}
	if strings.HasPrefix(str, "-") {
		str = str[1:]
		builder.WriteByte('-')
	}

	delta := len(str) - scale
	if delta > 0 {
		builder.Grow(len(str) + 1)
		builder.WriteString(str[:delta])
		builder.WriteString(".")
		builder.WriteString(str[delta:])
		return builder.String()
	}
	delta = -delta
	builder.Grow(len(str) + 2 + delta)
	builder.WriteString("0.")
	for i := 0; i < delta; i++ {
		builder.WriteString("0")
	}
	builder.WriteString(str)
	return builder.String()
}

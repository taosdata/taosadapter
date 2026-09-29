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

package sqltype

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

const insertStr = "insert"
const selectStr = "select"

type SqlType int

const (
	OtherType  SqlType = -1
	InsertType SqlType = 1
	SelectType SqlType = 2
)

var asciiSpace = [256]uint8{'\t': 1, '\n': 1, '\v': 1, '\f': 1, '\r': 1, ' ': 1}

func GetSqlType(sql string) SqlType {
	// don't use strings.TrimSpace to avoid extra memory allocation and time cost for large sql
	// find the first non-space character
	start := 0
	for ; start < len(sql); start++ {
		c := sql[start]
		if c >= utf8.RuneSelf {
			// If we run into a non-ASCII byte, return other type
			return OtherType
		}
		if asciiSpace[c] == 0 {
			break
		}
	}
	if len(sql)-start < 6 {
		return OtherType
	}
	switch strings.ToLower(sql[start : start+6]) {
	case insertStr:
		return InsertType
	case selectStr:
		return SelectType
	}
	return OtherType
}

// RemoveSpacesAndLowercase removes all spaces from the input string and converts it to lowercase.
// If maxResultLen is greater than 0, the function stops processing once the result grows to at least maxResultLen bytes.
func RemoveSpacesAndLowercase(str string, maxResultLen int) string {
	var result strings.Builder
	if maxResultLen > 0 {
		result.Grow(maxResultLen)
	} else {
		result.Grow(len(str))
	}
	for _, ch := range str {
		if !unicode.IsSpace(ch) {
			result.WriteRune(unicode.ToLower(ch))
		}
		if maxResultLen > 0 && result.Len() >= maxResultLen {
			break
		}
	}
	return result.String()
}

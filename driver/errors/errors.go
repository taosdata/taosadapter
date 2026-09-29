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

//! TDengine error codes.
//! THIS IS AUTO GENERATED FROM TDENGINE <taoserror.h>, MAKE SURE YOU KNOW WHAT YOU ARE CHANING.

package errors

import "fmt"

type TaosError struct {
	Code   int32
	ErrStr string
}

const (
	SUCCESS int32 = 0
	UNKNOWN int32 = 0xffff
)

func (e *TaosError) Error() string {
	if e.Code != UNKNOWN {
		return fmt.Sprintf("[0x%x] %s", e.Code, e.ErrStr)
	}
	return e.ErrStr
}

func NewError(code int, errStr string) error {
	return &TaosError{
		Code:   int32(code) & 0xffff,
		ErrStr: errStr,
	}
}

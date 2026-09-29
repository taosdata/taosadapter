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

package errors

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// @author: xftan
// @date: 2023/10/13 11:20
// @description: test new error
func TestNewError(t *testing.T) {
	type args struct {
		code   int
		errStr string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "common",
			args: args{
				code:   0,
				errStr: "success",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := NewError(tt.args.code, tt.args.errStr); (err != nil) != tt.wantErr {
				t.Errorf("NewError() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestError(t *testing.T) {
	var ErrTscInvalidConnection = &TaosError{
		Code:   0x020B,
		ErrStr: "Invalid connection",
	}
	invalidError := ErrTscInvalidConnection.Error()
	assert.Equal(t, "[0x20b] Invalid connection", invalidError)
	unknownError := &TaosError{
		Code:   0xffff,
		ErrStr: "unknown error",
	}
	assert.Equal(t, "unknown error", unknownError.Error())
}

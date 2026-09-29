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

package log

import (
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestTaosSqlLogFormatter_Format(t1 *testing.T) {
	type args struct {
		entry *logrus.Entry
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "common",
			args: args{
				entry: &logrus.Entry{
					Time:    time.Unix(1657084598, 0),
					Message: "select 1",
				},
			},
			want: []byte(fmt.Sprintf("%s %s select 1\n", time.Unix(1657084598, 0).Format("01/02 15:04:05.000000"), ServerID)),
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				if err != nil {
					t.Errorf("%s,%v", err.Error(), i)
					return false
				}
				return true
			},
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TaosSqlLogFormatter{}
			got, err := t.Format(tt.args.entry)
			if !tt.wantErr(t1, err, fmt.Sprintf("Format(%v)", tt.args.entry)) {
				return
			}
			assert.Equalf(t1, tt.want, got, "Format(%v)", tt.args.entry)
		})
	}
}

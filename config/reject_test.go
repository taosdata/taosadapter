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

package config

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestReject_GetRejectQuerySqlRegex(t *testing.T) {
	type fields struct {
		rejectQuerySqlRegex []*regexp.Regexp
	}
	tests := []struct {
		name   string
		fields fields
		want   []*regexp.Regexp
	}{
		{
			name: "test1",
			fields: fields{
				rejectQuerySqlRegex: []*regexp.Regexp{
					regexp.MustCompile(`^SELECT \* FROM sensitive_table`),
					regexp.MustCompile(`^DELETE FROM important_data`),
				},
			},
			want: []*regexp.Regexp{
				regexp.MustCompile(`^SELECT \* FROM sensitive_table`),
				regexp.MustCompile(`^DELETE FROM important_data`),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Reject{
				rejectQuerySqlRegex: tt.fields.rejectQuerySqlRegex,
			}
			assert.Equalf(t, tt.want, r.GetRejectQuerySqlRegex(), "GetRejectQuerySqlRegex()")
		})
	}
}

func TestReject_SetValue(t *testing.T) {
	type args struct {
		regex []string
	}
	tests := []struct {
		name    string
		args    args
		want    []*regexp.Regexp
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "valid regex",
			args: args{regex: []string{`^SELECT \* FROM sensitive_table`, `^DELETE FROM important_data`}},
			want: []*regexp.Regexp{
				regexp.MustCompile(`^SELECT \* FROM sensitive_table`),
				regexp.MustCompile(`^DELETE FROM important_data`),
			},
			wantErr: assert.NoError,
		},
		{
			name:    "invalid regex",
			args:    args{regex: []string{`^SELECT * FROM [`, `^DELETE FROM important_data`}},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name:    "empty regex list",
			args:    args{regex: []string{}},
			want:    []*regexp.Regexp{},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Reject{}
			v := viper.New()
			v.Set("rejectQuerySqlRegex", tt.args.regex)
			tt.wantErr(t, r.SetValue(v), fmt.Sprintf("SetValue(%v)", v))
			assert.Equalf(t, tt.want, r.rejectQuerySqlRegex, "rejectQuerySqlRegex")
		})
	}
}

func Test_initRejectConfig(t *testing.T) {
	type args struct {
		v *viper.Viper
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "default initialization",
			args: args{v: viper.New()},
			want: nil,
		},
		{
			name: "custom regex initialization",
			args: args{v: func() *viper.Viper {
				v := viper.New()
				v.Set("rejectQuerySqlRegex", []string{`^DROP TABLE`, `^DELETE FROM sensitive_data`})
				return v
			}()},
			want: []string{`^DROP TABLE`, `^DELETE FROM sensitive_data`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initRejectConfig(tt.args.v)
			got := tt.args.v.Get("rejectQuerySqlRegex")
			if tt.want == nil {
				assert.Nil(t, got, "Get rejectQuerySqlRegex")
				return
			}
			assert.Equalf(t, tt.want, got, "Get rejectQuerySqlRegex")
		})
	}
}

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

package otp

import (
	"reflect"
	"testing"
)

func TestGenerateTOTPSecret(t *testing.T) {
	type args struct {
		seed []byte
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test1",
			args: args{
				seed: []byte("12345678901234567890"),
			},
			want: "VR62SA7EK3RP7MRTH7QXSIVZXXS57OY2SRUMGLKDJPREZ62OHFEQ",
		},
		{
			name: "Test2",
			args: args{
				seed: []byte("abcdefghijklmnopqrstuvwxyz"),
			},
			want: "OMYPD744HIZB2KZPAUNLEWUFBNRQBILEPWD2FGPUDYCZMFTCRFXQ",
		},
		{
			name: "Test3",
			args: args{
				seed: []byte("!@#$%^&*()_+-=[]{}|;':,.<>/?`~"),
			},
			want: "FURKOZ6REIGLQHP5OMKLZZFUQNOCRDJPCOSDP5ESH2VR3IU7NAYA",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TOTPSecretStr(GenerateTOTPSecret(tt.args.seed))
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GenerateTOTPSecret() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGenerateTOTPCode(t *testing.T) {
	type args struct {
		key     []byte
		counter uint64
		digits  int
	}
	ts := uint64(1765854733) / 30
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "Test1",
			args: args{
				key:     GenerateTOTPSecret([]byte("12345678901234567890")),
				counter: ts,
				digits:  6,
			},
			want: 383089,
		},
		{
			name: "Test2",
			args: args{
				key:     GenerateTOTPSecret([]byte("abcdefghijklmnopqrstuvwxyz")),
				counter: ts,
				digits:  6,
			},
			want: 269095,
		},
		{
			name: "Test3",
			args: args{
				key:     GenerateTOTPSecret([]byte("!@#$%^&*()_+-=[]{}|;':,.<>/?`~")),
				counter: ts,
				digits:  6,
			},
			want: 203356,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GenerateTOTPCode(tt.args.key, tt.args.counter, tt.args.digits); got != tt.want {
				t.Errorf("GenerateTOTPCode() = %v, want %v", got, tt.want)
			}
		})
	}
}

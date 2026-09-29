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

package innerjson

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestStruct struct {
	A int    `json:"a"`
	B string `json:"b"`
	C bool   `json:"c"`
}

type TestWrongStruct struct {
	C chan struct{} `json:"c"`
}

func TestMarshal(t *testing.T) {
	var s = "ts >= 123"
	x, _ := json.Marshal(s)
	fmt.Println(string(x))
	bs, err := Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, `"ts >= 123"`, string(bs))
	var ts = TestStruct{
		A: 1,
		B: "test",
		C: true,
	}
	bs, err = Marshal(ts)
	if err != nil {
		t.Fatal(err)
	}
	expected := `{"a":1,"b":"test","c":true}`
	assert.Equal(t, expected, string(bs))

	var tws = TestWrongStruct{
		C: make(chan struct{}),
	}
	_, err = Marshal(tws)
	assert.Error(t, err)
	bs, err = Marshal(ts)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expected, string(bs))
}

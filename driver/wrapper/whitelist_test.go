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

package wrapper

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
)

func TestGetWhiteList(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer TaosClose(conn)
	c := make(chan *WhitelistResult, 1)
	handler := cgo.NewHandle(c)
	TaosFetchIPWhitelistA(conn, handler)
	data := <-c
	assert.Nil(t, data.Err)
	assert.Equal(t, 2, len(data.AllowIPNets))
	assert.Equal(t, "0.0.0.0/0", data.AllowIPNets[0].String())
	assert.Equal(t, "::/0", data.AllowIPNets[1].String())
	assert.Empty(t, data.BlockIPNets)
}

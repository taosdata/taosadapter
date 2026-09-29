// Copyright (c) 2021 TAOS Data, Inc.
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

package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

func BenchmarkRoute(b *testing.B) {
	router := setupRouter()
	w := httptest.NewRecorder()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest("POST", "/rest/sql", nil)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		router.ServeHTTP(w, req)
		assert.Equal(b, 200, w.Code)
	}
}

// @author: xftan
// @date: 2021/12/14 14:59
// @description: test gin.Router
func Test_setupRouter(t *testing.T) {
	router := setupRouter()
	w := httptest.NewRecorder()
	tests := []struct {
		name string
	}{
		{
			name: "common",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest("POST", "/rest/sql", nil)
			req.RemoteAddr = testtools.GetRandomRemoteAddr()
			router.ServeHTTP(w, req)
			assert.Equal(t, 200, w.Code)
		})
	}
}

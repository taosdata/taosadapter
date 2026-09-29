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

package rest

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

func TestVgID(t *testing.T) {
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists test_vgid")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	assert.NoError(t, testtools.EnsureDBCreated("test_vgid"))

	w = httptest.NewRecorder()
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	err := encoder.Encode([]string{"d0", "d1"})
	assert.NoError(t, err)
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_vgid/vgid", buffer)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatal("get vgID fail ", w.Code, w.Body)
	}
	var result tableVgIDResp
	err = json.NewDecoder(w.Body).Decode(&result)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(result.VgIDs))
	w = httptest.NewRecorder()
	body = strings.NewReader("drop database if exists test_vgid")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

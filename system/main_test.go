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

package system

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
)

func TestStart(t *testing.T) {
	r := Init()
	config.Conf.Port = 39999
	go func() {
		Start(r, func(server *http.Server) {
			ln, err := net.Listen("tcp", server.Addr)
			if err != nil {
				logger.Fatalf("listen: %s", err)
			}
			if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
				logger.Fatalf("listen: %s", err)
			}
		})
	}()
	time.Sleep(time.Second)
	success := false
	for i := 0; i < 3; i++ {
		resp, err := http.Get("http://127.0.0.1:39999/-/ping")
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		_ = resp.Body.Close()
		success = true
		break
	}
	if !success {
		t.Fatal("failed to start server")
	}
	err := testProg.Stop(nil)
	assert.NoError(t, err)
	time.Sleep(time.Second)
	_, err = http.Get("http://127.0.0.1:39999/-/ping")
	assert.Error(t, err)
}

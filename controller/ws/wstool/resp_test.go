// Copyright (c) 2023 TAOS Data, Inc.
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

package wstool

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

func TestWSWriteJson(t *testing.T) {
	m := melody.New()
	m.Config.MaxMessageSize = 4 * 1024 * 1024
	data := &WSVersionResp{
		Code:    200,
		Message: "Success",
		Action:  "version",
		Version: "1.0.0",
	}
	m.HandleMessage(func(session *melody.Session, _ []byte) {
		logger := log.GetLogger("test").WithField("test", "TestWSWriteJson")
		session.Set("logger", logger)
		WSWriteJson(session, logger, data)
	})
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.GET("/test", func(c *gin.Context) {
		_ = m.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{})
	})
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/test", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	err = ws.WriteMessage(websocket.TextMessage, []byte{'1'})
	assert.NoError(t, err)
	wt, resp, err := ws.ReadMessage()
	assert.NoError(t, err)
	assert.NoError(t, err)
	assert.Equal(t, websocket.TextMessage, wt)
	var respS WSVersionResp
	err = json.Unmarshal(resp, &respS)
	assert.NoError(t, err)
	assert.Equal(t, 200, respS.Code)
	assert.Equal(t, "Success", respS.Message)
	assert.Equal(t, "1.0.0", respS.Version)
}

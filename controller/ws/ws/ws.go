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

package ws

import (
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

func init() {
	controller.AddController(initController())
}

type webSocketCtl struct {
	m *melody.Melody
}

func (ws *webSocketCtl) Init(ctl gin.IRouter) {
	ctl.GET("ws", func(c *gin.Context) {
		sessionID := generator.GetSessionID()
		logger := log.GetLogger("WSC").WithFields(logrus.Fields{
			config.SessionIDKey: sessionID})
		if err := ws.m.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": logger}); err != nil {
			logger.Errorf("handle request error: %v", err)
		}
	})
}

func initController() *webSocketCtl {
	m := melody.New()
	m.Config.MaxMessageSize = 0
	m.Upgrader.EnableCompression = true

	m.HandleConnect(func(session *melody.Session) {
		monitor.RecordWSWSConn()
		logger := wstool.GetLogger(session)
		logger.Debug("ws connect")
		session.Set(TaosKey, newHandler(session))
	})
	m.HandleMessage(func(session *melody.Session, data []byte) {
		h := session.MustGet(TaosKey).(*messageHandler)
		if h.IsClosed() {
			return
		}
		h.wait.Add(1)
		go func() {
			defer h.wait.Done()
			if h.IsClosed() {
				return
			}
			h.handleMessage(session, data)
		}()
	})
	m.HandleMessageBinary(func(session *melody.Session, data []byte) {
		h := session.MustGet(TaosKey).(*messageHandler)
		if h.IsClosed() {
			return
		}
		h.wait.Add(1)
		go func() {
			defer h.wait.Done()
			if h.IsClosed() {
				return
			}
			h.handleMessageBinary(session, data)
		}()
	})
	m.HandleClose(func(session *melody.Session, i int, s string) error {
		logger := wstool.GetLogger(session)
		logger.Debugf("ws close, code:%d, msg %s", i, s)
		CloseWs(session)
		return session.Close()
	})
	m.HandleError(func(session *melody.Session, err error) {
		wstool.LogWSError(session, err)
		CloseWs(session)
	})
	m.HandleDisconnect(func(session *melody.Session) {
		monitor.RecordWSWSDisconnect()
		logger := wstool.GetLogger(session)
		logger.Debug("ws disconnect")
		CloseWs(session)
	})

	return &webSocketCtl{m: m}
}

func CloseWs(session *melody.Session) {
	t, exist := session.Get(TaosKey)
	if exist && t != nil {
		t.(*messageHandler).Close()
	}
}

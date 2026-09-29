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
	"context"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

func GetDuration(ctx context.Context) int64 {
	startTime := ctx.Value(StartTimeKey).(time.Time)
	duration := time.Since(startTime).Nanoseconds()
	if duration < 0 {
		return 0
	}
	return duration
}

func GetLogger(session *melody.Session) *logrus.Entry {
	return session.MustGet("logger").(*logrus.Entry)
}

func LogWSError(session *melody.Session, err error) {
	logger := session.MustGet("logger").(*logrus.Entry)
	var wsCloseErr *websocket.CloseError
	wsCloseErr, is := err.(*websocket.CloseError)
	if is {
		if wsCloseErr.Code == websocket.CloseNormalClosure {
			logger.Debug("ws close normal")
		} else {
			logger.Debugf("ws close in error, err:%s", wsCloseErr)
		}
	} else {
		if os.IsTimeout(err) {
			logger.Debugf("ws close due to timeout, err:%s", err)
		} else {
			logger.Debugf("ws error, err:%s", err)
		}
	}
}

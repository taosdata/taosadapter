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
	"errors"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

func TestGetDuration(t *testing.T) {
	startTime := time.Now()
	ctx := context.WithValue(context.Background(), StartTimeKey, startTime)
	time.Sleep(100 * time.Millisecond)
	duration := GetDuration(ctx)
	assert.Greater(t, duration, int64(0))

	startTime = startTime.Add(5 * time.Second)
	ctx = context.WithValue(context.Background(), StartTimeKey, startTime)
	duration = GetDuration(ctx)
	assert.Equal(t, int64(0), duration)
}

func TestGetLogger(t *testing.T) {
	logger := log.GetLogger("test")
	session := &melody.Session{}
	session.Set("logger", logger.WithField("test_field", "test_value"))
	entry := GetLogger(session)
	assert.Equal(t, "test_value", entry.Data["test_field"])
}

func TestLogWSError(t *testing.T) {
	logger := log.GetLogger("test")
	session := &melody.Session{}
	session.Set("logger", logger.WithField("test_field", "test_value"))
	LogWSError(session, nil)
	LogWSError(session, &websocket.CloseError{Code: websocket.CloseNormalClosure})
	LogWSError(session, &websocket.CloseError{Code: websocket.CloseAbnormalClosure})
	LogWSError(session, errors.New("common error"))
}

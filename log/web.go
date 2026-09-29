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

package log

import (
	"net/url"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
)

func GinLog() gin.HandlerFunc {
	logger := GetLogger("WEB")

	return func(c *gin.Context) {
		startTime := time.Now()
		c.Next()
		latencyTime := time.Since(startTime)
		reqMethod := c.Request.Method
		reqUri := url.QueryEscape(c.Request.RequestURI)
		statusCode := c.Writer.Status()
		clientIP := iptool.GetRealIP(c.Request)
		reqID, exist := c.Get(config.ReqIDKey)
		if exist {
			logger.Debugf("QID:0x%x finish request, status_code:%3d, latency:%v, client_ip:%s, method:%s, uri:%s", reqID, statusCode, latencyTime, clientIP, reqMethod, reqUri)
		} else {
			logger.Debugf("finish request, status_code:%3d, latency:%v, client_ip:%s, method:%s, uri:%s", statusCode, latencyTime, clientIP, reqMethod, reqUri)
		}
		if config.Conf.Log.EnableRecordHttpSql {
			sql, exist := c.Get("sql")
			if exist {
				sqlLogger.Debugf("%d '%s' '%s' '%s' %s", reqID, reqUri, reqMethod, clientIP, sql)
			}
		}
	}
}

type recoverLog struct {
	logger logrus.FieldLogger
}

func (r *recoverLog) Write(p []byte) (n int, err error) {
	r.logger.Error(string(p))
	return len(p), nil
}

func GinRecoverLog() gin.HandlerFunc {
	logger := GetLogger("WEB")
	return func(c *gin.Context) {
		writer := &recoverLog{logger: logger}
		gin.RecoveryWithWriter(writer)(c)
	}
}

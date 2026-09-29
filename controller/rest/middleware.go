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

package rest

import (
	"fmt"
	"net/http"
	"strconv"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/db/tool"
	taoserrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
)

func prepareCtx(c *gin.Context) {
	timing := c.Query("timing")
	if timing == "true" {
		c.Set(RequireTiming, true)
	}
	c.Set(StartTimeKey, time.Now())
	var reqID int64
	var err error
	if reqIDStr := c.Query("req_id"); len(reqIDStr) != 0 {
		if reqID, err = strconv.ParseInt(reqIDStr, 10, 64); err != nil {
			logger.Errorf("illegal param, req_id must be numeric:%s, err:%s", reqIDStr, err)
			BadRequestResponseWithMsg(c, logger, 0xffff, fmt.Sprintf("illegal param, req_id must be numeric %s", err.Error()))
			return
		}
	}
	if reqID == 0 {
		reqID = generator.GetReqID()
		logger.Tracef("request:%s, client_ip:%s, req_id not set, generate new QID:0x%x", c.Request.RequestURI, c.ClientIP(), reqID)
	}
	c.Set(config.ReqIDKey, reqID)
	ctxLogger := logger.WithField(config.ReqIDKey, reqID)
	c.Set(LoggerKey, ctxLogger)
}

func checkTDengineConnection(c *gin.Context) {
	token := c.GetString(TokenKey)
	user := c.GetString(UserKey)
	password := c.GetString(PasswordKey)
	logger := c.MustGet(LoggerKey).(*logrus.Entry)
	var conn unsafe.Pointer
	var err error
	if len(token) != 0 {
		conn, err = syncinterface.TaosConnectToken("", token, "", 0, logger, log.IsDebug())
	} else {
		conn, err = syncinterface.TaosConnect("", user, password, "", 0, logger, log.IsDebug())
	}
	if err != nil {
		logger.Errorf("connect TDengine failed, err: %s", err)
		taosErr := err.(*taoserrors.TaosError)
		ErrorResponse(c, logger, http.StatusUnauthorized, int(taosErr.Code), taosErr.ErrStr)
		return
	}
	defer func() {
		syncinterface.TaosClose(conn, logger, log.IsDebug())
	}()
	allowlist, blocklist, err := tool.GetWhitelist(conn, logger, log.IsDebug())
	if err != nil {
		logger.Errorf("get whitelist failed, err: %s", err)
		taosErr := err.(*taoserrors.TaosError)
		InternalErrorResponse(c, logger, int(taosErr.Code), taosErr.ErrStr)
		return
	}
	valid := tool.CheckWhitelist(allowlist, blocklist, iptool.GetRealIP(c.Request))
	if !valid {
		logger.Errorf("whitelist prohibits current IP access, ip:%s, allowlist:%s, blocklist:%s", iptool.GetRealIP(c.Request), tool.IpNetSliceToString(allowlist), tool.IpNetSliceToString(blocklist))
		ForbiddenResponse(c, logger, commonpool.ErrWhitelistForbidden.Error())
		return
	}
}

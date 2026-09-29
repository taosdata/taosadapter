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

package generator

import (
	"sync/atomic"

	"github.com/taosdata/taosadapter/v3/config"
)

var reqIncrement int64

func GetReqID() int64 {
	id := atomic.AddInt64(&reqIncrement, 1)
	if id > 0x00ffffffffffffff {
		atomic.StoreInt64(&reqIncrement, 1)
		id = 1
	}
	reqId := int64(config.Conf.InstanceID)<<56 | id
	return reqId
}

var sessionID int64

func GetSessionID() int64 {
	return atomic.AddInt64(&sessionID, 1)
}

var uploadKeeperReqID uint32

func GetUploadKeeperReqID() int64 {
	// 0 instanceID
	// 1-2 must be 0
	// 3-6 increment
	// 7 must be 0
	id := atomic.AddUint32(&uploadKeeperReqID, 1)
	reqId := int64(config.Conf.InstanceID)<<56 | int64(id)<<8
	return reqId
}

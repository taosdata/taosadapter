// Copyright (c) 2026 TAOS Data, Inc.
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
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
)

type NotifyHandles struct {
	WhitelistChangeChan   chan int64
	WhitelistChangeHandle cgo.Handle
	DropUserChan          chan struct{}
	DropUserHandle        cgo.Handle
}

func (n *NotifyHandles) InitNotifyHandles() {
	if n.WhitelistChangeHandle == 0 {
		n.WhitelistChangeChan, n.WhitelistChangeHandle = tool.GetRegisterChangeWhiteListHandle()
	}
	if n.DropUserHandle == 0 {
		n.DropUserChan, n.DropUserHandle = tool.GetRegisterDropUserHandle()
	}
}

func (n *NotifyHandles) PutNotifyHandles() {
	if n.WhitelistChangeHandle != 0 {
		tool.PutRegisterChangeWhiteListHandle(n.WhitelistChangeHandle)
		n.WhitelistChangeHandle = 0
		n.WhitelistChangeChan = nil
	}
	if n.DropUserHandle != 0 {
		tool.PutRegisterDropUserHandle(n.DropUserHandle)
		n.DropUserHandle = 0
		n.DropUserChan = nil
	}
}

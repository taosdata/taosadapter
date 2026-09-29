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

import "testing"

func TestNotifyHandlesInitAndPut(t *testing.T) {
	var n NotifyHandles

	n.InitNotifyHandles()
	if n.WhitelistChangeHandle == 0 {
		t.Fatal("WhitelistChangeHandle should be initialized")
	}
	if n.DropUserHandle == 0 {
		t.Fatal("DropUserHandle should be initialized")
	}
	if n.WhitelistChangeChan == nil {
		t.Fatal("WhitelistChangeChan should be initialized")
	}
	if n.DropUserChan == nil {
		t.Fatal("DropUserChan should be initialized")
	}

	whitelistHandle := n.WhitelistChangeHandle
	dropUserHandle := n.DropUserHandle
	whitelistChan := n.WhitelistChangeChan
	dropUserChan := n.DropUserChan

	// Init should be idempotent when handles already exist.
	n.InitNotifyHandles()
	if n.WhitelistChangeHandle != whitelistHandle {
		t.Fatal("WhitelistChangeHandle should not change after re-init")
	}
	if n.DropUserHandle != dropUserHandle {
		t.Fatal("DropUserHandle should not change after re-init")
	}
	if n.WhitelistChangeChan != whitelistChan {
		t.Fatal("WhitelistChangeChan should not change after re-init")
	}
	if n.DropUserChan != dropUserChan {
		t.Fatal("DropUserChan should not change after re-init")
	}

	n.PutNotifyHandles()
	if n.WhitelistChangeHandle != 0 {
		t.Fatal("WhitelistChangeHandle should be reset after PutNotifyHandles")
	}
	if n.DropUserHandle != 0 {
		t.Fatal("DropUserHandle should be reset after PutNotifyHandles")
	}
	if n.WhitelistChangeChan != nil {
		t.Fatal("WhitelistChangeChan should be reset after PutNotifyHandles")
	}
	if n.DropUserChan != nil {
		t.Fatal("DropUserChan should be reset after PutNotifyHandles")
	}
}

func TestNotifyHandlesPutWithoutInit(t *testing.T) {
	var n NotifyHandles
	n.PutNotifyHandles()

	if n.WhitelistChangeHandle != 0 || n.DropUserHandle != 0 {
		t.Fatal("handles should remain zero when PutNotifyHandles is called without init")
	}
	if n.WhitelistChangeChan != nil || n.DropUserChan != nil {
		t.Fatal("channels should remain nil when PutNotifyHandles is called without init")
	}
}

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

package recordsql

import (
	"container/list"
	"sync"
)

type RecordList struct {
	list *list.List
	lock sync.Mutex
}

func NewRecordList() *RecordList {
	return &RecordList{
		list: list.New(),
	}
}

// Add adds a new item to the end of the list and returns the element.
func (rl *RecordList) Add(item interface{}) *list.Element {
	rl.lock.Lock()
	defer rl.lock.Unlock()
	return rl.list.PushBack(item)
}

// Remove removes the element from the recordList and returns the value stored in it.
// If return nil, it means the element is already removed.
func (rl *RecordList) Remove(ele *list.Element) interface{} {
	rl.lock.Lock()
	defer rl.lock.Unlock()
	if ele != nil {
		val := rl.list.Remove(ele)
		ele.Value = nil
		return val
	}
	return nil
}

// RemoveAllSqlRecords removes all elements from the recordList and returns a slice of the removed records.
// Each element in the recordList is removed and its value is set to nil,
// in this way RecordList.Remove can check if the element is removed already.
func (rl *RecordList) RemoveAllSqlRecords() []*SQLRecord {
	rl.lock.Lock()
	defer rl.lock.Unlock()
	count := rl.list.Len()
	records := make([]*SQLRecord, count)
	for i := 0; i < count; i++ {
		ele := rl.list.Front()
		records[i] = rl.list.Remove(ele).(*SQLRecord)
		if ele != nil {
			ele.Value = nil
		}
	}
	return records
}

func (rl *RecordList) RemoveAllStmtRecords() []*StmtRecord {
	rl.lock.Lock()
	defer rl.lock.Unlock()
	count := rl.list.Len()
	records := make([]*StmtRecord, count)
	for i := 0; i < count; i++ {
		ele := rl.list.Front()
		records[i] = rl.list.Remove(ele).(*StmtRecord)
		if ele != nil {
			ele.Value = nil
		}
	}
	return records
}

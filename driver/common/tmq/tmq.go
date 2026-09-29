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

package tmq

import "fmt"

type Meta struct {
	Type          string        `json:"type"`
	TableName     string        `json:"tableName"`
	TableType     string        `json:"tableType"`
	CreateList    []*CreateItem `json:"createList"`
	Columns       []*Column     `json:"columns"`
	Using         string        `json:"using"`
	TagNum        int           `json:"tagNum"`
	Tags          []*Tag        `json:"tags"`
	TableNameList []string      `json:"tableNameList"`
	AlterType     int           `json:"alterType"`
	ColName       string        `json:"colName"`
	ColNewName    string        `json:"colNewName"`
	ColType       int           `json:"colType"`
	ColLength     int           `json:"colLength"`
	ColValue      string        `json:"colValue"`
	ColValueNull  bool          `json:"colValueNull"`
	// VST inheritance (BASE ON): present for create-super and alterType 22/23.
	// BaseOn holds the parent super table names; OwnColStart/OwnTagStart mark the
	// first own (non-inherited) column/tag in the merged Columns/Tags slices.
	BaseOn      []string `json:"baseOn"`
	OwnColStart int      `json:"ownColStart"`
	OwnTagStart int      `json:"ownTagStart"`
}

type Tag struct {
	Name  string      `json:"name"`
	Type  int         `json:"type"`
	Value interface{} `json:"value"`
}

type Column struct {
	Name   string `json:"name"`
	Type   int    `json:"type"`
	Length int    `json:"length"`
}

type CreateItem struct {
	TableName string `json:"tableName"`
	Using     string `json:"using"`
	TagNum    int    `json:"tagNum"`
	Tags      []*Tag `json:"tags"`
}

type Offset int64

const OffsetInvalid = Offset(-2147467247)

func (o Offset) String() string {
	if o == OffsetInvalid {
		return "unset"
	}
	return fmt.Sprintf("%d", int64(o))
}

func (o Offset) Valid() bool {
	if o < 0 && o != OffsetInvalid {
		return false
	}
	return true
}

type TopicPartition struct {
	Topic     *string
	Partition int32
	Offset    Offset
	Metadata  *string
	Error     error
}

func (p TopicPartition) String() string {
	topic := "<null>"
	if p.Topic != nil {
		topic = *p.Topic
	}
	if p.Error != nil {
		return fmt.Sprintf("%s[%d]@%s(%s)",
			topic, p.Partition, p.Offset, p.Error)
	}
	return fmt.Sprintf("%s[%d]@%s",
		topic, p.Partition, p.Offset)
}

type Assignment struct {
	VGroupID int32 `json:"vgroup_id"`
	Offset   int64 `json:"offset"`
	Begin    int64 `json:"begin"`
	End      int64 `json:"end"`
}

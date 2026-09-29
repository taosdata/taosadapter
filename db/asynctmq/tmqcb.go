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

package asynctmq

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
*/
import "C"
import (
	"unsafe"

	"github.com/taosdata/taosadapter/v3/db/asynctmq/tmqhandle"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
)

//export AdapterTMQPollCallback
func AdapterTMQPollCallback(handle C.uintptr_t, res *C.TAOS_RES, code int, errstr *C.char) {
	h := cgo.Handle(handle)
	caller := h.Value().(*tmqhandle.TMQCaller)
	caller.PollCall(unsafe.Pointer(res), int32(code), C.GoString(errstr))
}

//export AdapterTMQFreeResultCallback
func AdapterTMQFreeResultCallback(handle C.uintptr_t) {
	h := cgo.Handle(handle)
	caller := h.Value().(*tmqhandle.TMQCaller)
	caller.FreeCall()
}

//export AdapterTMQCommitCallback
func AdapterTMQCommitCallback(handle C.uintptr_t, code int) {
	h := cgo.Handle(handle)
	caller := h.Value().(*tmqhandle.TMQCaller)
	caller.CommitCall(int32(code))
}

//export AdapterTMQFetchRawBlockCallback
func AdapterTMQFetchRawBlockCallback(handle C.uintptr_t, code int, blockSize int, block unsafe.Pointer) {
	h := cgo.Handle(handle)
	caller := h.Value().(*tmqhandle.TMQCaller)
	caller.FetchRawBlockCall(code, blockSize, block)
}

//export AdapterTMQNewConsumerCallback
func AdapterTMQNewConsumerCallback(handle C.uintptr_t, tmq unsafe.Pointer, errP *C.char) {
	h := cgo.Handle(handle)
	caller := h.Value().(*tmqhandle.TMQCaller)
	errStr := C.GoString(errP)
	C.free(unsafe.Pointer(errP))
	caller.NewConsumerCall(tmq, errStr)
}

//export AdapterTMQSubscribeCallback
func AdapterTMQSubscribeCallback(handle C.uintptr_t, errorCode int32) {
	h := cgo.Handle(handle)
	caller := h.Value().(*tmqhandle.TMQCaller)
	caller.SubscribeCall(errorCode)
}

//export AdapterTMQUnsubscribeCallback
func AdapterTMQUnsubscribeCallback(handle C.uintptr_t, errorCode int32) {
	h := cgo.Handle(handle)
	caller := h.Value().(*tmqhandle.TMQCaller)
	caller.UnsubscribeCall(errorCode)
}

//export AdapterTMQConsumerCloseCallback
func AdapterTMQConsumerCloseCallback(handle C.uintptr_t, errorCode int32) {
	h := cgo.Handle(handle)
	caller := h.Value().(*tmqhandle.TMQCaller)
	caller.ConsumerCloseCall(errorCode)
}

//export AdapterTMQGetRawCallback
func AdapterTMQGetRawCallback(handle C.uintptr_t, errorCode int32) {
	h := cgo.Handle(handle)
	caller := h.Value().(*tmqhandle.TMQCaller)
	caller.GetRawCall(errorCode)
}

//export AdapterTMQGetJsonMetaCallback
func AdapterTMQGetJsonMetaCallback(handle C.uintptr_t, meta *C.char) {
	h := cgo.Handle(handle)
	caller := h.Value().(*tmqhandle.TMQCaller)
	caller.GetJsonMetaCall(unsafe.Pointer(meta))
}

//export AdapterTMQGetTopicAssignmentCallback
func AdapterTMQGetTopicAssignmentCallback(handle C.uintptr_t, topicName unsafe.Pointer, errorCode int32, assignment unsafe.Pointer, numOfAssignment int32) {
	C.free(topicName)
	h := cgo.Handle(handle)
	caller := h.Value().(*tmqhandle.TMQCaller)
	result := make([]*tmqhandle.Assignment, numOfAssignment)
	for i := 0; i < int(numOfAssignment); i++ {
		item := *(*C.tmq_topic_assignment)(unsafe.Pointer(uintptr(assignment) + uintptr(C.sizeof_struct_tmq_topic_assignment*C.int(i))))
		result[i] = &tmqhandle.Assignment{
			VGroupID: int32(item.vgId),
			Offset:   int64(item.currentOffset),
			Begin:    int64(item.begin),
			End:      int64(item.end),
		}
	}
	caller.GetTopicAssignment(errorCode, result)
}

//export AdapterTMQOffsetSeekCallback
func AdapterTMQOffsetSeekCallback(handle C.uintptr_t, topicName unsafe.Pointer, errorCode int32) {
	C.free(topicName)
	h := cgo.Handle(handle)
	caller := h.Value().(*tmqhandle.TMQCaller)
	caller.OffsetSeekCall(errorCode)
}

//export AdapterTMQCommitOffsetCallback
func AdapterTMQCommitOffsetCallback(handle C.uintptr_t, topicName unsafe.Pointer, code int32) {
	C.free(topicName)
	cgo.Handle(handle).Value().(*tmqhandle.TMQCaller).CommitCall(code)
}

//export AdapterTMQCommittedCallback
func AdapterTMQCommittedCallback(handle C.uintptr_t, topicName unsafe.Pointer, code int64) {
	C.free(topicName)
	cgo.Handle(handle).Value().(*tmqhandle.TMQCaller).CommittedCall(code)
}

//export AdapterTMQPositionCallback
func AdapterTMQPositionCallback(handle C.uintptr_t, topicName unsafe.Pointer, code int64) {
	C.free(topicName)
	cgo.Handle(handle).Value().(*tmqhandle.TMQCaller).PositionCall(code)
}

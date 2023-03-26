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

	"github.com/taosdata/driver-go/v3/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/db/asynctmq/tmqhandle"
)

//export AdapterTMQPollCallback
func AdapterTMQPollCallback(handle C.uintptr_t, res *C.TAOS_RES) {
	h := cgo.Handle(handle)
	caller := h.Value().(*tmqhandle.TMQCaller)
	caller.PollCall(unsafe.Pointer(res))
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

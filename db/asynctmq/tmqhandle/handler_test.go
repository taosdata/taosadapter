package tmqhandle

import (
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestTMQHandlerPool(t *testing.T) {
	poolSize := 10
	handlerPool := NewHandlerPool(poolSize)
	req := poolReq{}

	// Test Get() and Put()
	for i := 0; i < poolSize; i++ {
		handler := handlerPool.Get()
		if handler == nil {
			t.Error("Get handler from empty pool error")
		} else if handler.Handler == 0 || handler.Caller == nil {
			t.Error("Get handler from pool with invalid caller error")
		}

		caller := handler.Caller
		pollRes := unsafe.Pointer(&struct{}{})
		caller.PollCall(pollRes)
		if <-caller.PollResult != pollRes {
			t.Errorf("PollCall failed")
		}

		handlerPool.Put(handler)
	}

	// Test Pending requests
	var reqChans []chan poolReq
	for i := 0; i < poolSize+1; i++ {
		reqChan := make(chan poolReq, 1)
		reqChans = append(reqChans, reqChan)
		go func() {
			handler := handlerPool.Get()
			reqChan <- poolReq{idleHandler: handler}
		}()
	}

	// Wait for pending requests to be processed
	for _, ch := range reqChans {
		req = <-ch
		if req.idleHandler == nil {
			t.Error("Pending request failed, got idle handler is nil")
		}

		handler := req.idleHandler
		caller := handler.Caller
		pollRes := unsafe.Pointer(&struct{}{})
		caller.PollCall(pollRes)
		if <-caller.PollResult != pollRes {
			t.Errorf("PollCall failed")
		}

		handlerPool.Put(handler)
	}
	handlerPool = NewHandlerPool(1)
	handle := handlerPool.Get()
	go func() {
		time.Sleep(time.Second)
		handlerPool.Put(handle)
	}()
	handle2 := handlerPool.Get()
	assert.NotNil(t, handle2)
}

func TestTMQCaller(t *testing.T) {
	caller := NewTMQCaller()

	// Test PollCall
	res := unsafe.Pointer(uintptr(0x12345))
	caller.PollCall(res)
	if <-caller.PollResult != res {
		t.Error("PollCall failed")
	}

	// Test FreeCall
	go func() { caller.FreeCall() }()
	<-caller.FreeResult

	// Test CommitCall
	code := int32(123)
	caller.CommitCall(code)
	if <-caller.CommitResult != code {
		t.Error("CommitCall failed")
	}

	// Test FetchRawBlockCall
	frbr := &FetchRawBlockResult{
		Code:      456,
		BlockSize: 789,
		Block:     unsafe.Pointer(uintptr(0x67890)),
	}
	caller.FetchRawBlockCall(frbr.Code, frbr.BlockSize, frbr.Block)
	result := <-caller.FetchRawBlockResult
	if result.Code != frbr.Code || result.BlockSize != frbr.BlockSize || result.Block != frbr.Block {
		t.Error("FetchRawBlockCall failed")
	}

	// Test NewConsumerCall
	consumer := unsafe.Pointer(uintptr(0xabcd))
	errStr := "some error"
	caller.NewConsumerCall(consumer, errStr)
	result2 := <-caller.NewConsumerResult
	if result2.Consumer != consumer || result2.ErrStr != errStr {
		t.Error("NewConsumerCall failed")
	}

	// Test SubscribeCall
	code2 := int32(456)
	caller.SubscribeCall(code2)
	if <-caller.SubscribeResult != code2 {
		t.Error("SubscribeCall failed")
	}

	// Test UnsubscribeCall
	code3 := int32(789)
	caller.UnsubscribeCall(code3)
	if <-caller.UnsubscribeResult != code3 {
		t.Error("UnsubscribeCall failed")
	}

	// Test ConsumerCloseCall
	code4 := int32(101112)
	caller.ConsumerCloseCall(code4)
	if <-caller.ConsumerCloseResult != code4 {
		t.Error("ConsumerCloseCall failed")
	}

	// Test GetRawCall
	code5 := int32(131415)
	caller.GetRawCall(code5)
	if <-caller.GetRawResult != code5 {
		t.Error("GetRawCall failed")
	}

	// Test GetJsonMetaCall
	meta := unsafe.Pointer(uintptr(0xeffe))
	caller.GetJsonMetaCall(meta)
	if <-caller.GetJsonMetaResult != meta {
		t.Error("GetJsonMetaCall failed")
	}
}

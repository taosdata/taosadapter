package connectpool

import (
	"context"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestConnectPool(t *testing.T) {
	var factoryCalled, closeCalled int

	pool, err := NewConnectPool(&Config{
		InitialCap: 2,
		MaxCap:     5,
		Factory: func() (unsafe.Pointer, error) {
			factoryCalled++
			return unsafe.Pointer(&struct{}{}), nil
		},
		Close: func(pointer unsafe.Pointer) error {
			closeCalled++
			return nil
		},
	})

	assert.NoError(t, err)
	assert.NotNil(t, pool)

	conn1, err := pool.Get()
	assert.NoError(t, err)
	assert.NotNil(t, conn1)

	conn2, err := pool.Get()
	assert.NoError(t, err)
	assert.NotNil(t, conn2)

	conn3, err := pool.Get()
	assert.NoError(t, err)
	assert.NotNil(t, conn3)

	pool.Put(conn1)
	pool.Put(conn2)
	pool.Put(conn3)

	arr := make([]unsafe.Pointer, 5)
	for i := 0; i < 5; i++ {
		conn, err := pool.Get()
		assert.NoError(t, err)
		assert.NotNil(t, conn)
		arr[i] = conn
	}
	for i := 0; i < 5; i++ {
		err = pool.Put(arr[i])
		assert.NoError(t, err)
	}
	arr = nil
	assert.Equal(t, 5, factoryCalled)

	pool.Release()

	assert.Equal(t, 5, closeCalled)
}

func TestRelease(t *testing.T) {
	value := []int{1, 2}
	deleteValue := []int{2, 1}
	deleteIndex := 0
	index := 0
	pool, err := NewConnectPool(&Config{
		InitialCap: 1,
		MaxCap:     2,
		Factory: func() (unsafe.Pointer, error) {
			p := unsafe.Pointer(&value[index])
			index += 1
			return p, nil
		},
		Close: func(pointer unsafe.Pointer) error {
			assert.Equal(t, deleteValue[deleteIndex], *(*int)(pointer))
			deleteIndex += 1
			t.Log("close", *(*int)(pointer))
			return nil
		},
	})
	assert.NoError(t, err)

	conn1, err := pool.Get()
	assert.NoError(t, err)
	assert.NotNil(t, conn1)
	t.Log("conn1", *(*int)(conn1))

	conn2, err := pool.Get()
	assert.NoError(t, err)
	assert.NotNil(t, conn2)
	t.Log("conn2", *(*int)(conn2))
	recv := make(chan unsafe.Pointer)
	go func() {
		conn3, err := pool.Get()
		assert.NoError(t, err)
		assert.NotNil(t, conn3)
		recv <- conn3
	}()
	time.Sleep(time.Second)
	pool.Release()
	pool.Put(conn1)
	pool.Put(conn2)
	assert.Equal(t, 1, pool.openingConns)
	timeout, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	select {
	case <-timeout.Done():
		t.Error("wait for conn3 timeout")
	case conn3 := <-recv:
		assert.Equal(t, conn1, conn3)
		t.Log("conn3", *(*int)(conn3))
		pool.Put(conn3)
	}
	assert.Equal(t, 2, deleteIndex)
	assert.Equal(t, 0, pool.openingConns)
}

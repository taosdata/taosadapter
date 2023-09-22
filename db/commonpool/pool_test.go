package commonpool

import (
	"net"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/config"
)

func TestMain(m *testing.M) {
	config.Init()
	m.Run()
}

func BenchmarkGetConnection(b *testing.B) {
	for i := 0; i < b.N; i++ {
		conn, err := GetConnection("root", "taosdata", net.ParseIP("127.0.0.1"))
		if err != nil {
			b.Error(err)
			return
		}
		conn.Put()
	}
}

// @author: xftan
// @date: 2021/12/14 15:03
// @description: test connection get connection
func TestGetConnection(t *testing.T) {
	type args struct {
		user     string
		password string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test",
			args: args{
				user:     "root",
				password: "taosdata",
			},
			wantErr: false,
		}, {
			name: "wrong",
			args: args{
				user:     "root",
				password: "wrong",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetConnection(tt.args.user, tt.args.password, net.ParseIP("127.0.0.1"))
			if (err != nil) != tt.wantErr {
				t.Errorf("GetConnection() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			err = got.Put()
			assert.NoError(t, err)
		})
	}
}

// @author: xftan
// @date: 2021/12/14 15:04
// @description: test connection pool re-get connection
func TestReGetConnection(t *testing.T) {
	conn, err := GetConnection("root", "taosdata", net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)
	conn2, err := GetConnection("root", "wrong", net.ParseIP("127.0.0.1"))
	assert.Error(t, err)
	assert.Nil(t, conn2)
	err = conn.Put()
	assert.NoError(t, err)
}

// @author: xftan
// @date: 2021/12/14 15:05
// @description: test connect pool close
func TestConnectorPool_Close(t *testing.T) {
	pool, err := NewConnectorPool("root", "taosdata")
	assert.NoError(t, err)
	conn, err := pool.Get()
	assert.NoError(t, err)
	err = pool.Put(conn)
	assert.NoError(t, err)
	conn2, err := pool.Get()
	assert.NoError(t, err)
	err = pool.Close(conn2)
	assert.NoError(t, err)
	pool.Release()
}

func TestChangePassword(t *testing.T) {
	c, err := GetConnection("root", "taosdata", net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)

	result := wrapper.TaosQuery(c.TaosConnection, "drop user test")
	assert.NotNil(t, result)
	wrapper.TaosFreeResult(result)

	result = wrapper.TaosQuery(c.TaosConnection, "create user test pass 'test'")
	assert.NotNil(t, result)
	errNo := wrapper.TaosError(result)
	assert.Equal(t, 0, errNo)
	wrapper.TaosFreeResult(result)

	defer func() {
		r := wrapper.TaosQuery(c.TaosConnection, "drop user test")
		wrapper.TaosFreeResult(r)
	}()

	conn, err := GetConnection("test", "test", net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)

	result = wrapper.TaosQuery(c.TaosConnection, "alter user test pass 'test2'")
	assert.NotNil(t, result)
	errNo = wrapper.TaosError(result)
	assert.Equal(t, 0, errNo)
	wrapper.TaosFreeResult(result)

	result = wrapper.TaosQuery(conn.TaosConnection, "show databases")
	errNo = wrapper.TaosError(result)
	wrapper.TaosFreeResult(result)
	assert.Equal(t, 0, errNo)
	wc1, err := conn.pool.Get()
	assert.Equal(t, AuthFailureError, err)
	assert.Equal(t, unsafe.Pointer(nil), wc1)
	time.Sleep(time.Second * 2)
	wc, err := conn.pool.Get()
	assert.Equal(t, AuthFailureError, err)
	assert.Equal(t, unsafe.Pointer(nil), wc)
	conn.Put()

	conn2, err := GetConnection("test", "test", net.ParseIP("127.0.0.1"))
	assert.Error(t, err)
	assert.Nil(t, conn2)

	conn3, err := GetConnection("test", "test2", net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)
	assert.NotNil(t, conn3)
	result2 := wrapper.TaosQuery(conn3.TaosConnection, "show databases")
	errNo = wrapper.TaosError(result2)
	wrapper.TaosFreeResult(result2)
	assert.Equal(t, 0, errNo)
	conn3.Put()

	conn4, err := GetConnection("test", "test2", net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)
	assert.NotNil(t, conn4)
	result3 := wrapper.TaosQuery(conn4.TaosConnection, "show databases")
	errNo = wrapper.TaosError(result3)
	wrapper.TaosFreeResult(result3)
	assert.Equal(t, 0, errNo)
	conn3.Put()
}

func TestChangePasswordConcurrent(t *testing.T) {
	c, err := GetConnection("root", "taosdata", net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)

	result := wrapper.TaosQuery(c.TaosConnection, "drop user test")
	assert.NotNil(t, result)
	wrapper.TaosFreeResult(result)

	result = wrapper.TaosQuery(c.TaosConnection, "create user test pass 'test'")
	assert.NotNil(t, result)
	errNo := wrapper.TaosError(result)
	assert.Equal(t, 0, errNo)
	wrapper.TaosFreeResult(result)

	defer func() {
		r := wrapper.TaosQuery(c.TaosConnection, "drop user test")
		wrapper.TaosFreeResult(r)
	}()
	conn, err := GetConnection("test", "test", net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)

	result = wrapper.TaosQuery(c.TaosConnection, "alter user test pass 'test2'")
	assert.NotNil(t, result)
	errNo = wrapper.TaosError(result)
	assert.Equal(t, 0, errNo)
	wrapper.TaosFreeResult(result)

	result = wrapper.TaosQuery(conn.TaosConnection, "show databases")
	errNo = wrapper.TaosError(result)
	wrapper.TaosFreeResult(result)
	assert.Equal(t, 0, errNo)
	wc1, err := conn.pool.Get()
	assert.Equal(t, AuthFailureError, err)
	assert.Equal(t, unsafe.Pointer(nil), wc1)
	conn.Put()
	wg := sync.WaitGroup{}
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			conn2, err := GetConnection("test", "test2", net.ParseIP("127.0.0.1"))
			assert.NoError(t, err)
			assert.NotNil(t, conn2)
			conn2.Put()
		}()
	}
	wg.Wait()
	conn2, err := GetConnection("test", "test", net.ParseIP("127.0.0.1"))
	assert.Error(t, err)
	assert.Nil(t, conn2)

	conn3, err := GetConnection("test", "test2", net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)
	assert.NotNil(t, conn3)
	result2 := wrapper.TaosQuery(conn3.TaosConnection, "show databases")
	errNo = wrapper.TaosError(result2)
	wrapper.TaosFreeResult(result2)
	assert.Equal(t, 0, errNo)
	conn3.Put()

	conn4, err := GetConnection("test", "test2", net.ParseIP("127.0.0.1"))
	assert.NoError(t, err)
	assert.NotNil(t, conn4)
	result3 := wrapper.TaosQuery(conn4.TaosConnection, "show databases")
	errNo = wrapper.TaosError(result3)
	wrapper.TaosFreeResult(result3)
	assert.Equal(t, 0, errNo)
	conn3.Put()
}

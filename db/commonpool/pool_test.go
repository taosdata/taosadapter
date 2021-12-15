package commonpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/config"
)

func TestMain(m *testing.M) {
	config.Init()
	m.Run()
}

func BenchmarkGetConnection(b *testing.B) {
	for i := 0; i < b.N; i++ {
		conn, err := GetConnection("root", "taosdata")
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
			got, err := GetConnection(tt.args.user, tt.args.password)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetConnection() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			code := wrapper.TaosSelectDB(got.TaosConnection, "log")
			assert.Equal(t, 0, code)
			err = got.Put()
			assert.NoError(t, err)
		})
	}
}

// @author: xftan
// @date: 2021/12/14 15:04
// @description: test connection pool re-get connection
func TestReGetConnection(t *testing.T) {
	conn, err := GetConnection("root", "taosdata")
	assert.NoError(t, err)
	conn2, err := GetConnection("root", "wrong")
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

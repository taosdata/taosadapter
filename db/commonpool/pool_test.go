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

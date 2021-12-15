package tool

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/db"
)

// @author: xftan
// @date: 2021/12/14 15:05
// @description: test creat database with connection
func TestCreateDBWithConnection(t *testing.T) {
	config.Init()
	db.PrepareConnection()
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	type args struct {
		connection unsafe.Pointer
		db         string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test",
			args: args{
				connection: conn,
				db:         "test_auto_create_db",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := CreateDBWithConnection(tt.args.connection, tt.args.db); (err != nil) != tt.wantErr {
				t.Errorf("CreateDBWithConnection() error = %v, wantErr %v", err, tt.wantErr)
			}
			code := wrapper.TaosSelectDB(tt.args.connection, tt.args.db)
			assert.Equal(t, 0, code)
		})
	}
}

package capi

import (
	"testing"
	"unsafe"

	tErrors "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/db"
)

func Test_selectDB(t *testing.T) {
	config.Init()
	db.PrepareConnection()
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer wrapper.TaosClose(conn)
	type args struct {
		taosConnect unsafe.Pointer
		db          string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "auto create database",
			args: args{
				taosConnect: conn,
				db:          "auto_create_db_test",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := wrapper.TaosQuery(tt.args.taosConnect, "drop database if exists "+tt.args.db)
			code := wrapper.TaosError(result)
			if code != 0 {
				errStr := wrapper.TaosErrorStr(result)
				wrapper.TaosFreeResult(result)
				t.Error(tErrors.NewError(code, errStr))
				return
			}
			wrapper.TaosFreeResult(result)
			if err := selectDB(tt.args.taosConnect, tt.args.db); (err != nil) != tt.wantErr {
				t.Errorf("selectDB() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

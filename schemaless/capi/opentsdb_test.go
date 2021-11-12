package capi

import (
	"testing"
	"unsafe"

	"github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
)

func TestInsertOpentsdbTelnet(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer wrapper.TaosClose(conn)
	result := wrapper.TaosQuery(conn, "create database if not exists test1")
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		t.Error(&errors.TaosError{
			Code:   int32(code) & 0xffff,
			ErrStr: errStr,
		})
		return
	}
	wrapper.TaosFreeResult(result)
	type args struct {
		taosConnect unsafe.Pointer
		data        string
		db          string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test",
			args: args{
				taosConnect: conn,
				data:        "df.data.df_complex.used 1636539620 21393473536 fqdn=vm130  status=production",
				db:          "test1",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := InsertOpentsdbTelnet(tt.args.taosConnect, tt.args.data, tt.args.db); (err != nil) != tt.wantErr {
				t.Errorf("InsertOpentsdbTelnet() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

package capi

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/db"
	"github.com/taosdata/taosadapter/schemaless/proto"
)

func TestInsertInfluxdb(t *testing.T) {
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
		data        []byte
		db          string
		precision   string
	}
	tests := []struct {
		name    string
		args    args
		want    *proto.InfluxResult
		wantErr bool
	}{
		{
			name: "test",
			args: args{
				taosConnect: conn,
				data:        []byte("measurement,host=host1 field1=2i,field2=2.0 1577836800000000000"),
				db:          "test",
			},
			want: &proto.InfluxResult{
				SuccessCount: 1,
				FailCount:    0,
				ErrorList:    make([]string, 1),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := InsertInfluxdb(tt.args.taosConnect, tt.args.data, tt.args.db, tt.args.precision)
			if (err != nil) != tt.wantErr {
				t.Errorf("InsertInfluxdb() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("InsertInfluxdb() got = %v, want %v", got, tt.want)
			}
		})
	}
}

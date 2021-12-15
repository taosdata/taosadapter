package capi_test

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/schemaless/capi"
	"github.com/taosdata/taosadapter/schemaless/proto"
)

// @author: xftan
// @date: 2021/12/14 15:11
// @description: test insert influxdb
func TestInsertInfluxdb(t *testing.T) {
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
				data:        []byte("measurement,host=host1 field1=2i,field2=2.0,fieldKey=\"Launch 🚀\" 1577836800000000001"),
				db:          "test_capi",
			},
			want: &proto.InfluxResult{
				SuccessCount: 1,
				FailCount:    0,
				ErrorList:    make([]string, 1),
			},
			wantErr: false,
		}, {
			name: "wrong",
			args: args{
				taosConnect: conn,
				data:        []byte("wrong,host=host1 field1=wrong 1577836800000000001"),
				db:          "test_capi",
			},
			want:    nil,
			wantErr: true,
		}, {
			name: "wrongdb",
			args: args{
				taosConnect: conn,
				data:        []byte("wrong,host=host1 field1=wrong 1577836800000000001"),
				db:          "1'test_capi",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := capi.InsertInfluxdb(tt.args.taosConnect, tt.args.data, tt.args.db, tt.args.precision)
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

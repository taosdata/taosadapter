package influxdb

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/taosdata/driver-go/v2/wrapper"
)

func TestInsertInfluxdb(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	type args struct {
		conn      unsafe.Pointer
		data      []byte
		db        string
		precision string
	}
	tests := []struct {
		name    string
		args    args
		want    *Result
		wantErr bool
	}{
		{
			name: "normal",
			args: args{
				conn:      conn,
				data:      []byte("measurement,host=host1 field1=2i,field2=2.0 1577836800000000000"),
				db:        "test",
				precision: "",
			},
			want: &Result{
				SuccessCount: 1,
				FailCount:    0,
				ErrorList:    make([]string, 1),
			},
			wantErr: false,
		},
		{
			name: "millisecond",
			args: args{
				conn:      conn,
				data:      []byte("measurement,host=host1 field1=2i,field2=2.0 1577836900000"),
				db:        "test",
				precision: "ms",
			},
			want: &Result{
				SuccessCount: 1,
				FailCount:    0,
				ErrorList:    make([]string, 1),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := InsertInfluxdb(tt.args.conn, tt.args.data, tt.args.db, tt.args.precision)
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

package opentsdb

import (
	"testing"
	"unsafe"

	"github.com/taosdata/driver-go/v2/wrapper"
)

func TestInsertTelnet(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	type args struct {
		conn unsafe.Pointer
		data string
		db   string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "normal",
			args: args{
				conn: conn,
				data: "put sys.if.bytes.out 1479496100 1.3E3 host=web01 interface=eth0",
				db:   "test",
			},
			wantErr: false,
		},
		{
			name: "wrong time",
			args: args{
				conn: conn,
				data: "put metric.foo notatime 42 host=web01",
				db:   "test",
			},
			wantErr: true,
		},
		{
			name: "no data",
			args: args{
				conn: conn,
				data: "put ",
				db:   "test",
			},
			wantErr: true,
		},
		{
			name: "reserved",
			args: args{
				conn: conn,
				data: "put key.create 1479496100 42 create=reserved",
				db:   "test",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := InsertTelnet(tt.args.conn, tt.args.data, tt.args.db); (err != nil) != tt.wantErr {
				t.Errorf("InsertTelnet() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

package opentsdb

import (
	"testing"
	"unsafe"

	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/db"
)

func TestMain(m *testing.M) {
	config.Init()
	db.PrepareConnection()
	m.Run()
}

// @author: xftan
// @date: 2021/12/14 15:13
// @description: test insert opentsdb telnet
func TestInsertTelnet(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer wrapper.TaosClose(conn)
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
				db:   "test_goapi",
			},
			wantErr: false,
		},
		{
			name: "wrong time",
			args: args{
				conn: conn,
				data: "put metric.foo notatime 42 host=web01",
				db:   "test_goapi",
			},
			wantErr: true,
		},
		{
			name: "no data",
			args: args{
				conn: conn,
				data: "put ",
				db:   "test_goapi",
			},
			wantErr: true,
		},
		{
			name: "reserved",
			args: args{
				conn: conn,
				data: "put key.create 1479496100 42 create=reserved",
				db:   "test_goapi",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := InsertOpentsdbTelnet(tt.args.conn, tt.args.data, tt.args.db); (err != nil) != tt.wantErr {
				t.Errorf("InsertTelnet() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func BenchmarkTelnet(b *testing.B) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		b.Error(err)
		return
	}
	defer wrapper.TaosClose(conn)
	for i := 0; i < b.N; i++ {
		err := InsertOpentsdbTelnet(conn, "put sys.if.bytes.out 1479496100 1.3E3 host=web01 interface=eth0", "test")
		if err != nil {
			b.Error(err)
		}
	}
}

package capi

import (
	"testing"
	"unsafe"

	"github.com/taosdata/driver-go/v2/wrapper"
)

func TestInsertOpentsdbTelnet(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer wrapper.TaosClose(conn)
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

func BenchmarkTelnet(b *testing.B) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		b.Error(err)
		return
	}
	defer wrapper.TaosClose(conn)
	for i := 0; i < b.N; i++ {
		//`sys.if.bytes.out`,`host`=web01,`interface`=eth0
		//t_98df8453856519710bfc2f1b5f8202cf
		//t_98df8453856519710bfc2f1b5f8202cf
		err := InsertOpentsdbTelnet(conn, `put sys.if.bytes.out 1479496100 1.3E3 host=web01 interface=eth0`, "test")
		if err != nil {
			b.Error(err)
		}
	}
}

func TestInsertOpentsdbJson(t *testing.T) {
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
				data: []byte(`
{
    "metric": "sys.cpu.nice",
    "timestamp": 1346846400,
    "value": 18,
    "tags": {
       "host": "web01",
       "dc": "lga"
    }
}`),
				db: "test",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := InsertOpentsdbJson(tt.args.taosConnect, tt.args.data, tt.args.db); (err != nil) != tt.wantErr {
				t.Errorf("InsertOpentsdbJson() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

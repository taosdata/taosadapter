package capi_test

import (
	"testing"
	"unsafe"

	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/db"
	"github.com/taosdata/taosadapter/schemaless/capi"
)

func TestMain(m *testing.M) {
	config.Init()
	db.PrepareConnection()
	m.Run()
}

// @author: xftan
// @date: 2021/12/14 15:11
// @description: test insert opentsdb telnet
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
				db:          "test_capi",
			},
			wantErr: false,
		}, {
			name: "wrong",
			args: args{
				taosConnect: conn,
				data:        "df.data.df_complex.used 163653962000 21393473536 fqdn=vm130  status=production",
				db:          "test_capi",
			},
			wantErr: true,
		}, {
			name: "wrongdb",
			args: args{
				taosConnect: conn,
				data:        "df.data.df_complex.used 1636539620 21393473536 fqdn=vm130  status=production",
				db:          "1'test_capi",
			},
			wantErr: true,
		}, {
			name: "nodata",
			args: args{
				taosConnect: conn,
				data:        "",
				db:          "test_capi",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := capi.InsertOpentsdbTelnet(tt.args.taosConnect, tt.args.data, tt.args.db); (err != nil) != tt.wantErr {
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
		err := capi.InsertOpentsdbTelnet(conn, `put sys.if.bytes.out 1479496100 1.3E3 host=web01 interface=eth0`, "test")
		if err != nil {
			b.Error(err)
		}
	}
}

// @author: xftan
// @date: 2021/12/14 15:12
// @description: test insert opentsdb json
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
				db: "test_capi",
			},
			wantErr: false,
		}, {
			name: "wrong",
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
    },
}`),
				db: "test_capi",
			},
			wantErr: true,
		}, {
			name: "wrongdb",
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
    },
}`),
				db: "1'test_capi",
			},
			wantErr: true,
		}, {
			name: "nodata",
			args: args{
				taosConnect: conn,
				data:        nil,
				db:          "test_capi",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := capi.InsertOpentsdbJson(tt.args.taosConnect, tt.args.data, tt.args.db); (err != nil) != tt.wantErr {
				t.Errorf("InsertOpentsdbJson() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

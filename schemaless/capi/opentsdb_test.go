package capi_test

import (
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/schemaless/capi"
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
	defer func() {
		r := wrapper.TaosQuery(conn, "drop database if exists test_capi")
		code := wrapper.TaosError(r)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(r)
			t.Error(errors.NewError(code, errStr))
		}
		wrapper.TaosFreeResult(r)
	}()
	type args struct {
		taosConnect unsafe.Pointer
		data        string
		db          string
		ttl         int
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
				ttl:         100,
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
				ttl:         1000,
			},
			wantErr: true,
		}, {
			name: "nodata",
			args: args{
				taosConnect: conn,
				data:        "",
				db:          "test_capi",
				ttl:         1000,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := capi.InsertOpentsdbTelnet(tt.args.taosConnect, []string{tt.args.data}, tt.args.db, tt.args.ttl, 0); (err != nil) != tt.wantErr {
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
	defer func() {
		r := wrapper.TaosQuery(conn, "drop database if exists test")
		code := wrapper.TaosError(r)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(r)
			b.Error(errors.NewError(code, errStr))
		}
		wrapper.TaosFreeResult(r)
	}()
	for i := 0; i < b.N; i++ {
		//`sys.if.bytes.out`,`host`=web01,`interface`=eth0
		//t_98df8453856519710bfc2f1b5f8202cf
		//t_98df8453856519710bfc2f1b5f8202cf
		err := capi.InsertOpentsdbTelnet(conn, []string{`put sys.if.bytes.out 1479496100 1.3E3 host=web01 interface=eth0`}, "test", 0, 0)
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
	now := time.Now().Unix()
	defer wrapper.TaosClose(conn)
	defer func() {
		r := wrapper.TaosQuery(conn, "drop database if exists test_capi")
		code := wrapper.TaosError(r)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(r)
			t.Error(errors.NewError(code, errStr))
		}
		wrapper.TaosFreeResult(r)
	}()
	type args struct {
		taosConnect unsafe.Pointer
		data        []byte
		db          string
		ttl         int
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
				data: []byte(fmt.Sprintf(`
{
    "metric": "sys.cpu.nice",
    "timestamp": %d,
    "value": 18,
    "tags": {
       "host": "web01",
       "dc": "lga"
    }
}`, now)),
				db:  "test_capi",
				ttl: 100,
			},
			wantErr: false,
		}, {
			name: "wrong",
			args: args{
				taosConnect: conn,
				data: []byte(fmt.Sprintf(`
{
    "metric": "sys.cpu.nice",
    "timestamp": %d,
    "value": 18,
    "tags": {
       "host": "web01",
       "dc": "lga"
    }
}`, now)),
				db:  "test_capi",
				ttl: 0,
			},
			wantErr: false,
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
    }
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
				ttl:         1000,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := capi.InsertOpentsdbJson(tt.args.taosConnect, tt.args.data, tt.args.db, tt.args.ttl, 0); (err != nil) != tt.wantErr {
				t.Errorf("InsertOpentsdbJson() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestInsertOpentsdbTelnetBatch(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer wrapper.TaosClose(conn)
	defer func() {
		r := wrapper.TaosQuery(conn, "drop database if exists test_capi")
		code := wrapper.TaosError(r)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(r)
			t.Error(errors.NewError(code, errStr))
		}
		wrapper.TaosFreeResult(r)
	}()
	type args struct {
		taosConnect unsafe.Pointer
		data        []string
		db          string
		ttl         int
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
				data: []string{
					"df.data.df_complex.used 1636539620 21393473536 fqdn=vm130  status=production",
					"df.data.df_complex.used 1636539621 21393473536 fqdn=vm129  status=production",
				},
				db:  "test_capi",
				ttl: 100,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := capi.InsertOpentsdbTelnet(tt.args.taosConnect, tt.args.data, tt.args.db, tt.args.ttl, 0); (err != nil) != tt.wantErr {
				t.Errorf("InsertOpentsdbTelnet() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

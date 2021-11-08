package opentsdb

import (
	"testing"
	"unsafe"

	"github.com/taosdata/driver-go/v2/wrapper"
)

func TestInsertJson(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	type args struct {
		conn unsafe.Pointer
		data []byte
		db   string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test single",
			args: args{
				conn: conn,
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
		{
			name: "test array",
			args: args{
				conn: conn,
				data: []byte(`
[
    {
        "metric": "sys.cpu.nice",
        "timestamp": 1346846500,
        "value": 18,
        "tags": {
           "host": "web01",
           "dc": "lga"
        }
    },
    {
        "metric": "sys.cpu.nice",
        "timestamp": 1346846500,
        "value": 9,
        "tags": {
           "host": "web02",
           "dc": "lga"
        }
    }
]
`),
				db: "test",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := InsertJson(tt.args.conn, tt.args.data, tt.args.db); (err != nil) != tt.wantErr {
				t.Errorf("InsertJson() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

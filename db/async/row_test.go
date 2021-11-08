package async

import (
	"database/sql/driver"
	"testing"
	"unsafe"

	"github.com/taosdata/driver-go/v2/wrapper"
)

func TestAsync_TaosExec(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	type fields struct {
		handlerPool *HandlerPool
	}
	type args struct {
		taosConnect unsafe.Pointer
		sql         string
		timeFormat  wrapper.FormatTimeFunc
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *ExecResult
		wantErr bool
	}{
		{
			name:   "show databases",
			fields: fields{NewHandlerPool(10000)},
			args: args{
				taosConnect: conn,
				sql:         "show databases",
				timeFormat: func(ts int64, precision int) driver.Value {
					return ts
				},
			},
			want: &ExecResult{
				FieldCount: 19,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Async{
				handlerPool: tt.fields.handlerPool,
			}
			got, err := a.TaosExec(tt.args.taosConnect, tt.args.sql, tt.args.timeFormat)
			if (err != nil) != tt.wantErr {
				t.Errorf("TaosExec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.FieldCount != tt.want.FieldCount {
				t.Errorf("field count execpt %d got %d", tt.want.FieldCount, got.FieldCount)
				return
			}
			t.Logf("%#v", got)
		})
	}
}

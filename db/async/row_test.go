package async

import (
	"database/sql/driver"
	"testing"
	"unsafe"

	"github.com/taosdata/driver-go/v2/wrapper"
)

// @author: xftan
// @date: 2021/12/14 15:02
// @description: test Async Execute
func TestAsync_TaosExec(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer wrapper.TaosClose(conn)
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
		}, {
			name:   "create database",
			fields: fields{NewHandlerPool(10000)},
			args: args{
				taosConnect: conn,
				sql:         "create database if not exists test_async_exec",
			},
			want: &ExecResult{
				AffectedRows: 0,
				FieldCount:   0,
				Header:       nil,
				Data:         nil,
			},
			wantErr: false,
		}, {
			name:   "wrong",
			fields: fields{NewHandlerPool(10000)},
			args: args{
				taosConnect: conn,
				sql:         "wrong sql",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Async{
				HandlerPool: tt.fields.handlerPool,
			}
			got, err := a.TaosExec(tt.args.taosConnect, tt.args.sql, tt.args.timeFormat)
			if (err != nil) != tt.wantErr {
				t.Errorf("TaosExec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil {
				if tt.want != nil {
					t.Errorf("TaosExec() expect  %v, get nil", tt.want)
				}
				return
			}
			if got.FieldCount != tt.want.FieldCount {
				t.Errorf("field count expect %d got %d", tt.want.FieldCount, got.FieldCount)
				return
			}
			t.Logf("%#v", got)
		})
	}
}

// @author: xftan
// @date: 2021/12/14 15:03
// @description: test async exec without result
func TestAsync_TaosExecWithoutResult(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer wrapper.TaosClose(conn)
	type fields struct {
		handlerPool *HandlerPool
	}
	type args struct {
		taosConnect unsafe.Pointer
		sql         string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:   "create database",
			fields: fields{NewHandlerPool(10000)},
			args: args{
				taosConnect: conn,
				sql:         "create database if not exists test_async_exec_without_result",
			},
			wantErr: false,
		}, {
			name: "wrong",
			fields: fields{
				handlerPool: NewHandlerPool(2),
			},
			args: args{
				taosConnect: conn,
				sql:         "wrong sql",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Async{
				HandlerPool: tt.fields.handlerPool,
			}
			if err := a.TaosExecWithoutResult(tt.args.taosConnect, tt.args.sql); (err != nil) != tt.wantErr {
				t.Errorf("TaosExecWithoutResult() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

package testtools

import (
	"database/sql/driver"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
)

func TestExec(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	require.NoError(t, err)
	defer func() {
		wrapper.TaosClose(conn)
	}()
	type args struct {
		sql string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "create db",
			args: args{
				sql: "CREATE DATABASE IF NOT EXISTS test_exec",
			},
			wantErr: false,
		},
		{
			name: "wrong sql",
			args: args{
				sql: "CREAT DATABASE IF NOT EXISTS test_exec",
			},
			wantErr: true,
		},
		{
			name: "drop db",
			args: args{
				sql: "DROP DATABASE IF EXISTS test_exec",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Exec(conn, tt.args.sql); (err != nil) != tt.wantErr {
				t.Errorf("TestExec() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestQuery(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	require.NoError(t, err)
	defer func() {
		wrapper.TaosClose(conn)
	}()
	type args struct {
		sql string
	}
	tests := []struct {
		name    string
		args    args
		want    [][]driver.Value
		wantErr bool
	}{
		{
			name: "select 1",
			args: args{
				sql: "SELECT 1",
			},
			want:    [][]driver.Value{{int64(1)}},
			wantErr: false,
		},
		{
			name: "wrong sql",
			args: args{
				sql: "SELEC 1",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Query(conn, tt.args.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Query() got = %v, want %v", got, tt.want)
			}
		})
	}
}

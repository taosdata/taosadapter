// Copyright (c) 2025 TAOS Data, Inc.
//
// SPDX-License-Identifier: MIT
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package testtools

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/tools/testtools/testenv"
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

func TestEnsureDBCreated(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	require.NoError(t, err)
	defer func() {
		wrapper.TaosClose(conn)
	}()
	dbName := "test_ensure_db_created"
	err = Exec(conn, "create database if not exists "+dbName)
	require.NoError(t, err)
	err = EnsureDBCreated(dbName)
	require.NoError(t, err)
	// clean up
	err = Exec(conn, "DROP DATABASE IF EXISTS "+dbName)
	require.NoError(t, err)
}

func TestEnsureTokenCreated(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("token test only for enterprise edition")
		return
	}
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	require.NoError(t, err)
	defer func() {
		wrapper.TaosClose(conn)
	}()
	tokenName := "test_ensure_token_created"
	values, err := Query(conn, fmt.Sprintf("CREATE TOKEN %s from user root", tokenName))
	require.NoError(t, err)
	token := values[0][0].(string)
	err = EnsureTokenCreated(tokenName)
	require.NoError(t, err)
	defer func() {
		// clean up
		err = Exec(conn, "DROP TOKEN "+tokenName)
		require.NoError(t, err)
	}()
	tokenConn, err := wrapper.TaosConnectToken("", token, "", 0)
	require.NoError(t, err)
	wrapper.TaosClose(tokenConn)
	require.NoError(t, err)
}

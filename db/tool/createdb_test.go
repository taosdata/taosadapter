package tool

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
	tErrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/log"
)

func TestMain(m *testing.M) {
	viper.Set("smlAutoCreateDB", true)
	viper.Set("logLevel", "trace")
	config.Init()
	log.ConfigLog()
	db.PrepareConnection()
	m.Run()
	viper.Set("smlAutoCreateDB", false)
}

// @author: xftan
// @date: 2021/12/14 15:05
// @description: test creat database with connection
func TestCreateDBWithConnection(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	type args struct {
		connection unsafe.Pointer
		db         string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test",
			args: args{
				connection: conn,
				db:         "test_auto_create_db",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := CreateDBWithConnection(tt.args.connection, log.GetLogger("test").WithField("test", "TestCreateDBWithConnection"), false, tt.args.db, 0); (err != nil) != tt.wantErr {
				t.Errorf("CreateDBWithConnection() error = %v, wantErr %v", err, tt.wantErr)
			}
			code := wrapper.TaosSelectDB(tt.args.connection, tt.args.db)
			assert.Equal(t, 0, code)
			r := wrapper.TaosQuery(conn, "drop database if exists collectd")
			code = wrapper.TaosError(r)
			if code != 0 {
				errStr := wrapper.TaosErrorStr(r)
				t.Error(tErrors.NewError(code, errStr))
			}
			wrapper.TaosFreeResult(r)
		})
	}
}

// @author: xftan
// @date: 2021/12/14 15:12
// @description:  test selectDB
func TestSchemalessSelectDB(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer wrapper.TaosClose(conn)
	type args struct {
		taosConnect unsafe.Pointer
		db          string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "auto create database",
			args: args{
				taosConnect: conn,
				db:          "auto_create_db_test",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := wrapper.TaosQuery(tt.args.taosConnect, "drop database if exists "+tt.args.db)
			code := wrapper.TaosError(result)
			if code != 0 {
				errStr := wrapper.TaosErrorStr(result)
				wrapper.TaosFreeResult(result)
				t.Error(tErrors.NewError(code, errStr))
				return
			}
			wrapper.TaosFreeResult(result)
			if err := SchemalessSelectDB(tt.args.taosConnect, log.GetLogger("test").WithField("test", "TestSchemalessSelectDB"), false, tt.args.db, 0); (err != nil) != tt.wantErr {
				t.Errorf("selectDB() error = %v, wantErr %v", err, tt.wantErr)
			}
			r := wrapper.TaosQuery(tt.args.taosConnect, fmt.Sprintf("drop database if exists %s", tt.args.db))
			code = wrapper.TaosError(r)
			if code != 0 {
				errStr := wrapper.TaosErrorStr(r)
				t.Error(tErrors.NewError(code, errStr))
			}
			wrapper.TaosFreeResult(r)
		})
	}
}

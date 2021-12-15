package schemaless

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/db"
)

func TestMain(m *testing.M) {
	config.Init()
	db.PrepareConnection()
	rand.Seed(int64(time.Now().Nanosecond()))
	m.Run()
}

// @author: xftan
// @date: 2021/12/14 15:15
// @description: test insert into TDengine with customized structure
func TestExecutor_InsertTDengine(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer wrapper.TaosClose(conn)
	exec, err := NewExecutor(conn)
	assert.NoError(t, err)
	id := rand.Int63n(100)
	value := rand.Int()
	sql, err := exec.InsertTDengine(&InsertLine{
		DB:         fmt.Sprintf("test_insert_%d", id),
		Ts:         time.Now(),
		TableName:  fmt.Sprintf("id_%d", id),
		STableName: "test",
		Fields: map[string]interface{}{
			"value": value,
		},
		TagNames:  []string{"id"},
		TagValues: []string{strconv.FormatInt(id, 10)},
	})
	t.Log(sql)
	assert.NoError(t, err)
	sql, err = exec.InsertTDengine(&InsertLine{
		DB:         fmt.Sprintf("test_insert_%d", id),
		Ts:         time.Now(),
		TableName:  fmt.Sprintf("id_%d", id),
		STableName: "test",
		Fields: map[string]interface{}{
			"value": value,
			"c1":    "c1",
			"c2":    float64(value),
			"c3":    uint(value),
			"v4":    true,
		},
		TagNames:  []string{"id"},
		TagValues: []string{strconv.FormatInt(id, 10)},
	})
	t.Log(sql)
	assert.NoError(t, err)
	sql, err = exec.InsertTDengine(&InsertLine{
		DB:         fmt.Sprintf("test_insert_%d", id),
		Ts:         time.Now(),
		TableName:  fmt.Sprintf("id_%d", id),
		STableName: "test",
		Fields: map[string]interface{}{
			"value": value,
			"c1":    "c1",
			"c2":    float64(value),
			"c3":    uint(value),
			"v4":    true,
		},
		TagNames:  []string{"id", "foo"},
		TagValues: []string{strconv.FormatInt(id, 10), "foo"},
	})
	t.Log(sql)
	assert.NoError(t, err)
	sql, err = exec.InsertTDengine(&InsertLine{
		DB:         fmt.Sprintf("test_insert_%d", id),
		Ts:         time.Now(),
		TableName:  fmt.Sprintf("id_%d", id),
		STableName: "test",
		Fields: map[string]interface{}{
			"value": value,
			"c1":    "c1",
			"c2":    float64(value),
			"c3":    uint(value),
			"v4":    true,
		},
		TagNames:  []string{"id", "foo"},
		TagValues: []string{strconv.FormatInt(id, 10), "foo333"},
	})
	t.Log(sql)
	assert.NoError(t, err)
}

// @author: xftan
// @date: 2021/12/14 15:15
// @description: test create database
func TestCreateDatabase(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer wrapper.TaosClose(conn)
	exec, err := NewExecutor(conn)
	assert.NoError(t, err)
	number := rand.Int()
	err = exec.createDatabase(fmt.Sprintf("_%d", number))
	assert.NoError(t, err)
}

// @author: xftan
// @date: 2021/12/14 15:16
// @description: test create stable
func TestExecutor_createStable(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer wrapper.TaosClose(conn)
	type fields struct {
		taosConn unsafe.Pointer
	}
	type args struct {
		info *InsertLine
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "test",
			fields: fields{
				taosConn: conn,
			},
			args: args{
				info: &InsertLine{
					DB:         fmt.Sprintf("test_insert_101"),
					Ts:         time.Now(),
					TableName:  fmt.Sprintf("id_101"),
					STableName: "test_101",
					Fields: map[string]interface{}{
						"value": 101,
						"c1":    "c1",
						"c2":    float64(101),
						"c3":    uint(101),
						"v4":    true,
					},
					TagNames:  []string{"id"},
					TagValues: []string{"101"},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Executor{
				taosConn: tt.fields.taosConn,
			}
			err = e.createDatabase(tt.args.info.DB)
			assert.NoError(t, err)
			if err := e.createStable(tt.args.info); (err != nil) != tt.wantErr {
				t.Errorf("createStable() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

package opentsdb

import (
	"database/sql/driver"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/af"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
)

// @author: xftan
// @date: 2021/12/14 15:08
// @description: test opentsdb test
func TestOpentsdb(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	config.Init()
	viper.Set("opentsdb.enable", true)
	db.PrepareConnection()

	p := Plugin{}
	router := gin.Default()
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	afC, err := af.NewConnector(conn)
	assert.NoError(t, err)
	defer afC.Close()
	if runtime.GOOS == "windows" {
		_, err = afC.Exec("create database if not exists test_plugin_opentsdb_http_json")
		assert.NoError(t, err)
	}
	err = p.Init(router)
	assert.NoError(t, err)
	err = p.Start()
	assert.NoError(t, err)
	number := rand.Int31()
	defer p.Stop()
	w := httptest.NewRecorder()
	reader := strings.NewReader(fmt.Sprintf("put metric %d %d host=web01 interface=eth0 ", time.Now().Unix(), number))
	req, _ := http.NewRequest("POST", "/put/telnet/test_plugin_opentsdb_http_telnet?ttl=1000", reader)
	req.SetBasicAuth("root", "taosdata")
	router.ServeHTTP(w, req)
	assert.Equal(t, 204, w.Code)
	w = httptest.NewRecorder()
	reader = strings.NewReader(fmt.Sprintf(`{
    "metric": "sys.cpu.nice",
    "timestamp": %d,
    "value": %d,
    "tags": {
       "host": "web01",
       "dc": "lga"
    }
}`, time.Now().Unix(), number))
	req, _ = http.NewRequest("POST", "/put/json/test_plugin_opentsdb_http_json?ttl=1000", reader)
	req.SetBasicAuth("root", "taosdata")
	router.ServeHTTP(w, req)
	assert.Equal(t, 204, w.Code)

	defer func() {
		r := wrapper.TaosQuery(conn, "drop database if exists test_plugin_opentsdb_http_json")
		code := wrapper.TaosError(r)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(r)
			t.Error(errors.NewError(code, errStr))
		}
		wrapper.TaosFreeResult(r)
	}()
	defer func() {
		r := wrapper.TaosQuery(conn, "drop database if exists test_plugin_opentsdb_http_telnet")
		code := wrapper.TaosError(r)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(r)
			t.Error(errors.NewError(code, errStr))
		}
		wrapper.TaosFreeResult(r)
	}()

	r, err := afC.Query("select last(_value) from test_plugin_opentsdb_http_json.`sys.cpu.nice`")
	if err != nil {
		t.Error(err)
		return
	}
	defer r.Close()
	values := make([]driver.Value, 1)
	err = r.Next(values)
	assert.NoError(t, err)
	if int32(values[0].(float64)) != number {
		t.Errorf("got %f expect %d", values[0], number)
	}

	r2, err := afC.Query("select last(_value) from test_plugin_opentsdb_http_telnet.`metric`")
	if err != nil {
		t.Error(err)
		return
	}
	defer r2.Close()
	values = make([]driver.Value, 1)
	err = r2.Next(values)
	assert.NoError(t, err)
	if int32(values[0].(float64)) != number {
		t.Errorf("got %f expect %d", values[0], number)
	}

	rows, err := afC.Query("select `ttl` from information_schema.ins_tables " +
		" where db_name='test_plugin_opentsdb_http_json' and stable_name='sys.cpu.nice'")
	if err != nil {
		t.Error(err)
		return
	}
	defer rows.Close()
	values = make([]driver.Value, 1)
	err = rows.Next(values)
	assert.NoError(t, err)
	if values[0].(int32) != 1000 {
		t.Fatal("ttl miss")
	}

	rows, err = afC.Query("select `ttl` from information_schema.ins_tables " +
		" where db_name='test_plugin_opentsdb_http_telnet' and stable_name='metric'")
	if err != nil {
		t.Error(err)
		return
	}
	defer rows.Close()
	values = make([]driver.Value, 1)
	err = rows.Next(values)
	assert.NoError(t, err)
	if values[0].(int32) != 1000 {
		t.Fatal("ttl miss")
	}
}

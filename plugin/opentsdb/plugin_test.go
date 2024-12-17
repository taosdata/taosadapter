package opentsdb

import (
	"database/sql/driver"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/log"
)

// @author: xftan
// @date: 2021/12/14 15:08
// @description: test opentsdb test
func TestOpentsdb(t *testing.T) {
	//nolint:staticcheck
	rand.Seed(time.Now().UnixNano())
	viper.Set("smlAutoCreateDB", true)
	defer viper.Set("smlAutoCreateDB", false)
	config.Init()
	viper.Set("opentsdb.enable", true)
	log.ConfigLog()
	db.PrepareConnection()

	p := Plugin{}
	router := gin.Default()
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		wrapper.TaosClose(conn)
	}()
	err = exec(conn, "drop database if exists test_plugin_opentsdb_http_telnet")
	assert.NoError(t, err)
	err = p.Init(router)
	assert.NoError(t, err)
	err = p.Start()
	assert.NoError(t, err)
	number := rand.Int31()
	defer func() {
		err = p.Stop()
		assert.NoError(t, err)
	}()
	w := httptest.NewRecorder()
	reader := strings.NewReader(fmt.Sprintf("put metric %d %d host=web01 interface=eth0 ", time.Now().Unix(), number))
	req, _ := http.NewRequest("POST", "/put/telnet/test_plugin_opentsdb_http_telnet?ttl=1000&app=test_telnet_http", reader)
	req.RemoteAddr = "127.0.0.1:33333"
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
	req, _ = http.NewRequest("POST", "/put/json/test_plugin_opentsdb_http_json?ttl=1000&app=test_json_http", reader)
	req.RemoteAddr = "127.0.0.1:33333"
	req.SetBasicAuth("root", "taosdata")
	router.ServeHTTP(w, req)
	assert.Equal(t, 204, w.Code)

	defer func() {
		err = exec(conn, "drop database if exists test_plugin_opentsdb_http_json")
		assert.NoError(t, err)
	}()
	defer func() {
		err = exec(conn, "drop database if exists test_plugin_opentsdb_http_telnet")
		assert.NoError(t, err)
	}()
	values, err := query(conn, "select last(_value) from test_plugin_opentsdb_http_json.`sys_cpu_nice`")
	assert.NoError(t, err)
	if int32(values[0][0].(float64)) != number {
		t.Errorf("got %f expect %d", values[0], number)
	}
	values, err = query(conn, "select last(_value) from test_plugin_opentsdb_http_telnet.`metric`")
	assert.NoError(t, err)
	if int32(values[0][0].(float64)) != number {
		t.Errorf("got %f expect %d", values[0], number)
	}
	values, err = query(conn, "select `ttl` from information_schema.ins_tables "+
		" where db_name='test_plugin_opentsdb_http_json' and stable_name='sys_cpu_nice'")
	assert.NoError(t, err)
	if values[0][0].(int32) != 1000 {
		t.Fatal("ttl miss")
	}
	values, err = query(conn, "select `ttl` from information_schema.ins_tables "+
		" where db_name='test_plugin_opentsdb_http_telnet' and stable_name='metric'")
	assert.NoError(t, err)
	if values[0][0].(int32) != 1000 {
		t.Fatal("ttl miss")
	}
}

func exec(conn unsafe.Pointer, sql string) error {
	res := wrapper.TaosQuery(conn, sql)
	defer wrapper.TaosFreeResult(res)
	code := wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		return errors.NewError(code, errStr)
	}
	return nil
}

func query(conn unsafe.Pointer, sql string) ([][]driver.Value, error) {
	res := wrapper.TaosQuery(conn, sql)
	defer wrapper.TaosFreeResult(res)
	code := wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		return nil, errors.NewError(code, errStr)
	}
	fileCount := wrapper.TaosNumFields(res)
	rh, err := wrapper.ReadColumn(res, fileCount)
	if err != nil {
		return nil, err
	}
	precision := wrapper.TaosResultPrecision(res)
	var result [][]driver.Value
	for {
		columns, errCode, block := wrapper.TaosFetchRawBlock(res)
		if errCode != 0 {
			errStr := wrapper.TaosErrorStr(res)
			return nil, errors.NewError(errCode, errStr)
		}
		if columns == 0 {
			break
		}
		r := parser.ReadBlock(block, columns, rh.ColTypes, precision)
		result = append(result, r...)
	}
	return result, nil
}

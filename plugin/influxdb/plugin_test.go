package influxdb

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
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
)

// @author: xftan
// @date: 2021/12/14 15:07
// @description: test influxdb plugin
func TestInfluxdb(t *testing.T) {
	//nolint:staticcheck
	rand.Seed(time.Now().UnixNano())
	viper.Set("smlAutoCreateDB", true)
	defer viper.Set("smlAutoCreateDB", false)
	viper.Set("influxdb.enable", true)
	config.Init()
	log.ConfigLog()
	db.PrepareConnection()
	logger := log.GetLogger("test")
	isDebug := log.IsDebug()
	p := Influxdb{}
	router := gin.Default()
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		syncinterface.TaosClose(conn, logger, isDebug)
	}()
	err = exec(conn, "drop database if exists test_plugin_influxdb")
	assert.NoError(t, err)
	defer func() {
		err = exec(conn, "drop database if exists test_plugin_influxdb")
		assert.NoError(t, err)
	}()
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
	reader := strings.NewReader(fmt.Sprintf("measurement,host=host1 field1=%di,field2=2.0,fieldKey=\"Launch 🚀\" %d", number, time.Now().UnixNano()))
	req, _ := http.NewRequest("POST", "/write?u=root&p=taosdata&db=test_plugin_influxdb&app=test_influxdb", reader)
	req.RemoteAddr = "127.0.0.1:33333"
	router.ServeHTTP(w, req)
	assert.Equal(t, 204, w.Code)
	w = httptest.NewRecorder()
	reader = strings.NewReader("measurement,host=host1 field1=a1")
	req, _ = http.NewRequest("POST", "/write?u=root&p=taosdata&db=test_plugin_influxdb&app=test_influxdb", reader)
	req.RemoteAddr = "127.0.0.1:33333"
	router.ServeHTTP(w, req)
	assert.Equal(t, 500, w.Code)
	w = httptest.NewRecorder()
	reader = strings.NewReader(fmt.Sprintf("measurement,host=host1 field1=%di,field2=2.0,fieldKey=\"Launch 🚀\" %d", number, time.Now().UnixNano()))
	req, _ = http.NewRequest("POST", "/write?u=root&p=taosdata&app=test_influxdb", reader)
	req.RemoteAddr = "127.0.0.1:33333"
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code)
	time.Sleep(time.Second)
	values, err := query(conn, "select * from test_plugin_influxdb.`measurement`")
	assert.NoError(t, err)
	if values[0][3].(string) != "Launch 🚀" {
		t.Errorf("got %s expect %s", values[0][3], "Launch 🚀")
		return
	}
	if int32(values[0][1].(int64)) != number {
		t.Errorf("got %d expect %d", values[0][1].(int64), number)
		return
	}

	w = httptest.NewRecorder()
	reader = strings.NewReader(fmt.Sprintf("measurement_ttl,host=host1 field1=%di,field2=2.0,fieldKey=\"Launch 🚀\" %d", number, time.Now().UnixNano()))
	req, _ = http.NewRequest("POST", "/write?u=root&p=taosdata&db=test_plugin_influxdb_ttl&ttl=1000&app=test_influxdb", reader)
	req.RemoteAddr = "127.0.0.1:33333"
	router.ServeHTTP(w, req)
	for i := 0; i < 10; i++ {
		values, err = query(conn, "select `ttl` from information_schema.ins_tables "+
			" where db_name='test_plugin_influxdb_ttl' and stable_name='measurement_ttl'")
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	assert.NoError(t, err)
	if values[0][0].(int32) != 1000 {
		t.Fatal("ttl miss")
	}
}

func exec(conn unsafe.Pointer, sql string) error {
	logger := log.GetLogger("test")
	isDebug := log.IsDebug()
	res := syncinterface.TaosQuery(conn, sql, logger, isDebug)
	defer syncinterface.TaosSyncQueryFree(res, logger, isDebug)
	code := syncinterface.TaosError(res, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosErrorStr(res, logger, isDebug)
		return errors.NewError(code, errStr)
	}
	return nil
}

func query(conn unsafe.Pointer, sql string) ([][]driver.Value, error) {
	logger := log.GetLogger("test")
	isDebug := log.IsDebug()
	res := syncinterface.TaosQuery(conn, sql, logger, isDebug)
	defer syncinterface.TaosSyncQueryFree(res, logger, isDebug)
	code := syncinterface.TaosError(res, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosErrorStr(res, logger, isDebug)
		return nil, errors.NewError(code, errStr)
	}
	fileCount := syncinterface.TaosNumFields(res, logger, isDebug)
	rh, err := syncinterface.ReadColumn(res, fileCount, logger, isDebug)
	if err != nil {
		return nil, err
	}
	precision := syncinterface.TaosResultPrecision(res, logger, isDebug)
	var result [][]driver.Value
	for {
		columns, errCode, block := syncinterface.TaosFetchRawBlock(res, logger, isDebug)
		if errCode != 0 {
			errStr := syncinterface.TaosErrorStr(res, logger, isDebug)
			return nil, errors.NewError(errCode, errStr)
		}
		if columns == 0 {
			break
		}
		r, err := parser.ReadBlock(block, columns, rh.ColTypes, precision)
		if err != nil {
			return nil, err
		}
		result = append(result, r...)
	}
	return result, nil
}

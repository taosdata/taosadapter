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

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/af"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/db"
)

// @author: xftan
// @date: 2021/12/14 15:07
// @description: test influxdb plugin
func TestInfluxdb(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	config.Init()
	viper.Set("influxdb.enable", true)
	db.PrepareConnection()
	p := Influxdb{}
	router := gin.Default()
	router.Use(func(c *gin.Context) {
		c.Set("currentID", uint32(1))
	})
	err := p.Init(router)
	assert.NoError(t, err)
	err = p.Start()
	assert.NoError(t, err)
	number := rand.Int31()
	defer p.Stop()
	w := httptest.NewRecorder()
	reader := strings.NewReader(fmt.Sprintf("measurement,host=host1 field1=%di,field2=2.0,fieldKey=\"Launch 🚀\" %d", number, time.Now().UnixNano()))
	req, _ := http.NewRequest("POST", "/write?u=root&p=taosdata&db=test_plugin_influxdb", reader)
	router.ServeHTTP(w, req)
	assert.Equal(t, 204, w.Code)
	w = httptest.NewRecorder()
	reader = strings.NewReader(fmt.Sprintf("measurement,host=host1 field1=a1"))
	req, _ = http.NewRequest("POST", "/write?u=root&p=taosdata&db=test_plugin_influxdb", reader)
	router.ServeHTTP(w, req)
	assert.Equal(t, 500, w.Code)
	w = httptest.NewRecorder()
	reader = strings.NewReader(fmt.Sprintf("measurement,host=host1 field1=%di,field2=2.0,fieldKey=\"Launch 🚀\" %d", number, time.Now().UnixNano()))
	req, _ = http.NewRequest("POST", "/write?u=root&p=taosdata", reader)
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code)
	time.Sleep(time.Second)
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer wrapper.TaosClose(conn)
	defer func() {
		r := wrapper.TaosQuery(conn, "drop database if exists test_plugin_influxdb")
		code := wrapper.TaosError(r)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(r)
			t.Error(errors.NewError(code, errStr))
		}
		wrapper.TaosFreeResult(r)
	}()
	afC, err := af.NewConnector(conn)
	assert.NoError(t, err)
	r, err := afC.Query("select last(*) from test_plugin_influxdb.`measurement`")
	if err != nil {
		t.Error(err)
		return
	}
	defer r.Close()
	fieldCount := len(r.Columns())
	values := make([]driver.Value, fieldCount)
	err = r.Next(values)
	assert.NoError(t, err)
	keyMap := map[string]int{}
	for i, s := range r.Columns() {
		keyMap[s] = i
	}
	if values[3].(string) != "Launch 🚀" {
		t.Errorf("got %s expect %s", values[3], "Launch 🚀")
		return
	}
	if int32(values[1].(int64)) != number {
		t.Errorf("got %d expect %d", values[1].(int64), number)
		return
	}
}

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

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v2/af"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/db"
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
	reader := strings.NewReader(fmt.Sprintf("put metric %d %d host=web01 interface=eth0 ", time.Now().Unix(), number))
	req, _ := http.NewRequest("POST", "/put/telnet/test_plugin_opentsdb_http_telnet", reader)
	req.SetBasicAuth("root", "taosdata")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
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
	req, _ = http.NewRequest("POST", "/put/json/test_plugin_opentsdb_http_json", reader)
	req.SetBasicAuth("root", "taosdata")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer wrapper.TaosClose(conn)
	afC, err := af.NewConnector(conn)
	assert.NoError(t, err)
	r, err := afC.Query("select last(value) from test_plugin_opentsdb_http_json.`sys.cpu.nice`")
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

	r2, err := afC.Query("select last(value) from test_plugin_opentsdb_http_telnet.`metric`")
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
}

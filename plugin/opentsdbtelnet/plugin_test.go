package opentsdbtelnet_test

import (
	"database/sql/driver"
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v2/af"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/db"
	"github.com/taosdata/taosadapter/plugin/opentsdbtelnet"
)

// @author: xftan
// @date: 2021/12/14 15:08
// @description: test opentsdb_telnet plugin
func TestPlugin(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	p := &opentsdbtelnet.Plugin{}
	config.Init()
	db.PrepareConnection()
	viper.Set("opentsdb_telnet.enable", true)
	err := p.Init(nil)
	assert.NoError(t, err)
	err = p.Start()
	assert.NoError(t, err)
	defer func() {
		err = p.Stop()
		assert.NoError(t, err)
	}()
	number := rand.Int31()
	c, err := net.Dial("tcp", "127.0.0.1:6046")
	assert.NoError(t, err)
	defer c.Close()
	_, err = c.Write([]byte(fmt.Sprintf("put sys.if.bytes.out 1479496100 %d host=web01 interface=eth0\r\n", number)))
	assert.NoError(t, err)
	time.Sleep(time.Second)
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer wrapper.TaosClose(conn)
	afC, err := af.NewConnector(conn)
	assert.NoError(t, err)
	r, err := afC.Query("select last(value) from opentsdb_telnet.`sys.if.bytes.out`")
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
}

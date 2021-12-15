package statsd

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
)

// @author: xftan
// @date: 2021/12/14 15:08
// @description: test statsd plugin
func TestStatsd(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	p := &Plugin{}
	config.Init()
	db.PrepareConnection()
	viper.Set("statsd.gatherInterval", time.Millisecond)
	viper.Set("statsd.enable", true)
	err := p.Init(nil)
	assert.NoError(t, err)
	err = p.Start()
	assert.NoError(t, err)
	defer func() {
		err = p.Stop()
		assert.NoError(t, err)
	}()
	number := rand.Int31()
	//	echo "foo:1|c" | nc -u -w0 127.0.0.1 8125
	c, err := net.Dial("udp", "127.0.0.1:6044")
	assert.NoError(t, err)
	_, err = c.Write([]byte(fmt.Sprintf("foo:%d|c", number)))
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
	r, err := afC.Query("select last(value) from statsd.`foo`")
	if err != nil {
		t.Error(err)
		return
	}
	defer r.Close()
	values := make([]driver.Value, 1)
	err = r.Next(values)
	assert.NoError(t, err)
	if int32(values[0].(int64)) != number {
		t.Errorf("got %f expect %d", values[0], number)
	}
}

package collectd

import (
	"context"
	"database/sql/driver"
	"math/rand"
	"net"
	"testing"
	"time"

	"collectd.org/api"
	"collectd.org/network"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v2/af"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/db"
)

// @author: xftan
// @date: 2021/12/14 15:07
// @description: test collectd plugin
func TestCollectd(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	p := &Plugin{}
	config.Init()
	db.PrepareConnection()
	viper.Set("collectd.enable", true)
	err := p.Init(nil)
	assert.NoError(t, err)
	err = p.Start()
	assert.NoError(t, err)
	defer func() {
		err = p.Stop()
		assert.NoError(t, err)
	}()
	number := rand.Int31()
	data := api.ValueList{
		Identifier: api.Identifier{
			Host:           "xyzzy",
			Plugin:         "cpu",
			PluginInstance: "0",
			Type:           "cpu",
			TypeInstance:   "user",
		},
		Values: []api.Value{
			api.Derive(number),
		},
		DSNames: []string{"t1", "t2"},
	}
	buffer := network.NewBuffer(0)

	ctx := context.Background()
	err = buffer.Write(ctx, &data)
	assert.NoError(t, err)
	bytes, err := buffer.Bytes()
	assert.NoError(t, err)
	c, err := net.Dial("udp", "127.0.0.1:6045")
	if err != nil {
		t.Error(err)
		return
	}
	defer c.Close()
	_, err = c.Write(bytes)
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(time.Second)
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer wrapper.TaosClose(conn)
	afC, err := af.NewConnector(conn)
	assert.NoError(t, err)
	r, err := afC.Query("select last(value) from collectd.`cpu_value`")
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

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
	"github.com/taosdata/driver-go/v3/af"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
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
	viper.Set("statsd.ttl", 1000)
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	afC, err := af.NewConnector(conn)
	assert.NoError(t, err)
	defer afC.Close()
	_, err = afC.Exec("create database if not exists statsd")
	assert.NoError(t, err)
	err = p.Init(nil)
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
	defer func() {
		r := wrapper.TaosQuery(conn, "drop database if exists statsd")
		code := wrapper.TaosError(r)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(r)
			t.Error(errors.NewError(code, errStr))
		}
		wrapper.TaosFreeResult(r)
	}()

	r, err := afC.Query("select last(`value`) from statsd.`foo`")
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

	rows, err := afC.Query("select `ttl` from information_schema.ins_tables " +
		" where db_name='statsd' and stable_name='foo'")
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

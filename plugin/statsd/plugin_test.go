package statsd

import (
	"database/sql/driver"
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"
	"unsafe"

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
// @description: test statsd plugin
func TestStatsd(t *testing.T) {
	//nolint:staticcheck
	rand.Seed(time.Now().UnixNano())
	p := &Plugin{}
	config.Init()
	log.ConfigLog()
	db.PrepareConnection()
	viper.Set("statsd.gatherInterval", time.Millisecond)
	viper.Set("statsd.enable", true)
	viper.Set("statsd.ttl", 1000)
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		wrapper.TaosClose(conn)
	}()
	err = exec(conn, "drop database if exists statsd")
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
	values, err := query(conn, "select last(`value`) from statsd.`foo`")
	assert.NoError(t, err)
	if int32(values[0][0].(int64)) != number {
		t.Errorf("got %f expect %d", values[0], number)
	}
	values, err = query(conn, "select `ttl` from information_schema.ins_tables "+
		" where db_name='statsd' and stable_name='foo'")
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

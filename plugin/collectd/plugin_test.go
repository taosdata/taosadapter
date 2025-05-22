package collectd

import (
	"context"
	"database/sql/driver"
	"math/rand"
	"net"
	"testing"
	"time"
	"unsafe"

	"collectd.org/api"
	"collectd.org/network"
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
// @description: test collectd plugin
func TestCollectd(t *testing.T) {
	config.Init()
	db.PrepareConnection()
	logger := log.GetLogger("test")
	isDebug := log.IsDebug()
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		syncinterface.TaosClose(conn, logger, isDebug)
	}()
	err = exec(conn, "drop database if exists collectd")
	assert.NoError(t, err)
	err = exec(conn, "create database if not exists collectd")
	assert.NoError(t, err)
	//nolint:staticcheck
	rand.Seed(time.Now().UnixNano())
	p := &Plugin{}
	viper.Set("collectd.enable", true)
	viper.Set("collectd.ttl", 1000)
	err = p.Init(nil)
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
	defer func() {
		err = c.Close()
		assert.NoError(t, err)
	}()
	_, err = c.Write(bytes)
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(3 * time.Second)

	defer func() {
		r := syncinterface.TaosQuery(conn, "drop database if exists collectd", logger, isDebug)
		code := syncinterface.TaosError(r, logger, isDebug)
		if code != 0 {
			errStr := syncinterface.TaosErrorStr(r, logger, isDebug)
			t.Error(errors.NewError(code, errStr))
		}
		syncinterface.TaosFreeResult(r, logger, isDebug)
	}()
	values, err := query(conn, "select last(`value`) from collectd.`cpu_value`")
	assert.NoError(t, err)
	if int32(values[0][0].(float64)) != number {
		t.Errorf("got %f expect %d", values[0], number)
	}
	for i := 0; i < 10; i++ {
		values, err = query(conn, "select `ttl` from information_schema.ins_tables "+
			" where db_name='collectd' and stable_name='cpu_value'")
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
	defer syncinterface.TaosFreeResult(res, logger, isDebug)
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
	defer syncinterface.TaosFreeResult(res, logger, isDebug)
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

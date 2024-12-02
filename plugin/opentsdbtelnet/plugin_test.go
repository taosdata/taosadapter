package opentsdbtelnet_test

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
	"github.com/taosdata/taosadapter/v3/plugin/opentsdbtelnet"
)

// @author: xftan
// @date: 2021/12/14 15:08
// @description: test opentsdb_telnet plugin
func TestPlugin(t *testing.T) {
	//nolint:staticcheck
	rand.Seed(time.Now().UnixNano())
	p := &opentsdbtelnet.Plugin{}
	config.Init()
	log.ConfigLog()
	db.PrepareConnection()
	viper.Set("opentsdb_telnet.enable", true)
	viper.Set("opentsdb_telnet.batchSize", 1)
	viper.Set("opentsdb_telnet.ttl", 1000)
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		wrapper.TaosClose(conn)
	}()
	err = exec(conn, "drop database if exists opentsdb_telnet")
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
	c, err := net.Dial("tcp", "127.0.0.1:6046")
	assert.NoError(t, err)
	defer func() {
		err = c.Close()
		assert.NoError(t, err)
	}()
	_, err = c.Write([]byte(fmt.Sprintf("put sys.if.bytes.out 1479496100 %d host=web01 interface=eth0\r\n", number)))
	assert.NoError(t, err)
	time.Sleep(time.Second)

	defer func() {
		r := wrapper.TaosQuery(conn, "drop database if exists opentsdb_telnet")
		code := wrapper.TaosError(r)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(r)
			t.Error(errors.NewError(code, errStr))
		}
		wrapper.TaosFreeResult(r)
	}()
	values, err := query(conn, "select last(_value) from opentsdb_telnet.`sys_if_bytes_out`")
	assert.NoError(t, err)
	if int32(values[0][0].(float64)) != number {
		t.Errorf("got %f expect %d", values[0], number)
	}
	values, err = query(conn, "select `ttl` from information_schema.ins_tables "+
		" where db_name='opentsdb_telnet' and stable_name='sys_if_bytes_out'")
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

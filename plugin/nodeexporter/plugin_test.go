package nodeexporter

import (
	"database/sql/driver"
	"net/http"
	"net/http/httptest"
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

var s = `
# HELP go_gc_duration_seconds A summary of the GC invocation durations.
# TYPE go_gc_duration_seconds summary
go_gc_duration_seconds{quantile="0"} 0.00010425500000000001
go_gc_duration_seconds{quantile="0.25"} 0.000139108
go_gc_duration_seconds{quantile="0.5"} 0.00015749400000000002
go_gc_duration_seconds{quantile="0.75"} 0.000331463
go_gc_duration_seconds{quantile="1"} 0.000667154
go_gc_duration_seconds_sum 0.0018183950000000002
go_gc_duration_seconds_count 7
# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines 15
# HELP test_metric An untyped metric with a timestamp
# TYPE test_metric untyped
test_metric{label="value"} 1.0 1490802350000
`

// @author: xftan
// @date: 2021/12/14 15:08
// @description: test node-exporter plugin
func TestNodeExporter_Gather(t *testing.T) {
	config.Init()
	log.ConfigLog()
	db.PrepareConnection()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte(s))
		if err != nil {
			return
		}
	}))
	defer ts.Close()
	api := ts.URL
	viper.Set("node_exporter.enable", true)
	viper.Set("node_exporter.urls", []string{api})
	viper.Set("node_exporter.gatherDuration", time.Second)
	viper.Set("node_exporter.ttl", 1000)
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer func() {
		wrapper.TaosClose(conn)
	}()
	err = exec(conn, "drop database if exists node_exporter")
	assert.NoError(t, err)
	err = exec(conn, "use node_exporter")
	assert.NoError(t, err)
	n := NodeExporter{}
	err = n.Init(nil)
	assert.NoError(t, err)
	err = n.Start()
	assert.NoError(t, err)
	time.Sleep(time.Second * 2)
	values, err := query(conn, "select last(`value`) as `value` from node_exporter.go_gc_duration_seconds;")
	assert.NoError(t, err)
	assert.Equal(t, float64(1), values[0][0])
	err = n.Stop()
	assert.NoError(t, err)
	values, err = query(conn, "select `ttl` from information_schema.ins_tables "+
		" where db_name='node_exporter' and stable_name='test_metric'")
	assert.NoError(t, err)
	if values[0][0].(int32) != 1000 {
		t.Fatal("ttl miss")
	}
	err = exec(conn, "drop database if exists node_exporter")
	assert.NoError(t, err)
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

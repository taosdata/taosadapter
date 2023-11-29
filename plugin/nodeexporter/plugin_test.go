package nodeexporter

import (
	"database/sql/driver"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/af"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
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
	db.PrepareConnection()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(s))
	}))
	defer ts.Close()
	api := ts.URL
	viper.Set("node_exporter.enable", true)
	viper.Set("node_exporter.urls", []string{api})
	viper.Set("node_exporter.gatherDuration", time.Second)
	viper.Set("node_exporter.ttl", 1000)
	conn, err := af.Open("", "", "", "", 0)
	assert.NoError(t, err)
	defer conn.Close()
	_, err = conn.Exec("create database if not exists node_exporter precision 'ns'")
	assert.NoError(t, err)
	err = conn.SelectDB("node_exporter")
	assert.NoError(t, err)
	n := NodeExporter{}
	err = n.Init(nil)
	assert.NoError(t, err)
	err = n.Start()
	assert.NoError(t, err)
	time.Sleep(time.Second * 2)
	rows, err := conn.Query("select last(`value`) as `value` from node_exporter.test_metric;")
	assert.NoError(t, err)
	defer rows.Close()
	assert.Equal(t, 1, len(rows.Columns()))
	d := make([]driver.Value, 1)
	err = rows.Next(d)
	assert.NoError(t, err)
	assert.Equal(t, float64(1), d[0])
	err = n.Stop()
	assert.NoError(t, err)

	rows, err = conn.Query("select `ttl` from information_schema.ins_tables " +
		" where db_name='node_exporter' and stable_name='test_metric'")
	if err != nil {
		t.Error(err)
		return
	}
	defer rows.Close()
	values := make([]driver.Value, 1)
	err = rows.Next(values)
	assert.NoError(t, err)
	if values[0].(int32) != 1000 {
		t.Fatal("ttl miss")
	}

	_, err = conn.Exec("drop database if exists node_exporter")
	assert.NoError(t, err)
}

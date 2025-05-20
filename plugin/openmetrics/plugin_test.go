package openmetrics

import (
	"database/sql/driver"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
	"unsafe"

	"github.com/influxdata/telegraf/plugins/parsers/openmetrics"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/log"
	"google.golang.org/protobuf/proto"
)

var prometheus004 = `
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

var openMetricsV1 = `
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
# TYPE test_metric gauge
test_metric{label="value"} 1.0 1490802350
# EOF
`

func TestOpenMetrics(t *testing.T) {
	config.Init()
	log.ConfigLog()
	db.PrepareConnection()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			w.Header().Set("Content-Type", "application/openmetrics-text")
			_, err := w.Write([]byte(openMetricsV1))
			if err != nil {
				return
			}
		} else if r.URL.Path == "/prometheus" {
			w.Header().Set("Content-Type", "application/openmetrics-text")
			_, err := w.Write([]byte(prometheus004))
			if err != nil {
				return
			}
		} else if r.URL.Path == "/wrongtype" {
			_, err := w.Write([]byte(prometheus004))
			if err != nil {
				return
			}
		} else if r.URL.Path == "/protobuf" {
			var metricSet openmetrics.MetricSet
			metricSet.MetricFamilies = []*openmetrics.MetricFamily{
				{
					Name: "test_gauge",
					Type: openmetrics.MetricType_GAUGE,
					Help: "test gauge",
					Metrics: []*openmetrics.Metric{
						{
							MetricPoints: []*openmetrics.MetricPoint{
								{
									Value: &openmetrics.MetricPoint_GaugeValue{
										GaugeValue: &openmetrics.GaugeValue{
											Value: &openmetrics.GaugeValue_DoubleValue{
												DoubleValue: 1234567,
											},
										},
									},
								},
							},
						},
					},
				},
			}
			bs, err := proto.Marshal(&metricSet)
			if err != nil {
				t.Fatal(err)
			}
			w.Header().Set("Content-Type", "application/openmetrics-protobuf; version=1.0.0")
			_, err = w.Write([]byte(bs))
			if err != nil {
				return
			}
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()
	api := ts.URL
	viper.Set("open_metrics.enable", true)
	viper.Set("open_metrics.urls", []string{api, api + "/prometheus", api + "/wrongtype", api + "/protobuf"})
	viper.Set("open_metrics.gatherDurationSeconds", []int{1, 1, 1, 1})
	viper.Set("open_metrics.ttl", []int{1000, 1000, 1000, 1000})
	viper.Set("open_metrics.dbs", []string{"open_metrics", "open_metrics", "open_metrics", "open_metrics_proto"})
	viper.Set("open_metrics.responseTimeoutSeconds", []int{5, 5, 5, 5})
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer func() {
		wrapper.TaosClose(conn)
	}()
	err = exec(conn, "create database if not exists open_metrics precision 'ns'")
	assert.NoError(t, err)
	err = exec(conn, "create database if not exists open_metrics_proto precision 'ns'")
	assert.NoError(t, err)
	openMetrics := &OpenMetrics{}
	assert.Equal(t, "openmetrics", openMetrics.String())
	assert.Equal(t, "v1", openMetrics.Version())
	err = openMetrics.Init(nil)
	assert.NoError(t, err)
	err = openMetrics.Start()
	assert.NoError(t, err)
	time.Sleep(time.Second * 3)
	values, err := query(conn, "select last(`gauge`) as `gauge` from open_metrics.test_metric;")
	assert.NoError(t, err)
	assert.Equal(t, float64(1), values[0][0])
	values, err = query(conn, "select last(`gauge`) as `gauge` from open_metrics_proto.test_gauge;")
	assert.NoError(t, err)
	assert.Equal(t, float64(1234567), values[0][0])
	err = openMetrics.Stop()
	assert.NoError(t, err)
	for i := 0; i < 10; i++ {
		values, err = query(conn, "select `ttl` from information_schema.ins_tables "+
			" where db_name='open_metrics' and stable_name='test_metric'")
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	assert.NoError(t, err)
	if values[0][0].(int32) != 1000 {
		t.Fatal("ttl miss")
	}
	err = exec(conn, "drop database if exists open_metrics")
	assert.NoError(t, err)
	err = exec(conn, "drop database if exists open_metrics_proto")
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
		r, err := parser.ReadBlock(block, columns, rh.ColTypes, precision)
		if err != nil {
			return nil, err
		}
		result = append(result, r...)
	}
	return result, nil
}

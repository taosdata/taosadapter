package system

import (
	_ "github.com/taosdata/taosadapter/plugin/collectd"
	_ "github.com/taosdata/taosadapter/plugin/influxdb"
	_ "github.com/taosdata/taosadapter/plugin/nodeexporter"
	_ "github.com/taosdata/taosadapter/plugin/opentsdb"
	_ "github.com/taosdata/taosadapter/plugin/opentsdbtelnet"
	_ "github.com/taosdata/taosadapter/plugin/prometheus"
	_ "github.com/taosdata/taosadapter/plugin/statsd"
)

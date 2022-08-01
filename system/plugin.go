package system

import (
	_ "github.com/taosdata/taosadapter/v3/plugin/collectd"
	_ "github.com/taosdata/taosadapter/v3/plugin/influxdb"
	_ "github.com/taosdata/taosadapter/v3/plugin/nodeexporter"
	_ "github.com/taosdata/taosadapter/v3/plugin/opentsdb"
	_ "github.com/taosdata/taosadapter/v3/plugin/opentsdbtelnet"
	_ "github.com/taosdata/taosadapter/v3/plugin/prometheus"
	_ "github.com/taosdata/taosadapter/v3/plugin/statsd"
)

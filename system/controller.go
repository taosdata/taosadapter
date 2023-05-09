package system

import (
	// http
	_ "github.com/taosdata/taosadapter/v3/controller/metrics"
	_ "github.com/taosdata/taosadapter/v3/controller/ping"
	_ "github.com/taosdata/taosadapter/v3/controller/rest"
	// websocket
	_ "github.com/taosdata/taosadapter/v3/controller/ws/query"
	_ "github.com/taosdata/taosadapter/v3/controller/ws/schemaless"
	_ "github.com/taosdata/taosadapter/v3/controller/ws/stmt"
	_ "github.com/taosdata/taosadapter/v3/controller/ws/tmq"
)

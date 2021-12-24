module github.com/taosdata/taosadapter

go 1.14

replace github.com/lestrrat-go/file-rotatelogs => ./log/file-rotatelogs

require (
	cloud.google.com/go/kms v1.0.0 // indirect
	cloud.google.com/go/monitoring v1.0.0 // indirect
	collectd.org v0.5.0
	github.com/gin-contrib/cors v1.3.1
	github.com/gin-contrib/gzip v0.0.3
	github.com/gin-contrib/pprof v1.3.0
	github.com/gin-gonic/gin v1.7.4
	github.com/influxdata/influxdb/v2 v2.0.9
	github.com/influxdata/telegraf v1.20.0
	github.com/lestrrat-go/file-rotatelogs v2.4.0+incompatible
	github.com/lestrrat-go/strftime v1.0.5 // indirect
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/prometheus/prometheus v1.8.2-0.20211209112917-bd21aafb66d6 // indirect
	github.com/shopspring/decimal v1.2.0 // indirect
	github.com/silenceper/pool v1.0.0
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.9.0
	github.com/stretchr/testify v1.7.0
	github.com/swaggo/swag v1.7.6
	github.com/taosdata/driver-go/v2 v2.0.1-0.20211215031937-7da3cc9e4ad1
	github.com/valyala/fastjson v1.6.3
)

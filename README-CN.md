# taosAdapter

taosAdapter 是一个 TDengine 的配套工具，是 TDengine 集群和应用程序之间的桥梁和适配器。它提供了一种易于使用和高效的方式来直接从数据收集代理软件（如 Telegraf、StatsD、collectd 等）摄取数据。它还提供了 InfluxDB/OpenTSDB 兼容的数据摄取接口，允许 InfluxDB/OpenTSDB 应用程序无缝移植到 TDengine。

taosAdapter提供以下功能：

    - RESTful 接口
    - 兼容 InfluxDB v1写接口
    - 兼容 OpenTSDB JSON 和 telnet 格式写入
    - 无缝连接到 Telegraf
    - 无缝连接到 collectd
    - 无缝连接到 StatsD


## taosAdapter 架构图
![taosAdapter-architecture](taosAdapter-architecture-for-public.png)

taosAdapter 从 TDengine v2.3.0.0 版本开始成为 TDengine 服务端软件 的一部分，您不需要任何额外的步骤来设置 taosAdapter。taosAdapter 将由 TDengine 服务端软件通过 systemd 管理，它将在启动 taosd 服务命令 systemctl start taosd 自动启动，通过退出 taosd 服务命令 systemctl stop taosd 停止。它也可以通过 systemctl start taosadapter 和 systemctl stop taosadapter 单独启动服务或停止服务。启动或停止 taosAdapter 并不会影响 taosd 自身的服务。

你可以从(涛思数据官方网站)[https://taosdata.com/cn/all-downloads/]下载TDengine（taosAdapter包含在v2.3.0.0及以上版本）。

## 构建 taosAdapter

我们强烈建议将 taosAdapter 和 TDengine 服务端共同部署在同一个系统上，并使用官方 TDengine 安装包安装 taosAdapter。如果你想对 taosAdapter 进行调试或贡献代码，你也可以单独构建它。

### 设置 golang 开发环境

taosAdapter 是由 Go 语言开发的。关于 golang 开发环境的设置，请参考 golang 的[官方文档](https://go.dev/learn/)。

请使用1.14以上版本的 golang。对于中国的用户，我们建议使用代理服务来加速软件包的下载。
```
go env -w GO111MODULE=on
go env -w GOPROXY=https://goproxy.cn,direct
```

## 作为 TDengine 的一个组件构建 taosAdapter

taosAdapter 的源代码是作为一个独立的代码库托管的，也通过子模块的方式存在于 TDengine 中。你可以下载 TDengine 的源代码并同时构建它们。步骤如下：
```
git clone https://github.com/taosdata/TDengine
cd TDengine
git submodule update --init --recursive
mkdir debug
cd debug
cmake ..
make
sudo make install
```

一旦 make install 完成，taosAdapter 和它的 systemd 服务文件就会被安装到有 TDengine 服务端软件的系统中。您可以使用 sudo systemctl start taosd 和 sudo systemclt stop taosd 来启动它们。

##  单独构建 taosAdapter
如果你已经部署了 TDengine 服务器 v2.3.0.0 或以上的版本，taosAdapter 也可以作为一个独立的应用程序被构建。

### 安装 TDengine 服务器或客户端安装包
请从官方网站下载 TDengine 服务器或客户端安装包。

### 构建 taosAdapter
```
git clone https://github.com/taosdata/taosadapter
cd taosadapter
go build
```

然后您应该在工作目录中找到 taosAdapter 的二进制可执行文件。您需要将 systemd 服务配置文件 taosadapter.service 复制到 /etc/systemd/system 目录，并将可执行的 taosAdapter 二进制文件复制到 Linux 的 $PATH 环境变量可以找到的路径下。


## 功能列表

* 与 RESTful 接口兼容
  [https://www.taosdata.com/cn/documentation/connector#restful](https://www.taosdata.com/cn/documentation/connector#restful)
* 兼容 InfluxDB v1 写接口
  [https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/](https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/)
* 兼容 OpenTSDB JSON 和 telnet 格式写入
  [http://opentsdb.net/docs/build/html/api_http/put.html](http://opentsdb.net/docs/build/html/api_http/put.html)
* 与collectd无缝连接
    collectd 是一个系统统计收集守护程序，请访问 [https://collectd.org/](https://collectd.org/) 了解更多信息。
* Seamless connection with StatsD
  StatsD 是一个简单而强大的统计信息汇总的守护程序。请访问 [https://github.com/statsd/statsd](https://github.com/statsd/statsd) 了解更多信息。
* 与 icinga2 的无缝连接
  icinga2 是一个收集检查结果指标和性能数据的软件。请访问 [https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer](https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer) 了解更多信息。
* 与 tcollector 无缝连接
  TCollector是一个客户端进程，从本地收集器收集数据，并将数据推送到OpenTSDB。请访问 [http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html](http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html) 了解更多信息。
* 无缝连接 node_exporter
  node_export 是一个机器指标的导出器。请访问 [https://github.com/prometheus/node_exporter](https://github.com/prometheus/node_exporter) 了解更多信息。

## 接口

### TDengine RESTful 接口
您可以使用任何支持 http 协议的客户端通过访问 RESTful 接口地址 “https://<fqdn>:6041/<APIEndPoint>” 来写入数据到 TDengine 或从 TDengine 中查询数据。细节请参考[官方文档](https://www.taosdata.com/cn/documentation/connector#restful)。支持如下 EndPoint ：
```
/rest/sql
/rest/sqlt
/rest/sqlutc
```

### InfluxDB
您可以使用任何支持 http 协议的客户端访问 Restful 接口地址 “https://<fqdn>:6041/<APIEndPoint>” 来写入 InfluxDB 兼容格式的数据到 TDengine。EndPoint 如下：
```
/influxdb/v1/write
```

支持 InfluxDB 查询参数如下：

* `db` 指定 TDengine 使用的数据库名
* `precision` TDengine 使用的时间精度
* `u` TDengine 用户名
* `p` TDengine 密码

### OpenTSDB
您可以使用任何支持 http 协议的客户端访问 Restful 接口地址 “https://<fqdn>:6041/<APIEndPoint>” 来写入 OpenTSDB 兼容格式的数据到 TDengine。EndPoint 如下：
```
/opentsdb/v1/put/json/:db
/opentsdb/v1/put/telnet/:db
```

### collectd

#### 直接采集

修改 collectd 配置文件 `/etc/collectd/collectd.conf`，taosAdapter 默认使用端口 6045 来接收 collectd 直接采集方式的数据。

```
LoadPlugin network
<Plugin network>
         Server "127.0.0.1" "6045"
</Plugin>
```

#### tsdb 写入方式

修改 collectd 配置文件 `/etc/collectd/collectd.conf`，taosAdapter 默认使用端口 6047 来接收 collectd tsdb 写入方式的数据。

```
LoadPlugin write_tsdb
<Plugin write_tsdb>
        <Node>
                Host "localhost"
                Port "6047"
                HostTags "status=production"
                StoreRates false
                AlwaysAppendDS false
        </Node>
</Plugin>
```

### StatsD

修改 StatsD 配置文件 `config.js`，taosAdapter 默认使用 6044 端口接收 StatsD 的写入数据。

* > `backends` add `"./backends/repeater"`
* > `repeater` add `{ host:'host to taosAdapter', port: 6044}`

配置文件示例

```
{
port: 8125
, backends: ["./backends/repeater"]
, repeater: [{ host: '127.0.0.1', port: 6044}]
}
```

### icinga2 OpenTSDB writer

使用 icinga2 收集监控数据的方法参见：

* 参考文档：
  opentsdb-writer [https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer](https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer)
* 使能 taosAdapter `opentsdb_telnet.enable` 来支持写入
* 修改配置文件 `/etc/icinga2/features-enabled/opentsdb.conf`， taosAdapter 默认使用 6048 端口接收 icinga2 的数据。

```
object OpenTsdbWriter "opentsdb" {
  host = "host to taosAdapter"
  port = 6048
}
```

### TCollector

Tcollector 是一个客户端进程，它从本地收集器中收集数据并将数据推送到 OpenTSDB。你在你的所有主机上运行它，它完成将每个主机的数据发送到 TSD （OpenTSDB 后台服务进程）的工作。

* 启用 taosAdapter 配置 opentsdb_telnet.enable
* 修改 TCollector 配置文件，将 OpenTSDB 主机修改为部署 taosAdapter 的主机，并修改端口为6049

### node_exporter

Prometheus 使用的由*NIX内核暴露的硬件和操作系统指标的输出器

* 启用 taosAdapter 的配置 node_exporter.enable
* 设置 node_exporter 的相关配置
* 重新启动 taosAdapter

## 配置方法

taosAdapter 支持通过命令行参数、环境变量和配置文件来进行配置。

命令行参数优先于环境变量优先于配置文件，命令行用法是arg=val，如 taosadapter -p=30000 --debug=true，详细列表如下：

```shell
Usage of taosAdapter:
      --collectd.db string                           collectd db name. Env "TAOS_ADAPTER_COLLECTD_DB" (default "collectd")
      --collectd.enable                              enable collectd. Env "TAOS_ADAPTER_COLLECTD_ENABLE" (default true)
      --collectd.password string                     collectd password. Env "TAOS_ADAPTER_COLLECTD_PASSWORD" (default "taosdata")
      --collectd.port int                            collectd server port. Env "TAOS_ADAPTER_COLLECTD_PORT" (default 6045)
      --collectd.user string                         collectd user. Env "TAOS_ADAPTER_COLLECTD_USER" (default "root")
      --collectd.worker int                          collectd write worker. Env "TAOS_ADAPTER_COLLECTD_WORKER" (default 10)
  -c, --config string                                config path default /etc/taos/taosadapter.toml
      --cors.allowAllOrigins                         cors allow all origins. Env "TAOS_ADAPTER_CORS_ALLOW_ALL_ORIGINS"
      --cors.allowCredentials                        cors allow credentials. Env "TAOS_ADAPTER_CORS_ALLOW_Credentials"
      --cors.allowHeaders stringArray                cors allow HEADERS. Env "TAOS_ADAPTER_ALLOW_HEADERS"
      --cors.allowOrigins stringArray                cors allow origins. Env "TAOS_ADAPTER_ALLOW_ORIGINS"
      --cors.allowWebSockets                         cors allow WebSockets. Env "TAOS_ADAPTER_CORS_ALLOW_WebSockets"
      --cors.exposeHeaders stringArray               cors expose headers. Env "TAOS_ADAPTER_Expose_Headers"
      --debug                                        enable debug mode. Env "TAOS_ADAPTER_DEBUG"
      --help                                         Print this help message and exit
      --influxdb.enable                              enable InfluxDB. Env "TAOS_ADAPTER_INFLUXDB_ENABLE" (default true)
      --log.path string                              log path. Env "TAOS_ADAPTER_LOG_PATH" (default "/var/log/taos")
      --log.rotationCount uint                       log rotation count. Env "TAOS_ADAPTER_LOG_ROTATION_COUNT" (default 30)
      --log.rotationSize string                      log rotation size(KB MB GB), must be a positive integer. Env "TAOS_ADAPTER_LOG_ROTATION_SIZE" (default "1GB")
      --log.rotationTime duration                    log rotation time. Env "TAOS_ADAPTER_LOG_ROTATION_TIME" (default 24h0m0s)
      --logLevel string                              log level (panic fatal error warn warning info debug trace). Env "TAOS_ADAPTER_LOG_LEVEL" (default "info")
      --node_exporter.caCertFile string              node_exporter ca cert file path. Env "TAOS_ADAPTER_NODE_EXPORTER_CA_CERT_FILE"
      --node_exporter.certFile string                node_exporter cert file path. Env "TAOS_ADAPTER_NODE_EXPORTER_CERT_FILE"
      --node_exporter.db string                      node_exporter db name. Env "TAOS_ADAPTER_NODE_EXPORTER_DB" (default "node_exporter")
      --node_exporter.enable                         enable node_exporter. Env "TAOS_ADAPTER_NODE_EXPORTER_ENABLE"
      --node_exporter.gatherDuration duration        node_exporter gather duration. Env "TAOS_ADAPTER_NODE_EXPORTER_GATHER_DURATION" (default 5s)
      --node_exporter.httpBearerTokenString string   node_exporter http bearer token. Env "TAOS_ADAPTER_NODE_EXPORTER_HTTP_BEARER_TOKEN_STRING"
      --node_exporter.httpPassword string            node_exporter http password. Env "TAOS_ADAPTER_NODE_EXPORTER_HTTP_PASSWORD"
      --node_exporter.httpUsername string            node_exporter http username. Env "TAOS_ADAPTER_NODE_EXPORTER_HTTP_USERNAME"
      --node_exporter.insecureSkipVerify             node_exporter skip ssl check. Env "TAOS_ADAPTER_NODE_EXPORTER_INSECURE_SKIP_VERIFY" (default true)
      --node_exporter.keyFile string                 node_exporter cert key file path. Env "TAOS_ADAPTER_NODE_EXPORTER_KEY_FILE"
      --node_exporter.password string                node_exporter password. Env "TAOS_ADAPTER_NODE_EXPORTER_PASSWORD" (default "taosdata")
      --node_exporter.responseTimeout duration       node_exporter response timeout. Env "TAOS_ADAPTER_NODE_EXPORTER_RESPONSE_TIMEOUT" (default 5s)
      --node_exporter.urls strings                   node_exporter urls. Env "TAOS_ADAPTER_NODE_EXPORTER_URLS" (default [http://localhost:9100])
      --node_exporter.user string                    node_exporter user. Env "TAOS_ADAPTER_NODE_EXPORTER_USER" (default "root")
      --opentsdb.enable                              enable opentsdb. Env "TAOS_ADAPTER_OPENTSDB_ENABLE" (default true)
      --opentsdb_telnet.dbs strings                  opentsdb_telnet db names. Env "TAOS_ADAPTER_OPENTSDB_TELNET_DBS" (default [opentsdb_telnet,collectd_tsdb,icinga2_tsdb,tcollector_tsdb])
      --opentsdb_telnet.enable                       enable opentsdb telnet,warning: without auth info(default false). Env "TAOS_ADAPTER_OPENTSDB_TELNET_ENABLE"
      --opentsdb_telnet.maxTCPConnections int        max tcp connections. Env "TAOS_ADAPTER_OPENTSDB_TELNET_MAX_TCP_CONNECTIONS" (default 250)
      --opentsdb_telnet.password string              opentsdb_telnet password. Env "TAOS_ADAPTER_OPENTSDB_TELNET_PASSWORD" (default "taosdata")
      --opentsdb_telnet.ports ints                   opentsdb telnet tcp port. Env "TAOS_ADAPTER_OPENTSDB_TELNET_PORTS" (default [6046,6047,6048,6049])
      --opentsdb_telnet.tcpKeepAlive                 enable tcp keep alive. Env "TAOS_ADAPTER_OPENTSDB_TELNET_TCP_KEEP_ALIVE"
      --opentsdb_telnet.user string                  opentsdb_telnet user. Env "TAOS_ADAPTER_OPENTSDB_TELNET_USER" (default "root")
      --opentsdb_telnet.worker int                   opentsdb_telnet write worker. Env "TAOS_ADAPTER_OPENTSDB_TELNET_WORKER" (default 1000)
      --pool.idleTimeout duration                    Set idle connection timeout. Env "TAOS_ADAPTER_POOL_IDLE_TIMEOUT" (default 1h0m0s)
      --pool.maxConnect int                          max connections to taosd. Env "TAOS_ADAPTER_POOL_MAX_CONNECT" (default 4000)
      --pool.maxIdle int                             max idle connections to taosd. Env "TAOS_ADAPTER_POOL_MAX_IDLE" (default 4000)
  -P, --port int                                     http port. Env "TAOS_ADAPTER_PORT" (default 6041)
      --ssl.certFile string                          ssl cert file path. Env "TAOS_ADAPTER_SSL_CERT_FILE"
      --ssl.enable                                   enable ssl. Env "TAOS_ADAPTER_SSL_ENABLE"
      --ssl.keyFile string                           ssl key file path. Env "TAOS_ADAPTER_SSL_KEY_FILE"
      --statsd.allowPendingMessages int              statsd allow pending messages. Env "TAOS_ADAPTER_STATSD_ALLOW_PENDING_MESSAGES" (default 50000)
      --statsd.db string                             statsd db name. Env "TAOS_ADAPTER_STATSD_DB" (default "statsd")
      --statsd.deleteCounters                        statsd delete counter cache after gather. Env "TAOS_ADAPTER_STATSD_DELETE_COUNTERS" (default true)
      --statsd.deleteGauges                          statsd delete gauge cache after gather. Env "TAOS_ADAPTER_STATSD_DELETE_GAUGES" (default true)
      --statsd.deleteSets                            statsd delete set cache after gather. Env "TAOS_ADAPTER_STATSD_DELETE_SETS" (default true)
      --statsd.deleteTimings                         statsd delete timing cache after gather. Env "TAOS_ADAPTER_STATSD_DELETE_TIMINGS" (default true)
      --statsd.enable                                enable statsd. Env "TAOS_ADAPTER_STATSD_ENABLE" (default true)
      --statsd.gatherInterval duration               statsd gather interval. Env "TAOS_ADAPTER_STATSD_GATHER_INTERVAL" (default 5s)
      --statsd.maxTCPConnections int                 statsd max tcp connections. Env "TAOS_ADAPTER_STATSD_MAX_TCP_CONNECTIONS" (default 250)
      --statsd.password string                       statsd password. Env "TAOS_ADAPTER_STATSD_PASSWORD" (default "taosdata")
      --statsd.port int                              statsd server port. Env "TAOS_ADAPTER_STATSD_PORT" (default 6044)
      --statsd.protocol string                       statsd protocol [tcp or udp]. Env "TAOS_ADAPTER_STATSD_PROTOCOL" (default "udp")
      --statsd.tcpKeepAlive                          enable tcp keep alive. Env "TAOS_ADAPTER_STATSD_TCP_KEEP_ALIVE"
      --statsd.user string                           statsd user. Env "TAOS_ADAPTER_STATSD_USER" (default "root")
      --statsd.worker int                            statsd write worker. Env "TAOS_ADAPTER_STATSD_WORKER" (default 10)
      --taosConfigDir string                         load taos client config path. Env "TAOS_ADAPTER_TAOS_CONFIG_FILE"
      --version                                      Print the version and exit
```

示例配置文件参见 [example/config/taosadapter.toml](example/config/taosadapter.toml)。

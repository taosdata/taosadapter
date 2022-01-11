# taosAdapter

taosAdapter is a TDengine’s companion tool and is a bridge/adapter between TDengine cluster and application. It provides an easy-to-use and efficient way to ingest data from data collections agents(like Telegraf, StatsD, collectd) directly. It also provides InfluxDB/OpenTSDB compatible data ingestion interface to allow InfluxDB/OpenTSDB applications to immigrate to TDengine seamlessly.

taosAdapter provides the following functions.

    - RESTful interface
    - Compatible with InfluxDB v1 write interface
    - Compatible with OpenTSDB JSON and telnet format write
    - Seamless connect to Telegraf
    - Seamless connect to collectD
    - Seamless connect to StatsD
    - Support Prometheus remote_read and remote_write (Working In Progress)

## taosAdapter architecture
![taosAdapter-architecture](taosAdapter-architecture-for-public.png)

taosAdapter is part of the TDengine server from TDengine v2.3.0.0. You don't need any additional steps to set taosAdapter up. taosAdapter will be managed by the TDengine server via systemd, which means it will be automatically launched by starting taosd service command `systemctl start taosd` and be stopped by exiting taosd service command `systemctl stop taosd`. You can also start taosAdapter by `systemctl start taosadapter` and stop taosAdapter by `systemctl stop taosadapter` too. Start/stop taosAdapter will not affect taosd service.

You can download TDengine (taosAdapter be included in v2.3.0.0 and above version) from the (official website)[https://taosdata.com/en/all-downloads/].

## Build taosAdapter

We strongly recommend to deploy taosAdapter with TDengine server and install taosAdapter with official TDengine installation package. If you want to debug or contribute to taosAdapter, you can build it seperately too.

### Setup golang environment

taosAdapter is developed by Go language. Please refer to golang [official documentation](https://go.dev/learn/) for golang environment setup.

Please use golang version 1.14+. For the user in China, we recommend using a proxy to accelerate package downloading.
```
go env -w GO111MODULE=on
go env -w GOPROXY=https://goproxy.cn,direct
```

## Build taosAdapter as a component of TDengine
taosAdapter source code is hosted as a stand-alone repository and also is part of TDengine as a submodule. You can download TDengine source code and build both of them. Following are steps:
```
git clone https://github.com/taosdata/TDengine
cd TDengine
git submodule update --init --recursive
mkdir debug
cd debug
cmake .. -DBUILD_HTTP=false
make
sudo make install
```

Once make install is done, taosAdapter and its systemd service file be installed to the system with the TDengine server. You can use `sudo systemctl start taosd` and `sudo systemclt stop taosd` to launch both of them.

##  Build stand-alone taosAdapter
taosAdapter can be built as a stand-alone application too if you already deployed TDengine server v2.3.0.0 or an above version.

### Install TDengine server or client installation package
Please download the TDengine server or client installation package from the [official website](https://www.taosdata.com/en/all-downloads/).

### Build taosAdapter
```
git clone https://github.com/taosdata/taosadapter
cd taosadapter
go build
```

Then you should find taosAdapter binary executable file in the working directory. You need to copy the systemd file `taosadapter.service` to /etc/systemd/system and copy executable taosAdapter binary file to a place the Linux $PATH environment variable defined.


## Function

* Compatible with RESTful interface.  
  [https://www.taosdata.com/cn/documentation/connector#restful](https://www.taosdata.com/cn/documentation/connector#restful)
* Compatible with InfluxDB v1 write interface.  
  [https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/](https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/)
* Compatible with opentsdb JSON and telnet format writing.  
  * <http://opentsdb.net/docs/build/html/api_http/put.html>
  * <http://opentsdb.net/docs/build/html/api_telnet/put.html>
* Seamless connection with collectd.
    collecd is a system statistics collection daemon. Pleae visit [https://collectd.org/](https://collectd.org/)for detail.
* Seamless connection with StatsD. 
    StatsD is a daemon for easy but powerful stats aggregation. Please visit [https://github.com/statsd/statsd](https://github.com/statsd/statsd) for detail.
* Seamless connection with icinga2.
    icinga2 is an agent to collect check result metrics and performance data. Please visit [https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer](https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer) for detail.
* Seamless connection with tcollector.
    TCollector is a client-side process that gathers data from local collectors and pushes the data to OpenTSDB. Please visit [http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html](http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html) for detail.
* Seamless connection with node_exporter.
    NodeExporter is an exporter software for machine metrics. Please visit [https://github.com/prometheus/node_exporter](https://github.com/prometheus/node_exporter) for detail.
* Support Prometheus remote_read and remote_write
  remote_read and remote_write are Prometheus data read-write separation cluster solutions. Please visit [https://prometheus.io/blog/2019/10/10/remote-read-meets-streaming/#remote-apis](https://prometheus.io/blog/2019/10/10/remote-read-meets-streaming/#remote-apis) for detail.


## Interface

### TDengine RESTful interface
You can use any http client to access the RESTful interface address "http://<fqdn>:6041/<APIEndPoint>" to insert to or query from TDengine. Please refer to the [official documentation](https://www.taosdata.com/cn/documentation/connector#restful) for detail. The end point could be following:
```
/rest/sql
/rest/sqlt
/rest/sqlutc
```

### InfluxDB
You can use any http client to access the RESTful interface address "http://<fqdn>:6041/<APIEndPoint>" to insert InfluxDB compatible protocol data to TDengine. The end point is:
```
/influxdb/v1/write
```

Support following InfluxDB query parameters:
* `db` Specify the necessary parameters for the database
* `precision` time precision non-essential parameter
* `u` user non-essential parameters
* `p` password Optional parameter

Note: There is currently not supported token authentication in InfluxDB only supports Basic authentication and query parameter authentication.

### OpenTSDB
You can use any http client to access the RESTful interface address "http://<fqdn>:6041/<APIEndPoint>" to insert OpenTSDB compatible protocol data to TDengine. The end point is:
```
/opentsdb/v1/put/json/:db
/opentsdb/v1/put/telnet/:db
```

### collectd

#### direct collection

Modify the collectd configuration `/etc/collectd/collectd.conf`. taosAdapter uses 6045 for collectd direct collection data write by default.

```
LoadPlugin network
<Plugin network>
         Server "127.0.0.1" "6045"
</Plugin>
```

#### tsdb writer

Modify the collectd configuration `/etc/collectd/collectd.conf`. taosAdapter uses 6047 for collectd tsdb write by default.

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

modify the configuration file `path_to_statsd/config.js`

* > `backends` add `"./backends/repeater"`
* > `repeater` add `{ host:'host to taosAdapter', port: 6044}`

An example configuration file as below:

```
{
port: 8125
, backends: ["./backends/repeater"]
, repeater: [{ host: '127.0.0.1', port: 6044}]
}
```

### icinga2 OpenTSDB writer

Use icinga2 to collect check result metrics and performance data

* Follow the doc to enable
  opentsdb-writer [https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer](https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer)
* Enable taosAdapter configuration `opentsdb_telnet.enable`
* Modify the configuration file `/etc/icinga2/features-enabled/opentsdb.conf`

```
object OpenTsdbWriter "opentsdb" {
  host = "host to taosAdapter"
  port = 6048
}
```

### TCollector

TCollector is a client-side process that gathers data from local collectors and pushes the data to OpenTSDB. You run it on all your hosts, and it does the work of sending each host’s data to the TSD (OpenTSDB backend process).

* Enable taosAdapter configuration `opentsdb_telnet.enable`
* Modify the TCollector configuration file, modify the OpenTSDB host to the host where taosAdapter is deployed, and modify the port to 6049

### node_exporter

Prometheus exporter for hardware and OS metrics exposed by *NIX kernels

* Enable taosAdapter configuration `node_exporter.enable`
* Set the relevant configuration of node_exporter
* Restart taosAdapter

### prometheus

Remote_read and remote_write are cluster schemes for Prometheus data read-write separation.
Just use the REMOTE_READ and REMOTE_WRITE URL to point to the URL corresponding to Taosadapter to use Basic authentication.
* Remote_read url: http://host_to_taosadapter:port (default 6041) /prometheus/v1/remote_read/:db
* Remote_write url: http://host_to_taosadapter:port (default 6041) /Prometheus/v1/remote_write/:db

Basic verification:

* Username: TDengine connection username
* Password: TDengine connection password

Example Prometheus.yml is as follows:

```yaml
remote_write:
  - url: "http://localhost:6041/prometheus/v1/remote_write/prometheus_data"
    basic_auth:
      username: root
      password: taosdata
 
remote_read:
  - url: "http://localhost:6041/prometheus/v1/remote_read/prometheus_data"
    basic_auth:
      username: root
      password: taosdata
    remote_timeout: 10s
    read_recent: true
```


## Configuration

Support command line parameters, environment variables, and configuration files  
`Command-line parameters take precedence over environment variables take precedence over configuration files`
The command line usage is arg=val such as `taosadapter -p=30000 --debug=true`

```shell
Usage of taosAdapter:
      --collectd.db string                           collectd db name. Env "TAOS_ADAPTER_COLLECTD_DB" (default "collectd")
      --collectd.enable                              enable collectd. Env "TAOS_ADAPTER_COLLECTD_ENABLE" (default true)
      --collectd.password string                     collectd password. Env "TAOS_ADAPTER_COLLECTD_PASSWORD" (default "taosdata")
      --collectd.port int                            collectd server port. Env "TAOS_ADAPTER_COLLECTD_PORT" (default 6045)
      --collectd.user string                         collectd user. Env "TAOS_ADAPTER_COLLECTD_USER" (default "root")
      --collectd.worker int                          collectd write worker. Env "TAOS_ADAPTER_COLLECTD_WORKER" (default 10)
  -c, --config string                                config path default /etc/taos/taosadapter.toml
      --cors.allowAllOrigins                         cors allow all origins. Env "TAOS_ADAPTER_CORS_ALLOW_ALL_ORIGINS" (default true)
      --cors.allowCredentials                        cors allow credentials. Env "TAOS_ADAPTER_CORS_ALLOW_Credentials"
      --cors.allowHeaders stringArray                cors allow HEADERS. Env "TAOS_ADAPTER_ALLOW_HEADERS"
      --cors.allowOrigins stringArray                cors allow origins. Env "TAOS_ADAPTER_ALLOW_ORIGINS"
      --cors.allowWebSockets                         cors allow WebSockets. Env "TAOS_ADAPTER_CORS_ALLOW_WebSockets"
      --cors.exposeHeaders stringArray               cors expose headers. Env "TAOS_ADAPTER_Expose_Headers"
      --debug                                        enable debug mode. Env "TAOS_ADAPTER_DEBUG"
      --help                                         Print this help message and exit
      --influxdb.enable                              enable influxdb. Env "TAOS_ADAPTER_INFLUXDB_ENABLE" (default true)
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
      --pool.idleTimeout duration                    Set idle connection timeout. Env "TAOS_ADAPTER_POOL_IDLE_TIMEOUT" (default 1h0m0s)
      --pool.maxConnect int                          max connections to taosd. Env "TAOS_ADAPTER_POOL_MAX_CONNECT" (default 4000)
      --pool.maxIdle int                             max idle connections to taosd. Env "TAOS_ADAPTER_POOL_MAX_IDLE" (default 4000)
  -P, --port int                                     http port. Env "TAOS_ADAPTER_PORT" (default 6041)
      --prometheus.enable                            enable prometheus. Env "TAOS_ADAPTER_PROMETHEUS_ENABLE" (default true)
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

Note:
If you support users using the web browser to access the interfaces, please configure the following CORS parameters according to your practical network setting:

    AllowAllOrigins
    AllowOrigins
    AllowHeaders
    ExposeHeaders
    AllowCredentials
    AllowWebSockets

If not, you don't need to configure them.

Please visit the webpage [https://www.w3.org/wiki/CORS_Enabled](https://www.w3.org/wiki/CORS_Enabled) or [https://developer.mozilla.org/zh-CN/docs/Web/HTTP/CORS](https://developer.mozilla.org/zh-CN/docs/Web/HTTP/CORS) for the detailed CORS protocol.


For the default configuration file, see [example/config/taosadapter.toml](https://github.com/taosdata/taosadapter/blob/develop/example/config/taosadapter.toml)

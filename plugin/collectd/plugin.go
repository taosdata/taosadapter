package collectd

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/parsers/collectd"
	"github.com/influxdata/telegraf/plugins/serializers/influx"
	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/db/commonpool"
	"github.com/taosdata/taosadapter/log"
	"github.com/taosdata/taosadapter/monitor"
	"github.com/taosdata/taosadapter/plugin"
	"github.com/taosdata/taosadapter/schemaless/inserter"
)

var logger = log.GetLogger("collectd")

type Plugin struct {
	conf       Config
	conn       net.PacketConn
	serializer *influx.Serializer
	parser     *collectd.CollectdParser
	metricChan chan []telegraf.Metric
	closeChan  chan struct{}
}

func (p *Plugin) Init(_ gin.IRouter) error {
	p.conf.setValue()
	if !p.conf.Enable {
		logger.Info("collectd disabled")
		return nil
	}
	p.conf.Port = viper.GetInt("collectd.port")
	p.conf.DB = viper.GetString("collectd.db")
	p.conf.User = viper.GetString("collectd.user")
	p.conf.Password = viper.GetString("collectd.password")
	p.parser = &collectd.CollectdParser{
		ParseMultiValue: "split",
	}
	return nil
}

func (p *Plugin) Start() error {
	if !p.conf.Enable {
		return nil
	}
	if p.conn != nil {
		err := p.conn.Close()
		if err != nil {
			return err
		}
	}
	conn, err := udpListen("udp", fmt.Sprintf(":%d", p.conf.Port))
	if err != nil {
		return err
	}
	p.closeChan = make(chan struct{})
	p.metricChan = make(chan []telegraf.Metric, 2*p.conf.Worker)
	for i := 0; i < p.conf.Worker; i++ {
		go func() {
			serializer := influx.NewSerializer()
			for {
				select {
				case metric := <-p.metricChan:
					p.HandleMetrics(serializer, metric)
				case <-p.closeChan:
					return
				}
			}
		}()
	}
	p.conn = conn
	go p.listen()
	return nil
}

func (p *Plugin) Stop() error {
	if !p.conf.Enable {
		return nil
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	close(p.closeChan)
	return nil
}

func (p *Plugin) String() string {
	return "collectd"
}

func (p *Plugin) Version() string {
	return "v1"
}

func (p *Plugin) HandleMetrics(serializer *influx.Serializer, metrics []telegraf.Metric) {
	if len(metrics) == 0 {
		return
	}

	for _, metric := range metrics {
		if metric.Time().IsZero() {
			metric.SetTime(time.Now())
		}
	}
	data, err := serializer.SerializeBatch(metrics)
	if err != nil {
		logger.WithError(err).Error("serialize collectd error")
		return
	}
	taosConn, err := commonpool.GetConnection(p.conf.User, p.conf.Password)
	if err != nil {
		logger.WithError(err).Errorln("connect taosd error")
		return
	}
	defer func() {
		putErr := taosConn.Put()
		if putErr != nil {
			logger.WithError(putErr).Errorln("taos connect pool put error")
		}
	}()
	start := time.Now()
	logger.Debugln(start, "insert lines", string(data))
	result, err := inserter.InsertInfluxdb(taosConn.TaosConnection, data, p.conf.DB, "ns")
	logger.Debugln("insert lines finish cost:", time.Now().Sub(start), string(data))
	if err != nil || result.FailCount != 0 {
		logger.WithError(err).WithField("result", result).Errorln("insert lines error", string(data))
		return
	}
}

func (p *Plugin) listen() {
	buf := make([]byte, 64*1024) // 64kb - maximum size of IP packet
	for {
		n, _, err := p.conn.ReadFrom(buf)
		if monitor.AllPaused() {
			continue
		}
		if err != nil {
			if !strings.HasSuffix(err.Error(), ": use of closed network connection") {
				logger.Error(err.Error())
			}
			break
		}

		metrics, err := p.parser.Parse(buf[:n])
		if err != nil {
			logger.Errorf("Unable to parse incoming packet: %s", err.Error())
			continue
		}
		p.metricChan <- metrics

	}
}

func udpListen(network string, address string) (net.PacketConn, error) {
	switch network {
	case "udp", "udp4", "udp6":
		var addr *net.UDPAddr
		var err error
		var ifi *net.Interface
		if spl := strings.SplitN(address, "%", 2); len(spl) == 2 {
			address = spl[0]
			ifi, err = net.InterfaceByName(spl[1])
			if err != nil {
				return nil, err
			}
		}
		addr, err = net.ResolveUDPAddr(network, address)
		if err != nil {
			return nil, err
		}
		if addr.IP.IsMulticast() {
			return net.ListenMulticastUDP(network, ifi, addr)
		}
		return net.ListenUDP(network, addr)
	}
	return net.ListenPacket(network, address)
}

func init() {
	plugin.Register(&Plugin{})
}

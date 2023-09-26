package statsd

import (
	"fmt"
	"net"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/agent"
	"github.com/influxdata/telegraf/plugins/serializers/influx"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/plugin"
	"github.com/taosdata/taosadapter/v3/schemaless/inserter"
)

var logger = log.GetLogger("statsd")

type Plugin struct {
	conf       Config
	ac         telegraf.Accumulator
	input      *Statsd
	closeChan  chan struct{}
	metricChan chan telegraf.Metric
}

func (p *Plugin) Init(_ gin.IRouter) error {
	p.conf.setValue()
	if !p.conf.Enable {
		logger.Info("statsd disabled")
		return nil
	}
	return nil
}

func (p *Plugin) Start() error {
	if !p.conf.Enable {
		return nil
	}
	p.closeChan = make(chan struct{})
	p.metricChan = make(chan telegraf.Metric, 2*p.conf.Worker)
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
	p.input = &Statsd{
		User:                   p.conf.User,
		Password:               p.conf.Password,
		Protocol:               p.conf.Protocol,
		ServiceAddress:         fmt.Sprintf(":%d", p.conf.Port),
		MaxTCPConnections:      p.conf.MaxTCPConnections,
		TCPKeepAlive:           p.conf.TCPKeepAlive,
		AllowedPendingMessages: p.conf.AllowedPendingMessages,
		DeleteCounters:         p.conf.DeleteCounters,
		DeleteGauges:           p.conf.DeleteGauges,
		DeleteSets:             p.conf.DeleteSets,
		DeleteTimings:          p.conf.DeleteTimings,
		Log:                    logger,
	}
	p.ac = agent.NewAccumulator(&MetricMaker{logger: logger}, p.metricChan)
	err := p.input.Start(p.ac)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(p.conf.GatherInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				if monitor.AllPaused() {
					return
				}
				err := p.input.Gather(p.ac)
				if err != nil {
					logger.WithError(err).Error("gather error")
				}
			case <-p.closeChan:
				ticker.Stop()
				ticker = nil
				return
			}
		}

	}()
	return nil
}

func (p *Plugin) Stop() error {
	if !p.conf.Enable {
		return nil
	}
	p.input.Stop()
	close(p.closeChan)
	return nil
}

func (p *Plugin) String() string {
	return "statsd"
}

func (p *Plugin) Version() string {
	return "v1"
}

var localhost = net.IPv4(127, 0, 0, 1)

func (p *Plugin) HandleMetrics(serializer *influx.Serializer, metric telegraf.Metric) {
	data, err := serializer.Serialize(metric)
	if err != nil {
		logger.WithError(err).Error("serialize statsd error")
		return
	}
	taosConn, err := commonpool.GetConnection(p.conf.User, p.conf.Password, localhost)
	if err != nil {
		logger.WithError(err).Errorln("connect server error")
		return
	}
	defer func() {
		putErr := taosConn.Put()
		if putErr != nil {
			logger.WithError(putErr).Errorln("connect pool put error")
		}
	}()

	start := time.Now()
	logger.Debugln(start, "insert line", string(data))
	err = inserter.InsertInfluxdb(taosConn.TaosConnection, data, p.conf.DB, "ns", p.conf.TTL, 0)
	logger.Debugln("insert line finish cost:", time.Since(start), string(data))
	if err != nil {
		logger.WithError(err).Errorln("insert lines error", string(data))
		return
	}
}

type MetricMaker struct {
	logger logrus.FieldLogger
}

func (m *MetricMaker) LogName() string {
	return "metric"
}

func (m *MetricMaker) MakeMetric(metric telegraf.Metric) telegraf.Metric {
	return metric
}

func (m *MetricMaker) Log() telegraf.Logger {
	return m.logger
}

func init() {
	plugin.Register(&Plugin{})
}

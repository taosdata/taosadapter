package opentsdbtelnet

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/db/commonpool"
	"github.com/taosdata/taosadapter/log"
	"github.com/taosdata/taosadapter/monitor"
	"github.com/taosdata/taosadapter/plugin"
	"github.com/taosdata/taosadapter/schemaless/inserter"
)

var logger = log.GetLogger("opentsdb_telnet")
var versionCommand = "version"

type Plugin struct {
	conf         Config
	done         chan struct{}
	wg           sync.WaitGroup
	TCPListeners []*TCPListener
}

type TCPListener struct {
	plugin    *Plugin
	index     int
	listener  *net.TCPListener
	id        uint64
	connList  map[uint64]*net.TCPConn
	accept    chan bool
	keepalive bool
	cleanup   sync.Mutex
	done      chan struct{}
	wg        sync.WaitGroup
}

func NewTCPListener(plugin *Plugin, index int, listener *net.TCPListener, maxConnections int, keepalive bool) *TCPListener {
	l := &TCPListener{plugin: plugin, index: index, listener: listener, keepalive: keepalive}
	l.done = make(chan struct{})
	l.connList = make(map[uint64]*net.TCPConn)
	l.accept = make(chan bool, maxConnections)
	for i := 0; i < maxConnections; i++ {
		l.accept <- true
	}
	return l
}

func (l *TCPListener) start() error {
	for {
		select {
		case <-l.done:
			return nil
		default:
			// Accept connection:
			conn, err := l.listener.AcceptTCP()
			if err != nil {
				return err
			}

			if l.keepalive {
				if err = conn.SetKeepAlive(true); err != nil {
					return err
				}
			}

			select {
			case <-l.accept:
				l.wg.Add(1)
				id := atomic.AddUint64(&l.id, 1)
				l.remember(id, conn)
				go l.handler(conn, id)
			default:
				l.refuser(conn)
			}
		}
	}
}

func (l *TCPListener) forget(id uint64) {
	l.cleanup.Lock()
	defer l.cleanup.Unlock()
	delete(l.connList, id)
}

func (l *TCPListener) remember(id uint64, conn *net.TCPConn) {
	l.cleanup.Lock()
	defer l.cleanup.Unlock()
	l.connList[id] = conn
}

func (l *TCPListener) refuser(conn *net.TCPConn) {
	_ = conn.Close()
	logger.Infof("Refused TCP Connection from %s", conn.RemoteAddr())
	logger.Warn("Maximum TCP Connections reached")
}

func (l *TCPListener) handler(conn *net.TCPConn, id uint64) {
	defer func() {
		l.wg.Done()
		conn.Close()
		l.accept <- true
		l.forget(id)
	}()
	for {
		select {
		case <-l.done:
			return
		default:
			b := bufio.NewReader(conn)
			for {
				s, err := b.ReadString('\n')
				if err != nil {
					if err != io.EOF {
						logger.WithError(err).Error("conn read")
					}
					return
				}
				if monitor.AllPaused() {
					continue
				}
				if len(s) == 0 {
					continue
				}
				s = s[:len(s)-1]
				if s == versionCommand {
					conn.Write([]byte{'1'})
					continue
				} else {
					l.plugin.handleData(l.index, s)
				}
			}
		}
	}
}

func (l *TCPListener) stop() error {
	close(l.done)
	l.listener.Close()
	var tcpConnList []*net.TCPConn
	l.cleanup.Lock()
	for _, conn := range l.connList {
		tcpConnList = append(tcpConnList, conn)
	}
	l.cleanup.Unlock()
	for _, conn := range tcpConnList {
		conn.Close()
	}
	l.wg.Wait()
	return nil
}

func (p *Plugin) Init(_ gin.IRouter) error {
	p.conf.setValue()
	if !p.conf.Enable {
		logger.Info("opentsdb_telnet disabled")
		return nil
	}
	if len(p.conf.PortList) == 0 {
		return errors.New("no port set")
	}
	if len(p.conf.PortList) != len(p.conf.DBList) {
		return errors.New("the number of dbs is not equal ports")
	}
	return nil
}

func (p *Plugin) Start() error {
	if !p.conf.Enable {
		return nil
	}
	p.TCPListeners = make([]*TCPListener, len(p.conf.PortList))
	p.done = make(chan struct{})
	for i := 0; i < len(p.conf.PortList); i++ {
		err := p.tcp(p.conf.PortList[i], i)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Plugin) Stop() error {
	if !p.conf.Enable {
		return nil
	}
	if p.done != nil {
		close(p.done)
	}
	for _, listener := range p.TCPListeners {
		listener.stop()
	}
	p.wg.Wait()
	return nil
}

func (p *Plugin) String() string {
	return "opentsdb_telnet"
}

func (p *Plugin) Version() string {
	return "v1"
}

func (p *Plugin) tcp(port int, index int) error {
	address, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	listener, err := net.ListenTCP("tcp", address)
	if err != nil {
		return err
	}

	logger.Infof("TCP listening on %q", listener.Addr().String())
	tcpListener := NewTCPListener(p, index, listener, p.conf.MaxTCPConnections, p.conf.TCPKeepAlive)
	p.TCPListeners[index] = tcpListener
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		if err := tcpListener.start(); err != nil {
			select {
			case <-tcpListener.done:
				return
			default:
				logger.WithError(err).Panic()
			}
		}
	}()
	return nil
}

func (p *Plugin) handleData(index int, line string) {
	taosConn, err := commonpool.GetConnection(p.conf.User, p.conf.Password)
	if err != nil {
		logger.WithError(err).Error("connect taosd error")
		return
	}
	defer func() {
		putErr := taosConn.Put()
		if putErr != nil {
			logger.WithError(putErr).Errorln("taos connect pool put error")
		}
	}()
	var start time.Time
	if logger.Logger.IsLevelEnabled(logrus.DebugLevel) {
		start = time.Now()
	}
	logger.Debugln(start, " insert telnet payload ", line)
	err = inserter.InsertOpentsdbTelnet(taosConn.TaosConnection, line, p.conf.DBList[index])
	if err != nil {
		logger.WithError(err).Errorln("insert telnet payload error :", line)
	}
	logger.Debug("insert telnet payload cost:", time.Now().Sub(start))
}

func init() {
	plugin.Register(&Plugin{})
}

package opentsdbtelnet

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/db/commonpool"
	"github.com/taosdata/taosadapter/log"
	"github.com/taosdata/taosadapter/plugin"
	"github.com/taosdata/taosadapter/schemaless/inserter"
)

var logger = log.GetLogger("opentsdb_telnet")
var versionCommand = "version"

type telnetMessage struct {
	body  []string
	index int
}

type Plugin struct {
	conf Config
	done chan struct{}
	in   chan *telnetMessage
	wg   sync.WaitGroup

	TCPListeners []*TCPListener
}

type TCPListener struct {
	index     int
	listener  *net.TCPListener
	id        uint64
	connList  map[uint64]*net.TCPConn
	accept    chan bool
	in        chan<- *telnetMessage
	keepalive bool
	cleanup   sync.Mutex
	done      chan struct{}
	wg        sync.WaitGroup
}

func NewTCPListener(index int, listener *net.TCPListener, in chan<- *telnetMessage, maxConnections int, keepalive bool) *TCPListener {
	l := &TCPListener{listener: listener, in: in, index: index, keepalive: keepalive}
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
	buffer := make([]byte, 0, 1024)
	d := make([]byte, 1024)
	for {
		select {
		case <-l.done:
			return
		default:
			n, err := conn.Read(d)
			if err != nil {
				if err != io.EOF {
					logger.WithError(err).Error("conn read")
				}
				return
			}
			if n == 0 {
				continue
			}
			buffer = append(buffer, d[:n]...)
			customIndex := 0
			for i := len(buffer) - 1; i > 0; i-- {
				if buffer[i] == '\n' {
					customIndex = i
					break
				}
			}

			if customIndex > 0 {
				data := make([]byte, customIndex)
				copy(data, buffer[:customIndex])
				buffer = buffer[customIndex+1:]
				lines := strings.Split(string(data), "\n")
				insertLines := make([]string, 0, len(lines))
				for _, line := range lines {
					if len(line) == 0 {
						continue
					}
					if line[len(line)-1] == '\r' {
						line = line[:len(line)-1]
					}
					if len(line) == 0 {
						continue
					}
					if line == versionCommand {
						conn.Write([]byte{'1'})
						continue
					} else {
						insertLines = append(insertLines, line)
					}
				}
				if len(insertLines) == 0 {
					continue
				}
				select {
				case l.in <- &telnetMessage{
					body:  insertLines,
					index: l.index,
				}:
				default:
					logger.Errorln("can not handle more message so far. increase opentsdb_telnet.worker")
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
	p.in = make(chan *telnetMessage, p.conf.Worker*2)
	p.done = make(chan struct{})
	for i := 0; i < p.conf.Worker; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.parser()
		}()
	}

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
	close(p.in)
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
	tcpListener := NewTCPListener(index, listener, p.in, p.conf.MaxTCPConnections, p.conf.TCPKeepAlive)
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

func (p *Plugin) parser() {
	for {
		select {
		case <-p.done:
			return
		case in, ok := <-p.in:
			if !ok {
				return
			}
			p.handleData(in)
		}
	}
}

func (p *Plugin) handleData(message *telnetMessage) {
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
	for _, line := range message.body {
		logger.Debugln(start, " insert telnet payload ", line)
		err = inserter.InsertOpentsdbTelnet(taosConn.TaosConnection, line, p.conf.DBList[message.index])
		if err != nil {
			logger.WithError(err).Errorln("insert telnet payload error :", line)
		}
		logger.Debug("insert telnet payload cost:", time.Now().Sub(start))
	}
}

func init() {
	plugin.Register(&Plugin{})
}

package tcp

import (
	"fmt"
	"net"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/plugin"
)

var logger = log.GetLogger("PLG").WithField("mod", "tcp")

type Server struct {
	conf     Config
	listener net.TCPListener
	stopSig  chan struct{}
	stopped  bool
	rwlock   sync.RWMutex
	clients  map[int64]*Connection
}

func NewServer() *Server {
	return &Server{
		stopSig: make(chan struct{}),
		clients: make(map[int64]*Connection),
	}
}

func (s *Server) Init(r gin.IRouter) error {
	s.conf.setValue()
	if !s.conf.Enable {
		logger.Info("tcp server disabled")
	}
	return nil
}

func (s *Server) Start() error {
	if !s.conf.Enable {
		return nil
	}
	addr := fmt.Sprintf("%s:%d", s.conf.Host, s.conf.Port)
	tcpProto := "tcp"
	if s.conf.EnableTCP4Only {
		tcpProto = "tcp4"
	}
	ln, err := net.Listen(tcpProto, addr)
	if err != nil {
		logger.Errorf("tcp server listen error: %s", err)
		return fmt.Errorf("tcp server start error: %s", err)
	}
	address := ln.Addr().(*net.TCPAddr).String()
	logger.Infof("tcp server started at %s", address)
	s.listener = *ln.(*net.TCPListener)
	go func() {
		errChan := make(chan error)
		s.startListener(ln, errChan)
		<-errChan
	}()
	return nil
}

func (s *Server) startListener(listener net.Listener, errChan chan error) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok {
				if opErr.Err.Error() == "use of closed network connection" {
					if s.stopped {
						errChan <- nil
						return
					}
				}
			}
			logger.Errorf("tcp server accept error: %s", err)
			errChan <- err
			return
		}
		logger.Debugf("tcp server accepted connection from %s", conn.RemoteAddr())
		connection := newConnection(s, conn)
		s.onConn(connection)
		go connection.run()
	}
}

func (s *Server) onConn(conn *Connection) {
	s.rwlock.Lock()
	s.clients[conn.sessionID] = conn
	s.rwlock.Unlock()
}

func (s *Server) OnCloseConn(conn *Connection) {
	s.rwlock.Lock()
	delete(s.clients, conn.sessionID)
	s.rwlock.Unlock()
}

func (s *Server) Stop() error {
	return nil
}

func (s *Server) String() string {
	return "tcp"
}

func (s *Server) Version() string {
	return "v1"
}

func init() {
	plugin.Register(NewServer())
}

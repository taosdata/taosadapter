package tcp

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/generator"
)

type Connection struct {
	conn                  unsafe.Pointer
	server                *Server
	sessionID             int64
	tcpConn               net.Conn
	logger                *logrus.Entry
	packet                *packet
	once                  sync.Once
	messageQueue          chan []byte
	closeChan             chan struct{}
	closed                uint32
	ip                    net.IP
	ipStr                 string
	whitelistChangeHandle cgo.Handle
	dropUserHandle        cgo.Handle
	dropUserChan          chan struct{}
	whitelistChangeChan   chan int64
	stmts                 *StmtHolder
	sync.Mutex
}

const DefaultReadTimeout = 28800

func newConnection(server *Server, conn net.Conn) *Connection {
	whitelistChangeChan, whitelistChangeHandle := tool.GetRegisterChangeWhiteListHandle()
	dropUserChan, dropUserHandle := tool.GetRegisterDropUserHandle()
	sid := generator.GetSessionID()
	remoteAdd := conn.RemoteAddr().String()
	// get ip
	ipStr, _, err := net.SplitHostPort(remoteAdd)
	if err != nil {
		panic(err)
	}
	ip := net.ParseIP(ipStr)
	return &Connection{
		server:                server,
		sessionID:             sid,
		tcpConn:               conn,
		logger:                logger.WithField(config.SessionIDKey, sid),
		packet:                newPacket(conn, time.Second*DefaultReadTimeout),
		messageQueue:          make(chan []byte, 1),
		stmts:                 NewStmtHolder(),
		ip:                    ip,
		ipStr:                 ipStr,
		closeChan:             make(chan struct{}),
		whitelistChangeChan:   whitelistChangeChan,
		whitelistChangeHandle: whitelistChangeHandle,
		dropUserChan:          dropUserChan,
		dropUserHandle:        dropUserHandle,
	}
}

func (c *Connection) run() {
	defer func() {
		err := c.doClose()
		if err != nil {
			logger.Errorf("close connection error, err: %s", err)
		}
		c.server.OnCloseConn(c)
	}()
	go func() {
		c.doWritePacket()
	}()
	for {
		data, err := c.readPacket()
		c.logger.Debugf("read packet data, err: %s", err)
		if err != nil {
			if err == io.EOF {
				return
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				c.logger.Error("read timeout, close connection")
			} else {
				if !strings.Contains(err.Error(), "use of closed network connection") {
					logger.Warnf("read packet error, close connection, err: %s", err)
				}
			}
			return
		}
		// sequence uint64 8
		// version uint8  1
		// command uint8  1
		// reserved [6]byte 6
		// signal [8]byte 8
		// payload []byte
		if len(data) < 25 {
			c.logger.Errorf("read packet length error, close connection, length: %d", len(data))
			return
		}
		version := data[8]
		if version != 0x01 {
			c.logger.Errorf("read packet version error, close connection, version: %d", version)
			return
		}
		ctx := context.WithValue(context.Background(), wstool.StartTimeKey, time.Now())
		cmd := data[9]
		reqID := binary.LittleEndian.Uint64(data)
		payload := data[24:]

		switch cmd {
		case CmdVersion:
			_, logger := c.getOrGenerateReqID(reqID, cmd)
			c.handleVersion(ctx, reqID, logger)
		case CmdConn:
			_, logger := c.getOrGenerateReqID(reqID, cmd)
			c.handleConn(ctx, reqID, payload, logger, log.IsDebug())
		default:
			panic("")
		}
	}
}

func (c *Connection) doWritePacket() {
	for {
		select {
		case <-c.closeChan:
			return
		case data := <-c.messageQueue:
			if err := c.packet.writePacket(data); err != nil {
				c.logger.Errorf("write packet error, err: %s", err)
				return
			}
		}
	}
}

func (c *Connection) readPacket() ([]byte, error) {
	return c.packet.readPacket()
}

func (c *Connection) writePacket(data []byte) {
	select {
	case <-c.closeChan:
		return
	case c.messageQueue <- data:
		c.logger.Info("write packet to message queue")
	}
}

func (c *Connection) doClose() error {
	var err error
	c.once.Do(func() {
		c.setClosed()
		close(c.closeChan)
		if c.tcpConn != nil {
			err = c.tcpConn.Close()
		}
	})
	return err
}

func (c *Connection) getOrGenerateReqID(reqID uint64, action byte) (uint64, *logrus.Entry) {
	innerReqID := reqID
	if reqID == 0 {
		innerReqID = uint64(generator.GetReqID())
		c.logger.Debugf("reqID is 0, generate a new one:0x%x", innerReqID)
	}
	logger := c.logger.WithFields(logrus.Fields{
		"action":        cmdName[action],
		config.ReqIDKey: innerReqID,
	})
	return innerReqID, logger
}

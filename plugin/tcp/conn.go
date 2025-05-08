package tcp

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/driver/common"
	taoserrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/log"
)

type connRequest struct {
	ReqID    uint64 `json:"req_id"`
	User     string `json:"user"`
	Password string `json:"password"`
	DB       string `json:"db"`
	Mode     *int   `json:"mode"`
	TZ       string `json:"tz"`
	App      string `json:"app"`
	IP       string `json:"ip"`
}

func unmarshalConnRequest(reqID uint64, bytes []byte) (req *connRequest, err error) {
	// version 1
	// userLen uint8
	// user string
	// passwordLen uint8
	// password string
	// dbLen uint8
	// db string
	// set_mode bool
	// mode int32
	// tzLen uint16
	// tz string
	// appLen uint16
	// app string
	// ipLen uint16
	// ip string
	defer func() {
		// recover from panic,if any return error
		if err := recover(); err != nil {
			err = fmt.Errorf("unmarshal conn request error:%v", err)
		}
	}()
	offset := 0
	version := bytes[offset]
	if version != 1 {
		return nil, fmt.Errorf("unexpected version:%d", version)
	}
	offset++
	userLen := int(bytes[offset])
	offset++
	if userLen == 0 {
		return nil, fmt.Errorf("user length is 0")
	}
	user := string(bytes[offset : offset+userLen])
	offset += userLen
	passwordLen := int(bytes[offset])
	if passwordLen == 0 {
		return nil, fmt.Errorf("password length is 0")
	}
	offset++
	password := string(bytes[offset : offset+passwordLen])
	offset += passwordLen
	dbLen := int(bytes[offset])
	offset++
	db := ""
	if dbLen != 0 {
		db = string(bytes[offset : offset+dbLen])
		offset += dbLen
	}
	setMode := bytes[offset]
	offset++
	var mode *int = nil
	if setMode == 1 {
		m := int(binary.LittleEndian.Uint32(bytes[offset : offset+4]))
		mode = &m
		offset += 4
	}
	tzLen := binary.BigEndian.Uint16(bytes[offset : offset+2])
	offset += 2
	tz := ""
	if tzLen != 0 {
		tz = string(bytes[offset : offset+int(tzLen)])
		offset += int(tzLen)
	}
	appLen := binary.BigEndian.Uint16(bytes[offset : offset+2])
	offset += 2
	app := ""
	if appLen != 0 {
		app = string(bytes[offset : offset+int(appLen)])
		offset += int(appLen)
	}
	ipLen := binary.BigEndian.Uint16(bytes[offset : offset+2])
	offset += 2
	ip := ""
	if ipLen != 0 {
		ip = string(bytes[offset : offset+int(ipLen)])
		offset += int(ipLen)
	}

	return &connRequest{
		ReqID:    reqID,
		User:     user,
		Password: password,
		DB:       db,
		Mode:     mode,
		TZ:       tz,
		App:      app,
		IP:       ip,
	}, nil
}

func (c *Connection) handleConn(ctx context.Context, reqID uint64, payload []byte, logger *logrus.Entry, isDebug bool) {
	req, err := unmarshalConnRequest(reqID, payload)
	if err != nil {
		logger.Errorf("unmarshal conn request error:%s", err)
		c.sendErrorResponse(ctx, reqID, 0xffff, err.Error(), CmdConn)
		return
	}
	c.lock(logger, isDebug)
	defer c.Unlock()
	if c.isClosed() {
		logger.Trace("server closed")
		return
	}
	cmd := CmdConn
	if c.conn != nil {
		logger.Trace("duplicate connections")
		c.sendErrorResponse(ctx, req.ReqID, 0xffff, "duplicate connections", cmd)
		//commonErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, "duplicate connections")
		return
	}

	conn, err := syncinterface.TaosConnect("", req.User, req.Password, req.DB, 0, logger, isDebug)

	if err != nil {
		c.handleConnectError(ctx, conn, logger, isDebug, req.ReqID, err, "connect to TDengine error")
		return
	}
	logger.Trace("get whitelist")
	s := log.GetLogNow(isDebug)
	whitelist, err := tool.GetWhitelist(conn)
	logger.Debugf("get whitelist cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		c.handleConnectError(ctx, conn, logger, isDebug, req.ReqID, err, "get whitelist error")
		return
	}
	logger.Tracef("check whitelist, ip:%s, whitelist:%s", c.ipStr, tool.IpNetSliceToString(whitelist))
	valid := tool.CheckWhitelist(whitelist, c.ip)
	if !valid {
		err = errors.New("ip not in whitelist")
		c.handleConnectError(ctx, conn, logger, isDebug, req.ReqID, err, "ip not in whitelist")
		return
	}
	s = log.GetLogNow(isDebug)
	logger.Trace("register whitelist change")
	err = tool.RegisterChangeWhitelist(conn, c.whitelistChangeHandle)
	logger.Debugf("register whitelist change cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		c.handleConnectError(ctx, conn, logger, isDebug, req.ReqID, err, "register whitelist change error")
		return
	}
	s = log.GetLogNow(isDebug)
	logger.Trace("register drop user")
	err = tool.RegisterDropUser(conn, c.dropUserHandle)
	logger.Debugf("register drop user cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		c.handleConnectError(ctx, conn, logger, isDebug, req.ReqID, err, "register drop user error")
		return
	}
	if req.Mode != nil {
		switch *req.Mode {
		case common.TAOS_CONN_MODE_BI:
			// BI mode
			logger.Trace("set connection mode to BI")
			code := wrapper.TaosSetConnMode(conn, common.TAOS_CONN_MODE_BI, 1)
			logger.Trace("set connection mode to BI done")
			if code != 0 {
				c.handleConnectError(ctx, conn, logger, isDebug, req.ReqID, taoserrors.NewError(code, wrapper.TaosErrorStr(nil)), "set connection mode to BI error")
				return
			}
		default:
			err = fmt.Errorf("unexpected mode:%d", *req.Mode)
			c.handleConnectError(ctx, conn, logger, isDebug, req.ReqID, err, err.Error())
			return
		}
	}
	// set connection ip
	clientIP := c.ipStr
	if req.IP != "" {
		clientIP = req.IP
	}
	logger.Tracef("set connection ip, ip:%s", clientIP)
	code := syncinterface.TaosOptionsConnection(conn, common.TSDB_OPTION_CONNECTION_USER_IP, &clientIP, logger, isDebug)
	logger.Trace("set connection ip done")
	if code != 0 {
		c.handleConnectError(ctx, conn, logger, isDebug, req.ReqID, taoserrors.NewError(code, wrapper.TaosErrorStr(nil)), "set connection ip error")
		return
	}
	// set timezone
	if req.TZ != "" {
		logger.Tracef("set timezone, tz:%s", req.TZ)
		code = syncinterface.TaosOptionsConnection(conn, common.TSDB_OPTION_CONNECTION_TIMEZONE, &req.TZ, logger, isDebug)
		logger.Trace("set timezone done")
		if code != 0 {
			c.handleConnectError(ctx, conn, logger, isDebug, req.ReqID, taoserrors.NewError(code, wrapper.TaosErrorStr(nil)), "set timezone error")
			return
		}
	}
	// set connection app
	if req.App != "" {
		logger.Tracef("set app, app:%s", req.App)
		code = syncinterface.TaosOptionsConnection(conn, common.TSDB_OPTION_CONNECTION_USER_APP, &req.App, logger, isDebug)
		logger.Trace("set app done")
		if code != 0 {
			c.handleConnectError(ctx, conn, logger, isDebug, req.ReqID, taoserrors.NewError(code, wrapper.TaosErrorStr(nil)), "set app error")
			return
		}
	}
	c.conn = conn
	logger.Trace("start wait signal goroutine")
	go c.waitSignal(c.logger)
	c.sendCommonSuccessResponse(ctx, req.ReqID, cmd)
}

func (c *Connection) handleConnectError(ctx context.Context, conn unsafe.Pointer, logger *logrus.Entry, isDebug bool, reqID uint64, err error, errorExt string) {
	var code int
	var errStr string
	taosError, ok := err.(*taoserrors.TaosError)
	if ok {
		code = int(taosError.Code)
		errStr = taosError.ErrStr
	} else {
		code = 0xffff
		errStr = err.Error()
	}
	logger.Errorf("%s, code:%d, message:%s", errorExt, code, errStr)
	syncinterface.TaosClose(conn, logger, isDebug)
	c.sendErrorResponse(ctx, reqID, uint32(code), errStr, CmdConn)
}

func (c *Connection) lock(logger *logrus.Entry, isDebug bool) {
	logger.Trace("get handler lock")
	s := log.GetLogNow(isDebug)
	c.Lock()
	logger.Debugf("get handler lock cost:%s", log.GetLogDuration(isDebug, s))
}

func (c *Connection) isClosed() bool {
	return atomic.LoadUint32(&c.closed) == 1
}

func (c *Connection) setClosed() {
	atomic.StoreUint32(&c.closed, 1)
}

func (c *Connection) waitSignal(logger *logrus.Entry) {
	defer func() {
		logger.Trace("exit wait signal")
		tool.PutRegisterChangeWhiteListHandle(c.whitelistChangeHandle)
		tool.PutRegisterDropUserHandle(c.dropUserHandle)
	}()
	for {
		select {
		case <-c.dropUserChan:
			logger.Info("get drop user signal")
			isDebug := log.IsDebug()
			c.lock(logger, isDebug)
			if c.isClosed() {
				logger.Trace("server closed")
				c.Unlock()
				return
			}
			logger.Info("user dropped, close connection")
			c.signalExit(logger, isDebug)
			return
		case <-c.whitelistChangeChan:
			logger.Info("get whitelist change signal")
			isDebug := log.IsDebug()
			c.lock(logger, isDebug)
			if c.isClosed() {
				logger.Trace("server closed")
				c.Unlock()
				return
			}
			logger.Trace("get whitelist")
			whitelist, err := tool.GetWhitelist(c.conn)
			if err != nil {
				logger.Errorf("get whitelist error, close connection, err:%s", err)
				c.signalExit(logger, isDebug)
				return
			}
			logger.Tracef("check whitelist, ip:%s, whitelist:%s", c.ipStr, tool.IpNetSliceToString(whitelist))
			valid := tool.CheckWhitelist(whitelist, c.ip)
			if !valid {
				logger.Errorf("ip not in whitelist! close connection, ip:%s, whitelist:%s", c.ipStr, tool.IpNetSliceToString(whitelist))
				c.signalExit(logger, isDebug)
				return
			}
			c.Unlock()
		case <-c.closeChan:
			return
		}
	}
}

func (c *Connection) signalExit(logger *logrus.Entry, isDebug bool) {
	logger.Trace("close session")
	err := c.doClose()
	if err != nil {
		logger.Errorf("close session error: %s", err)
	}
	logger.Trace("close session done")
}

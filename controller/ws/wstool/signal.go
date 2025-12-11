package wstool

import (
	"net"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/log"
)

type WSHandler interface {
	Lock(logger *logrus.Entry, isDebug bool)
	Unlock()
	UnlockAndExit(logger *logrus.Entry, isDebug bool)
	IsClosed() bool
}

func WaitSignal(h WSHandler, conn unsafe.Pointer, ip net.IP, ipStr string, whitelistChangeHandle cgo.Handle, dropUserHandle cgo.Handle, whitelistChangeChan chan int64, dropUserChan chan struct{}, exit chan struct{}, logger *logrus.Entry) {
	defer func() {
		logger.Trace("exit wait signal")
		tool.PutRegisterChangeWhiteListHandle(whitelistChangeHandle)
		tool.PutRegisterDropUserHandle(dropUserHandle)
	}()
	for {
		select {
		case <-dropUserChan:
			logger.Trace("get drop user signal")
			isDebug := log.IsDebug()
			h.Lock(logger, isDebug)
			if h.IsClosed() {
				logger.Trace("server closed")
				h.Unlock()
				return
			}
			logger.Trace("user dropped, close connection")
			h.UnlockAndExit(logger, isDebug)
			return
		case <-whitelistChangeChan:
			logger.Trace("get whitelist change signal")
			isDebug := log.IsDebug()
			h.Lock(logger, isDebug)
			if h.IsClosed() {
				logger.Trace("server closed")
				h.Unlock()
				return
			}
			logger.Trace("get whitelist")
			allowlist, blocklist, err := tool.GetWhitelist(conn, logger, isDebug)
			if err != nil {
				logger.Errorf("get whitelist error, close connection, err:%s", err)
				h.UnlockAndExit(logger, isDebug)
				return
			}
			allowlistStr := tool.IpNetSliceToString(allowlist)
			blocklistStr := tool.IpNetSliceToString(blocklist)
			logger.Tracef("check whitelist, ip:%s, allowlist:%s, blocklist:%s", ipStr, allowlistStr, blocklistStr)
			valid := tool.CheckWhitelist(allowlist, blocklist, ip)
			if !valid {
				logger.Errorf("ip not in whitelist! close connection, ip:%s, allowlist:%s, blocklist:%s", ipStr, allowlistStr, blocklistStr)
				h.UnlockAndExit(logger, isDebug)
				return
			}
			h.Unlock()
		case <-exit:
			return
		}
	}
}

package tool

import (
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/thread"
)

func FreeResult(res unsafe.Pointer, logger *logrus.Entry, isDebug bool) {
	if res == nil {
		logger.Traceln("result is nil")
		return
	}
	logger.Traceln("get thread lock for free result")
	s := log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("get thread lock for free result cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	logger.Traceln("start free result")
	wrapper.TaosFreeResult(res)
	logger.Debugln("free result cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
}

func TaosClose(conn unsafe.Pointer, logger *logrus.Entry, isDebug bool) {
	if conn == nil {
		logger.Traceln("connection is nil")
		return
	}
	logger.Traceln("get thread lock for close")
	s := log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("get thread lock for close cost:", log.GetLogDuration(isDebug, s))
	logger.Traceln("close connection")
	s = log.GetLogNow(isDebug)
	wrapper.TaosClose(conn)
	logger.Debugln("close connection cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
}

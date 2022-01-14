package monitor

import (
	"sync/atomic"

	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/log"
	"github.com/taosdata/taosadapter/tools/monitor"
)

var logger = log.GetLogger("monitor")

const (
	NormalStatus = uint32(0)
	PauseStatus  = uint32(1)
)

var (
	pauseStatus = NormalStatus
	queryStatus = NormalStatus
)

func StartMonitor() {
	systemStatus := make(chan monitor.SysStatus)
	go func() {
		for {
			select {
			case status := <-systemStatus:
				if status.MemError == nil {
					if status.MemPercent >= config.Conf.Monitor.PauseAllMemoryPercent {
						if atomic.CompareAndSwapUint32(&pauseStatus, NormalStatus, PauseStatus) {
							logger.Warn("pause all")
						}
					} else {
						if atomic.CompareAndSwapUint32(&pauseStatus, PauseStatus, NormalStatus) {
							logger.Warn("resume all")
						}
					}
					if status.MemPercent >= config.Conf.Monitor.PauseQueryMemoryPercent {
						if atomic.CompareAndSwapUint32(&queryStatus, NormalStatus, PauseStatus) {
							logger.Warn("pause query")
						}
					} else {
						if atomic.CompareAndSwapUint32(&queryStatus, PauseStatus, NormalStatus) {
							logger.Warn("resume query")
						}
					}
				}
			}
		}
	}()
	monitor.SysMonitor.Register(systemStatus)
	monitor.Start(config.Conf.Monitor.CollectDuration, config.Conf.Monitor.InCGroup)
}

func QueryPaused() bool {
	return atomic.LoadUint32(&queryStatus) == PauseStatus || atomic.LoadUint32(&pauseStatus) == PauseStatus
}

func AllPaused() bool {
	return atomic.LoadUint32(&pauseStatus) == PauseStatus
}

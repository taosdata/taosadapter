// Copyright (c) 2022 TAOS Data, Inc.
//
// SPDX-License-Identifier: MIT
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package monitor

import (
	"runtime"
	"sync"
	"time"

	"github.com/taosdata/taosadapter/v3/log"
)

var logger = log.GetLogger("MON")

type SysStatus struct {
	CollectTime     time.Time
	CpuPercent      float64
	CpuError        error
	MemPercent      float64
	MemError        error
	GoroutineCounts int
	ThreadCounts    int
}

type sysMonitor struct {
	sync.Mutex
	collectDuration time.Duration
	collector       SysCollector
	status          *SysStatus
	outputs         map[chan<- SysStatus]struct{}
	ticker          *time.Ticker
}

func (s *sysMonitor) collect() {
	s.status.CollectTime = time.Now()
	s.status.CpuPercent, s.status.CpuError = s.collector.CpuPercent()
	s.status.MemPercent, s.status.MemError = s.collector.MemPercent()
	s.status.GoroutineCounts = runtime.NumGoroutine()
	s.status.ThreadCounts, _ = runtime.ThreadCreateProfile(nil)
	s.Lock()
	for output := range s.outputs {
		select {
		case output <- *s.status:
		default:
		}
	}
	s.Unlock()
}

func (s *sysMonitor) Register(c chan<- SysStatus) {
	s.Lock()
	if s.outputs == nil {
		s.outputs = map[chan<- SysStatus]struct{}{
			c: {},
		}
	} else {
		s.outputs[c] = struct{}{}
	}
	s.Unlock()
}

func (s *sysMonitor) Deregister(c chan<- SysStatus) {
	s.Lock()
	if s.outputs != nil {
		delete(s.outputs, c)
	}
	s.Unlock()
}

var SysMonitor = &sysMonitor{status: &SysStatus{}}

func Start(collectDuration time.Duration, inCGroup bool) {
	SysMonitor.collectDuration = collectDuration
	if inCGroup {
		collector, err := NewCGroupCollector(readUint)
		if err != nil {
			logger.WithError(err).Fatal("new normal controller")
		}
		SysMonitor.collector = collector
	} else {
		collector, err := NewNormalCollector()
		if err != nil {
			logger.WithError(err).Fatal("new normal controller")
		}
		SysMonitor.collector = collector
	}
	SysMonitor.collect()
	SysMonitor.ticker = time.NewTicker(SysMonitor.collectDuration)
	go func() {
		for range SysMonitor.ticker.C {
			SysMonitor.collect()
		}
	}()
}

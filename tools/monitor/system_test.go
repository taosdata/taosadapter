// Copyright (c) 2024 TAOS Data, Inc.
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
	"testing"
	"time"
)

// Mock SysCollector for testing
type MockSysCollector struct {
	cpuPercent float64
	memPercent float64
	cpuErr     error
	memErr     error
}

func (m *MockSysCollector) CpuPercent() (float64, error) {
	return m.cpuPercent, m.cpuErr
}

func (m *MockSysCollector) MemPercent() (float64, error) {
	return m.memPercent, m.memErr
}

func TestSysMonitorCollect(t *testing.T) {
	mockCollector := &MockSysCollector{
		cpuPercent: 50.0,
		memPercent: 70.0,
		cpuErr:     nil,
		memErr:     nil,
	}
	SysMonitor.collector = mockCollector
	SysMonitor.status = &SysStatus{}

	ch := make(chan SysStatus, 1)
	SysMonitor.Register(ch)
	defer SysMonitor.Deregister(ch)

	SysMonitor.collect()

	status := <-ch
	if status.CpuPercent != 50.0 || status.MemPercent != 70.0 {
		t.Errorf("Unexpected values in status: CpuPercent = %f, MemPercent = %f", status.CpuPercent, status.MemPercent)
	}
	if status.CpuError != nil || status.MemError != nil {
		t.Errorf("Unexpected errors in status: CpuError = %v, MemError = %v", status.CpuError, status.MemError)
	}
	if status.GoroutineCounts != runtime.NumGoroutine() {
		t.Errorf("Unexpected GoroutineCounts: got %d, want %d", status.GoroutineCounts, runtime.NumGoroutine())
	}
}

func TestSysMonitorRegisterDeregister(t *testing.T) {
	ch := make(chan SysStatus, 1)
	SysMonitor.Register(ch)
	SysMonitor.collect()
	select {
	case <-ch:
		// Expected to receive data
	default:
		t.Error("Expected to receive data in registered channel, but did not")
	}

	SysMonitor.Deregister(ch)
	SysMonitor.collect()
	select {
	case <-ch:
		t.Error("Expected not to receive data in deregistered channel, but did")
	default:
		// Expected to receive nothing after deregistration
	}
}

func TestSysMonitorStart(t *testing.T) {
	mockCollector := &MockSysCollector{}
	SysMonitor.collector = mockCollector
	SysMonitor.ticker = nil // Ensure no pre-existing ticker

	// Register a channel to verify output
	ch := make(chan SysStatus, 1)
	SysMonitor.Register(ch)
	defer SysMonitor.Deregister(ch)

	// Start the SysMonitor with a 100ms interval
	Start(100*time.Millisecond, false)
	defer func() {
		if SysMonitor.ticker != nil {
			SysMonitor.ticker.Stop()
		}
	}()

	// Give enough time for the ticker to trigger the collect function
	time.Sleep(200 * time.Millisecond)

	select {
	case <-ch:
		// Expected to receive data
	default:
		t.Error("Expected to receive data at interval, but did not")
	}

	// Clean up
	SysMonitor.Deregister(ch)
}

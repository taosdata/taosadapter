// Copyright (c) 2021 TAOS Data, Inc.
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

package thread

import "github.com/taosdata/taosadapter/v3/monitor/metrics"

type Semaphore struct {
	c     chan struct{}
	gauge *metrics.Gauge
}

var SyncSemaphore *Semaphore
var AsyncSemaphore *Semaphore

func NewSemaphore(count int) *Semaphore {
	return &Semaphore{c: make(chan struct{}, count)}
}
func (l *Semaphore) SetGauge(gauge *metrics.Gauge) {
	l.gauge = gauge
}

func (l *Semaphore) Acquire() {
	l.c <- struct{}{}
	if l.gauge != nil {
		l.gauge.Inc()
	}
}

func (l *Semaphore) Release() {
	<-l.c
	if l.gauge != nil {
		l.gauge.Dec()
	}
}

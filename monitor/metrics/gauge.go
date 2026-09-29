// Copyright (c) 2023 TAOS Data, Inc.
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

package metrics

import (
	"math"
	"sync/atomic"
)

type Gauge struct {
	valBits    uint64
	metricName string
}

func NewGauge(metricName string) *Gauge {
	return &Gauge{metricName: metricName}
}

func (g *Gauge) Set(val float64) {
	atomic.StoreUint64(&g.valBits, math.Float64bits(val))
}

func (g *Gauge) Inc() {
	g.Add(1)
}

func (g *Gauge) Dec() {
	g.Add(-1)
}

func (g *Gauge) Add(val float64) {
	for {
		oldBits := atomic.LoadUint64(&g.valBits)
		newBits := math.Float64bits(math.Float64frombits(oldBits) + val)
		if atomic.CompareAndSwapUint64(&g.valBits, oldBits, newBits) {
			return
		}
	}
}

func (g *Gauge) Sub(val float64) {
	g.Add(val * -1)
}

func (g *Gauge) Value() float64 {
	return math.Float64frombits(atomic.LoadUint64(&g.valBits))
}

func (g *Gauge) MetricName() string {
	return g.metricName
}

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
	"testing"
)

func TestGaugeSetAndGet(t *testing.T) {
	g := &Gauge{}
	val := 42.0
	g.Set(val)
	result := g.Value()
	if result != val {
		t.Errorf("Expected %f, got %f", val, result)
	}
}

func TestGaugeIncAndDec(t *testing.T) {
	g := &Gauge{}
	g.Inc()
	result := g.Value()
	if result != 1.0 {
		t.Errorf("Expected 1.0, got %f", result)
	}

	g.Dec()
	result = g.Value()
	if result != 0.0 {
		t.Errorf("Expected 0.0, got %f", result)
	}
}

func TestGaugeAddAndSub(t *testing.T) {
	g := &Gauge{}
	g.Add(10.0)
	result := g.Value()
	if result != 10.0 {
		t.Errorf("Expected 10.0, got %f", result)
	}

	g.Sub(5.0)
	result = g.Value()
	if result != 5.0 {
		t.Errorf("Expected 5.0, got %f", result)
	}
}

func TestGaugeMetricName(t *testing.T) {
	g := NewGauge("test")
	result := g.MetricName()
	if result != "test" {
		t.Errorf("Expected test, got %s", result)
	}
}

func BenchmarkGauge_Inc(b *testing.B) {
	g := NewGauge("test")
	for i := 0; i < b.N; i++ {
		if g != nil {
			g.Inc()
		}
	}
}

func BenchmarkGauge_Dec(b *testing.B) {
	g := NewGauge("test")
	for i := 0; i < b.N; i++ {
		if g != nil {
			g.Dec()
		}
	}
}

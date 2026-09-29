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

package statsd

import (
	"math"
	"math/rand"
	"sort"
)

const defaultPercentileLimit = 1000

// RunningStats calculates a running mean, variance, standard deviation,
// lower bound, upper bound, count, and can calculate estimated percentiles.
// It is based on the incremental algorithm described here:
//
//	https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
type RunningStats struct {
	k   float64
	n   int64
	ex  float64
	ex2 float64

	// Array used to calculate estimated percentiles
	// We will store a maximum of PercLimit values, at which point we will start
	// randomly replacing old values, hence it is an estimated percentile.
	perc      []float64
	PercLimit int

	sum float64

	lower float64
	upper float64

	// cache if we have sorted the list so that we never re-sort a sorted list,
	// which can have very bad performance.
	sorted bool
}

func (rs *RunningStats) AddValue(v float64) {
	// Whenever a value is added, the list is no longer sorted.
	rs.sorted = false

	if rs.n == 0 {
		rs.k = v
		rs.upper = v
		rs.lower = v
		if rs.PercLimit == 0 {
			rs.PercLimit = defaultPercentileLimit
		}
		rs.perc = make([]float64, 0, rs.PercLimit)
	}

	// These are used for the running mean and variance
	rs.n++
	rs.ex += v - rs.k
	rs.ex2 += (v - rs.k) * (v - rs.k)

	// add to running sum
	rs.sum += v

	// track upper and lower bounds
	if v > rs.upper {
		rs.upper = v
	} else if v < rs.lower {
		rs.lower = v
	}

	if len(rs.perc) < rs.PercLimit {
		rs.perc = append(rs.perc, v)
	} else {
		// Reached limit, choose random index to overwrite in the percentile array
		rs.perc[rand.Intn(len(rs.perc))] = v
	}
}

func (rs *RunningStats) Mean() float64 {
	return rs.k + rs.ex/float64(rs.n)
}

func (rs *RunningStats) Variance() float64 {
	return (rs.ex2 - (rs.ex*rs.ex)/float64(rs.n)) / float64(rs.n)
}

func (rs *RunningStats) Stddev() float64 {
	return math.Sqrt(rs.Variance())
}

func (rs *RunningStats) Sum() float64 {
	return rs.sum
}

func (rs *RunningStats) Upper() float64 {
	return rs.upper
}

func (rs *RunningStats) Lower() float64 {
	return rs.lower
}

func (rs *RunningStats) Count() int64 {
	return rs.n
}

func (rs *RunningStats) Percentile(n float64) float64 {
	if n > 100 {
		n = 100
	}

	if !rs.sorted {
		sort.Float64s(rs.perc)
		rs.sorted = true
	}

	i := float64(len(rs.perc)) * n / float64(100)
	return rs.perc[clamp(i, 0, len(rs.perc)-1)]
}

func clamp(i float64, min int, max int) int {
	if i < float64(min) {
		return min
	}
	if i > float64(max) {
		return max
	}
	return int(i)
}

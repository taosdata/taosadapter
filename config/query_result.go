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

package config

import (
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type QueryResult struct {
	IdleTimeout time.Duration
}

func initQueryResult() {
	viper.SetDefault("queryResult.idleTimeout", time.Hour)
	_ = viper.BindEnv("queryResult.idleTimeout", "TAOS_ADAPTER_QUERY_RESULT_IDLE_TIMEOUT")
	pflag.Duration("queryResult.idleTimeout", time.Hour, `Release WS query result sets that have been idle (no fetch) longer than this duration, 0 means disable. Env "TAOS_ADAPTER_QUERY_RESULT_IDLE_TIMEOUT"`)
}

func (q *QueryResult) setValue() {
	q.IdleTimeout = viper.GetDuration("queryResult.idleTimeout")
}

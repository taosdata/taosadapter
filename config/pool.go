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

const DefaultWaitTimeout = 60

type Pool struct {
	MaxConnect  int
	MaxIdle     int
	IdleTimeout time.Duration
	WaitTimeout int
	MaxWait     int
}

func initPool() {
	viper.SetDefault("pool.maxConnect", 0)
	_ = viper.BindEnv("pool.maxConnect", "TAOS_ADAPTER_POOL_MAX_CONNECT")
	pflag.Int("pool.maxConnect", 0, `max connections to server. Env "TAOS_ADAPTER_POOL_MAX_CONNECT"`)

	viper.SetDefault("pool.maxIdle", 0)
	_ = viper.BindEnv("pool.maxIdle", "TAOS_ADAPTER_POOL_MAX_IDLE")
	pflag.Int("pool.maxIdle", 0, `max idle connections to server. Env "TAOS_ADAPTER_POOL_MAX_IDLE"`)

	viper.SetDefault("pool.idleTimeout", time.Duration(0))
	_ = viper.BindEnv("pool.idleTimeout", "TAOS_ADAPTER_POOL_IDLE_TIMEOUT")
	pflag.Duration("pool.idleTimeout", time.Duration(0), `Set idle connection timeout. Env "TAOS_ADAPTER_POOL_IDLE_TIMEOUT"`)

	viper.SetDefault("pool.waitTimeout", DefaultWaitTimeout)
	_ = viper.BindEnv("pool.waitTimeout", "TAOS_ADAPTER_POOL_WAIT_TIMEOUT")
	pflag.Int("pool.waitTimeout", DefaultWaitTimeout, `wait for connection timeout seconds. Env "TAOS_ADAPTER_POOL_WAIT_TIMEOUT"`)

	viper.SetDefault("pool.maxWait", 0)
	_ = viper.BindEnv("pool.maxWait", "TAOS_ADAPTER_POOL_MAX_WAIT")
	pflag.Int("pool.maxWait", 0, `max count of waiting for connection. Env "TAOS_ADAPTER_POOL_MAX_WAIT"`)

}

func (p *Pool) setValue() {
	p.MaxConnect = viper.GetInt("pool.maxConnect")
	p.MaxIdle = viper.GetInt("pool.maxIdle")
	p.IdleTimeout = viper.GetDuration("pool.idleTimeout")
	p.WaitTimeout = viper.GetInt("pool.waitTimeout")
	p.MaxWait = viper.GetInt("pool.maxWait")
}

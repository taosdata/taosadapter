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

package collectd

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/v3/driver/common"
)

type Config struct {
	Enable   bool
	Port     int
	DB       string
	User     string
	Password string
	Worker   int
	TTL      int
	Token    string
}

func (c *Config) setValue() {
	c.Enable = viper.GetBool("collectd.enable")
	c.Port = viper.GetInt("collectd.port")
	c.DB = viper.GetString("collectd.db")
	c.User = viper.GetString("collectd.user")
	c.Password = viper.GetString("collectd.password")
	c.Worker = viper.GetInt("collectd.worker")
	c.TTL = viper.GetInt("collectd.ttl")
	c.Token = viper.GetString("collectd.token")
}

func init() {
	_ = viper.BindEnv("collectd.enable", "TAOS_ADAPTER_COLLECTD_ENABLE")
	pflag.Bool("collectd.enable", false, `enable collectd. Env "TAOS_ADAPTER_COLLECTD_ENABLE"`)
	viper.SetDefault("collectd.enable", false)

	_ = viper.BindEnv("collectd.port", "TAOS_ADAPTER_COLLECTD_PORT")
	pflag.Int("collectd.port", 6045, `collectd server port. Env "TAOS_ADAPTER_COLLECTD_PORT"`)
	viper.SetDefault("collectd.port", 6045)

	_ = viper.BindEnv("collectd.db", "TAOS_ADAPTER_COLLECTD_DB")
	pflag.String("collectd.db", "collectd", `collectd db name. Env "TAOS_ADAPTER_COLLECTD_DB"`)
	viper.SetDefault("collectd.db", "collectd")

	_ = viper.BindEnv("collectd.user", "TAOS_ADAPTER_COLLECTD_USER")
	pflag.String("collectd.user", common.DefaultUser, `collectd user. Env "TAOS_ADAPTER_COLLECTD_USER"`)
	viper.SetDefault("collectd.user", common.DefaultUser)

	_ = viper.BindEnv("collectd.password", "TAOS_ADAPTER_COLLECTD_PASSWORD")
	pflag.String("collectd.password", common.DefaultPassword, `collectd password. Env "TAOS_ADAPTER_COLLECTD_PASSWORD"`)
	viper.SetDefault("collectd.password", common.DefaultPassword)

	_ = viper.BindEnv("collectd.token", "TAOS_ADAPTER_COLLECTD_TOKEN")
	pflag.String("collectd.token", "", `collectd token. Env "TAOS_ADAPTER_COLLECTD_TOKEN"`)
	viper.SetDefault("collectd.token", "")

	_ = viper.BindEnv("collectd.worker", "TAOS_ADAPTER_COLLECTD_WORKER")
	pflag.Int("collectd.worker", 10, `collectd write worker. Env "TAOS_ADAPTER_COLLECTD_WORKER"`)
	viper.SetDefault("collectd.worker", 10)

	_ = viper.BindEnv("collectd.ttl", "TAOS_ADAPTER_COLLECTD_TTL")
	pflag.Int("collectd.ttl", 0, `collectd data ttl. Env "TAOS_ADAPTER_COLLECTD_TTL"`)
	viper.SetDefault("collectd.ttl", 0)
}

// Copyright (c) 2025 TAOS Data, Inc.
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

package watch

import (
	"github.com/fsnotify/fsnotify"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/log"
)

func OnConfigChange(file string, _ fsnotify.Op, logger *logrus.Entry) {
	logger.Info("config file changed, reload config")
	v := viper.New()
	v.SetConfigType("toml")
	v.SetConfigFile(file)
	err := v.ReadInConfig()
	if err != nil {
		logger.Errorf("read config failed: %s", err)
		return
	}
	err = config.Conf.Reject.SetValue(v)
	if err != nil {
		logger.Errorf("reject set value failed: %s", err)
		return
	}
	if v.IsSet("log.level") {
		logLevel := v.GetString("log.level")
		logger.Debugf("set log level: %s", logLevel)
		err = log.SetLevel(logLevel)
		if err != nil {
			logger.Errorf("set log level failed: %s, level: %s", err, logLevel)
			return
		}
	}
	logger.Infof("reload config success")
}

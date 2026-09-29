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

package plugin

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/taosdata/taosadapter/v3/log"
)

var logger = log.GetLogger("PLG")

type Plugin interface {
	Init(r gin.IRouter) error
	Start() error
	Stop() error
	String() string
	Version() string
}

var plugins = map[string]Plugin{}

func Register(plugin Plugin) {
	name := fmt.Sprintf("%s/%s", plugin.String(), plugin.Version())
	if _, ok := plugins[name]; ok {
		logger.Panicf("duplicate registration of plugin %s", name)
	}
	plugins[name] = plugin
}

func Init(r gin.IRouter) {
	for name, plugin := range plugins {
		logger.Debugf("init plugin %s", name)
		router := r.Group(name)
		err := plugin.Init(router)
		if err != nil {
			logger.WithError(err).Panicf("init plugin %s", name)
		}
	}
	logger.Debug("all plugin init finish")
}

func Start() {
	for name, plugin := range plugins {
		err := plugin.Start()
		if err != nil {
			logger.WithError(err).Panicf("start plugin %s", name)
		}
	}
	logger.Debug("all plugin start finish")
}

func Stop() {
	for name, plugin := range plugins {
		err := plugin.Stop()
		if err != nil {
			logger.WithError(err).Warnf("stop plugin %s", name)
		}
	}
}

func StopWithCtx(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		Stop()
	}()

	select {
	case <-ctx.Done():
	case <-done:
	}
}

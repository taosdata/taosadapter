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

package telegraflog

import (
	"bytes"
	"strings"
	"testing"

	"github.com/influxdata/telegraf"
	"github.com/sirupsen/logrus"
)

func newTestLogger() (*WrapperLogger, *bytes.Buffer) {
	buf := new(bytes.Buffer)
	baseLogger := logrus.New()
	baseLogger.SetOutput(buf)
	baseLogger.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true, DisableColors: true})
	entry := logrus.NewEntry(baseLogger)
	return NewWrapperLogger(entry), buf
}

func TestWrapperLoggerLogsAllLevels(t *testing.T) {
	logger, buf := newTestLogger()
	logger.logger.Logger.SetLevel(logrus.TraceLevel)
	logger.Errorf("errorf: %s", "err")
	logger.Error("error")
	logger.Warnf("warnf: %s", "warn")
	logger.Warn("warn")
	logger.Infof("infof: %s", "info")
	logger.Info("info")
	logger.Debugf("debugf: %s", "debug")
	logger.Debug("debug")
	logger.Tracef("tracef: %s", "trace")
	logger.Trace("trace")

	logs := buf.String()
	for _, want := range []string{
		"errorf: err", "error",
		"warnf: warn", "warn",
		"infof: info", "info",
		"debugf: debug", "debug",
		"tracef: trace", "trace",
	} {
		if !strings.Contains(logs, want) {
			t.Errorf("log output missing: %s", want)
		}
	}
}

func TestWrapperLoggerAddAttribute(t *testing.T) {
	logger, buf := newTestLogger()
	logger.AddAttribute("key", "value")
	logger.Info("with attribute")
	logs := buf.String()
	if !strings.Contains(logs, "key=value") {
		t.Error("attribute not found in log output")
	}
}

func TestWrapperLoggerLevelMapping(t *testing.T) {
	logger, _ := newTestLogger()
	for _, tc := range []struct {
		setLevel  logrus.Level
		wantLevel telegraf.LogLevel
	}{
		{logrus.PanicLevel, telegraf.Error},
		{logrus.FatalLevel, telegraf.Error},
		{logrus.ErrorLevel, telegraf.Error},
		{logrus.WarnLevel, telegraf.Warn},
		{logrus.InfoLevel, telegraf.Info},
		{logrus.DebugLevel, telegraf.Debug},
		{logrus.TraceLevel, telegraf.Trace},
		{100, telegraf.None},
	} {
		logger.logger.Logger.SetLevel(tc.setLevel)
		got := logger.Level()
		if got != tc.wantLevel {
			t.Errorf("for logrus level %v, want telegraf level %v, got %v", tc.setLevel, tc.wantLevel, got)
		}
	}
}

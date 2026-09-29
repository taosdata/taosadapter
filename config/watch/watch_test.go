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
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/fsnotify/fsnotify"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/log"
)

func TestOnConfigChange(t *testing.T) {
	config.Init()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "config.toml")
	logger := logrus.New().WithField("test", "TestOnConfigChange")
	// file not exists
	OnConfigChange(file, fsnotify.Create, logger)
	// create file, no content
	f, err := os.Create(file)
	require.NoError(t, err)
	err = f.Close()
	assert.NoError(t, err)
	OnConfigChange(file, fsnotify.Create, logger)
	// invalid reject content
	err = os.WriteFile(file, []byte("rejectQuerySqlRegex = ['^SELECT * FROM [']"), 0644)
	require.NoError(t, err)
	OnConfigChange(file, fsnotify.Write, logger)
	// valid reject content
	err = os.WriteFile(file, []byte("rejectQuerySqlRegex = ['^SELECT * FROM .*']"), 0644)
	require.NoError(t, err)
	OnConfigChange(file, fsnotify.Write, logger)
	got := config.Conf.Reject.GetRejectQuerySqlRegex()
	assert.Equal(t, []*regexp.Regexp{regexp.MustCompile("^SELECT * FROM .*")}, got)
	// log level change
	err = os.WriteFile(file, []byte("rejectQuerySqlRegex = ['^SELECT * FROM .*']\n[log]\nlevel = 'debug'"), 0644)
	require.NoError(t, err)
	OnConfigChange(file, fsnotify.Write, logger)
	got = config.Conf.Reject.GetRejectQuerySqlRegex()
	assert.Equal(t, []*regexp.Regexp{regexp.MustCompile("^SELECT * FROM .*")}, got)
	logLevel := log.GetLogLevel()
	assert.Equal(t, logrus.DebugLevel, logLevel)
	// wrong log level
	err = os.WriteFile(file, []byte("rejectQuerySqlRegex = ['^SELECT * FROM .*']\n[log]\nlevel = 'dbg'"), 0644)
	require.NoError(t, err)
	OnConfigChange(file, fsnotify.Write, logger)
	got = config.Conf.Reject.GetRejectQuerySqlRegex()
	assert.Equal(t, []*regexp.Regexp{regexp.MustCompile("^SELECT * FROM .*")}, got)
	logLevel = log.GetLogLevel()
	assert.Equal(t, logrus.DebugLevel, logLevel)
}

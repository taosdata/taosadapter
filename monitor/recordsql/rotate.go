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

package recordsql

import (
	"fmt"
	"path/filepath"
	"time"

	rotatelogs "github.com/taosdata/file-rotatelogs/v2"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/version"
)

var globalSQLRotateWriter *rotatelogs.RotateLogs
var globalStmtRotateWriter *rotatelogs.RotateLogs

func getRotateWriter(recordType RecordType) (*rotatelogs.RotateLogs, error) {
	var err error
	switch recordType {
	case RecordTypeSQL:
		if globalSQLRotateWriter == nil {
			globalSQLRotateWriter, err = newRotateWriter(recordType)
			if err != nil {
				return nil, fmt.Errorf("failed to initialize rotate writer: %s", err)
			}
		} else {
			err = globalSQLRotateWriter.Rotate()
			if err != nil {
				return nil, fmt.Errorf("failed to rotate file: %s", err)
			}
		}
		return globalSQLRotateWriter, nil
	case RecordTypeStmt:
		if globalStmtRotateWriter == nil {
			globalStmtRotateWriter, err = newRotateWriter(recordType)
			if err != nil {
				return nil, fmt.Errorf("failed to initialize rotate writer: %s", err)
			}
		} else {
			err = globalStmtRotateWriter.Rotate()
			if err != nil {
				return nil, fmt.Errorf("failed to rotate file: %s", err)
			}
		}
		return globalStmtRotateWriter, nil
	}
	return nil, fmt.Errorf("unknown record type: %d", recordType)
}

func newRotateWriter(recordType RecordType) (*rotatelogs.RotateLogs, error) {
	return rotatelogs.New(
		filepath.Join(config.Conf.Log.Path, fmt.Sprintf("%sadapter%s_%d_%%Y%%m%%d.csv", version.CUS_PROMPT, recordType, config.Conf.InstanceID)),
		rotatelogs.WithRotationCount(config.Conf.Log.RotationCount),
		rotatelogs.WithRotationTime(time.Hour*24),
		rotatelogs.WithRotationSize(int64(config.Conf.Log.RotationSize)),
		rotatelogs.WithReservedDiskSize(int64(config.Conf.Log.ReservedDiskSize)),
		rotatelogs.WithRotateGlobPattern(filepath.Join(config.Conf.Log.Path, fmt.Sprintf("%sadapter%s_%d_*.csv*", version.CUS_PROMPT, recordType, config.Conf.InstanceID))),
		rotatelogs.WithCompress(config.Conf.Log.Compress),
		rotatelogs.WithCleanLockFile(filepath.Join(config.Conf.Log.Path, fmt.Sprintf(".%sadapter%s_%d_rotate_lock", version.CUS_PROMPT, recordType, config.Conf.InstanceID))),
		rotatelogs.ForceNewFile(),
		rotatelogs.WithMaxAge(time.Hour*24*time.Duration(config.Conf.Log.KeepDays)),
	)
}

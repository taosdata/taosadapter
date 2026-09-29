// Copyright (c) 2024 TAOS Data, Inc.
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

package wrapper

import (
	"testing"
)

// @author: xftan
// @date: 2022/1/27 17:27
// @description: test taos_set_config
func TestSetConfig(t *testing.T) {
	source := map[string]string{
		"numOfThreadsPerCore":   "1.000000",
		"rpcTimer":              "300",
		"rpcForceTcp":           "0",
		"rpcMaxTime":            "600",
		"compressMsgSize":       "-1",
		"maxSQLLength":          "1048576",
		"maxWildCardsLength":    "100",
		"maxNumOfOrderedRes":    "100000",
		"keepColumnName":        "0",
		"timezone":              "Asia/Shanghai (CST, +0800)",
		"locale":                "C.UTF-8",
		"charset":               "UTF-8",
		"numOfLogLines":         "10000000",
		"asyncLog":              "1",
		"debugFlag":             "135",
		"rpcDebugFlag":          "131",
		"tmrDebugFlag":          "131",
		"cDebugFlag":            "131",
		"jniDebugFlag":          "131",
		"odbcDebugFlag":         "131",
		"uDebugFlag":            "131",
		"qDebugFlag":            "131",
		"maxBinaryDisplayWidth": "30",
		"tempDir":               "/tmp/",
	}
	err := TaosSetConfig(source)
	if err != nil {
		t.Error(err)
	}
}

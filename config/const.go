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

package config

import (
	"net/http"

	"github.com/taosdata/taosadapter/v3/httperror"
)

const ReqIDKey = "QID"
const SessionIDKey = "SID"
const ModelKey = "model"

var ErrorStatusMap = map[int32]int{
	//400
	httperror.TSDB_CODE_TSC_SQL_SYNTAX_ERROR:        http.StatusBadRequest,
	httperror.TSDB_CODE_TSC_LINE_SYNTAX_ERROR:       http.StatusBadRequest,
	httperror.TSDB_CODE_PAR_SYNTAX_ERROR:            http.StatusBadRequest,
	httperror.TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE:  http.StatusBadRequest,
	httperror.TSDB_CODE_TSC_VALUE_OUT_OF_RANGE:      http.StatusBadRequest,
	httperror.TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE: http.StatusBadRequest,
	//401
	httperror.TSDB_CODE_MND_USER_ALREADY_EXIST:  http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_USER_NOT_EXIST:      http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_INVALID_USER_FORMAT: http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_INVALID_PASS_FORMAT: http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_NO_USER_FROM_CONN:   http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_TOO_MANY_USERS:      http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_INVALID_ALTER_OPER:  http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_AUTH_FAILURE:        http.StatusUnauthorized,
	//502
	httperror.RPC_NETWORK_UNAVAIL: http.StatusBadGateway,
}

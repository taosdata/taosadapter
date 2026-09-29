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

package common

import "unsafe"

//revive:disable
const (
	MaxTaosSqlLen   = 1048576
	DefaultUser     = "root"
	DefaultPassword = "taosdata"
)

const (
	PrecisionMilliSecond = 0
	PrecisionMicroSecond = 1
	PrecisionNanoSecond  = 2
)

const (
	TSDB_OPTION_LOCALE = iota
	TSDB_OPTION_CHARSET
	TSDB_OPTION_TIMEZONE
	TSDB_OPTION_CONFIGDIR
	TSDB_OPTION_SHELL_ACTIVITY_TIMER
	TSDB_OPTION_USE_ADAPTER
)

const (
	TSDB_OPTION_CONNECTION_CLEAR = iota - 1
	TSDB_OPTION_CONNECTION_CHARSET
	TSDB_OPTION_CONNECTION_TIMEZONE
	TSDB_OPTION_CONNECTION_USER_IP
	TSDB_OPTION_CONNECTION_USER_APP
	TSDB_OPTION_CONNECTION_CONNECTOR_INFO // 3.4.0.0
)

const (
	TMQ_RES_INVALID    = -1
	TMQ_RES_DATA       = 1
	TMQ_RES_TABLE_META = 2
	TMQ_RES_METADATA   = 3
	TMQ_RES_RAWDATA    = 4
)

var TypeLengthMap = map[int]int{
	TSDB_DATA_TYPE_NULL:      1,
	TSDB_DATA_TYPE_BOOL:      1,
	TSDB_DATA_TYPE_TINYINT:   1,
	TSDB_DATA_TYPE_SMALLINT:  2,
	TSDB_DATA_TYPE_INT:       4,
	TSDB_DATA_TYPE_BIGINT:    8,
	TSDB_DATA_TYPE_FLOAT:     4,
	TSDB_DATA_TYPE_DOUBLE:    8,
	TSDB_DATA_TYPE_TIMESTAMP: 8,
	TSDB_DATA_TYPE_UTINYINT:  1,
	TSDB_DATA_TYPE_USMALLINT: 2,
	TSDB_DATA_TYPE_UINT:      4,
	TSDB_DATA_TYPE_UBIGINT:   8,
}

const (
	Int8Size    = unsafe.Sizeof(int8(0))
	Int16Size   = unsafe.Sizeof(int16(0))
	Int32Size   = unsafe.Sizeof(int32(0))
	Int64Size   = unsafe.Sizeof(int64(0))
	UInt8Size   = unsafe.Sizeof(uint8(0))
	UInt16Size  = unsafe.Sizeof(uint16(0))
	UInt32Size  = unsafe.Sizeof(uint32(0))
	UInt64Size  = unsafe.Sizeof(uint64(0))
	Float32Size = unsafe.Sizeof(float32(0))
	Float64Size = unsafe.Sizeof(float64(0))
)

const ReqIDKey = "taos_req_id"

const (
	TAOS_NOTIFY_PASSVER       = 0
	TAOS_NOTIFY_WHITELIST_VER = 1
	TAOS_NOTIFY_USER_DROPPED  = 2
)

const (
	TAOS_CONN_MODE_BI = 0
)

const (
	TSDB_CONNECTION_INFO_USER = 0
)

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

import (
	"fmt"
	"time"
)

func TimestampConvertToTime(timestamp int64, precision int) time.Time {
	switch precision {
	case PrecisionMilliSecond: // milli-second
		return time.Unix(0, timestamp*1e6)
	case PrecisionMicroSecond: // micro-second
		return time.Unix(0, timestamp*1e3)
	case PrecisionNanoSecond: // nano-second
		return time.Unix(0, timestamp)
	default:
		s := fmt.Sprintln("unknown precision", precision, "timestamp", timestamp)
		panic(s)
	}
}

func TimeToTimestamp(t time.Time, precision int) (timestamp int64) {
	switch precision {
	case PrecisionMilliSecond:
		return t.UnixNano() / 1e6
	case PrecisionMicroSecond:
		return t.UnixNano() / 1e3
	case PrecisionNanoSecond:
		return t.UnixNano()
	default:
		s := fmt.Sprintln("unknown precision", precision, "time", t)
		panic(s)
	}
}

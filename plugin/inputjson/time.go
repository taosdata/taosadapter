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

package inputjson

import (
	"strconv"
	"time"
)

var predefinedTimeFormats = []string{
	"unix",
	"unix_ms",
	"unix_us",
	"unix_ns",
	"ansic",
	"rubydate",
	"rfc822z",
	"rfc1123z",
	"rfc3339",
	"rfc3339nano",
	"stamp",
	"stampmilli",
	"datetime",
}

func isPredefinedTimeFormat(format string) bool {
	for _, f := range predefinedTimeFormats {
		if f == format {
			return true
		}
	}
	return false
}

func parseTime(strTime string, format string, loc *time.Location) (time.Time, error) {
	switch format {
	case "unix":
		ts, err := strconv.ParseInt(strTime, 10, 64)
		if err != nil {
			return time.Time{}, err
		}
		return time.Unix(ts, 0), nil
	case "unix_ms":
		ts, err := strconv.ParseInt(strTime, 10, 64)
		if err != nil {
			return time.Time{}, err
		}
		return time.UnixMilli(ts), nil
	case "unix_us":
		ts, err := strconv.ParseInt(strTime, 10, 64)
		if err != nil {
			return time.Time{}, err
		}
		return time.UnixMicro(ts), nil
	case "unix_ns":
		ts, err := strconv.ParseInt(strTime, 10, 64)
		if err != nil {
			return time.Time{}, err
		}
		return time.Unix(0, ts), nil
	case "ansic":
		layout := time.ANSIC
		return time.ParseInLocation(layout, strTime, loc)
	case "rubydate":
		layout := time.RubyDate
		return time.ParseInLocation(layout, strTime, loc)
	case "rfc822z":
		layout := time.RFC822Z
		return time.ParseInLocation(layout, strTime, loc)
	case "rfc1123z":
		layout := time.RFC1123Z
		return time.ParseInLocation(layout, strTime, loc)
	case "rfc3339":
		layout := time.RFC3339
		return time.ParseInLocation(layout, strTime, loc)
	case "rfc3339nano":
		layout := time.RFC3339Nano
		return time.ParseInLocation(layout, strTime, loc)
	case "stamp":
		layout := time.Stamp
		return time.ParseInLocation(layout, strTime, loc)
	case "stampmilli":
		layout := time.StampMilli
		return time.ParseInLocation(layout, strTime, loc)
	case "datetime":
		layout := "2006-01-02 15:04:05.999999999"
		return time.ParseInLocation(layout, strTime, loc)
	default:
		// strftime format
		return time.ParseInLocation(format, strTime, loc)
	}
}

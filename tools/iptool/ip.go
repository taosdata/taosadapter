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

package iptool

import (
	"net"
	"net/http"
	"strconv"
	"strings"
)

func GetRealIP(r *http.Request) net.IP {
	ip := r.Header.Get("X-Real-Ip")
	if ip != "" {
		return net.ParseIP(ip)
	}
	host, _, _ := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	parsedIP := net.ParseIP(host)
	if parsedIP != nil {
		return parsedIP
	}
	// Fallback to resolving the host if the host has zone information
	ipAddr, err := net.ResolveIPAddr("ip", host)
	if err != nil {
		return nil
	}
	return ipAddr.IP
}

func GetRealPort(r *http.Request) (string, error) {
	port := r.Header.Get("X-Real-Port")
	if port != "" {
		_, err := strconv.ParseUint(port, 10, 32)
		if err != nil {
			return "", err
		}
		return port, nil
	}

	_, port, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err != nil {
		return "", err
	}
	if port != "" {
		_, err = strconv.ParseUint(port, 10, 32)
		if err != nil {
			return "", err
		}
		return port, nil
	}

	return port, nil
}

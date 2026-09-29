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

package testtools

import (
	"net"
	"strconv"
	"strings"
	"testing"
)

func IsValidPort(port string) bool {
	p, err := strconv.Atoi(port)
	return err == nil && p >= 1024 && p <= 65535
}

func TestGetRandomRemoteAddrGeneratesValidIPv4OrIPv6(t *testing.T) {
	for i := 0; i < 100; i++ { // Test multiple times to cover randomness
		addr := GetRandomRemoteAddr()
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			t.Errorf("Invalid address format: %v", err)
			continue
		}

		if ip := net.ParseIP(host); ip == nil {
			t.Errorf("Invalid IP address: %s", host)
		}

		if !IsValidPort(port) {
			t.Errorf("Invalid port: %s", port)
		}
	}
}

func TestGetRandomRemoteAddrGeneratesIPv4(t *testing.T) {
	foundIPv4 := false
	for i := 0; i < 100; i++ { // Test multiple times to ensure IPv4 is generated
		addr := GetRandomRemoteAddr()
		host, _, _ := net.SplitHostPort(addr)
		if ip := net.ParseIP(host); ip != nil && strings.Count(host, ":") == 0 {
			foundIPv4 = true
			break
		}
	}
	if !foundIPv4 {
		t.Error("Failed to generate IPv4 address")
	}
}

func TestGetRandomRemoteAddrGeneratesIPv6(t *testing.T) {
	foundIPv6 := false
	for i := 0; i < 100; i++ { // Test multiple times to ensure IPv6 is generated
		addr := GetRandomRemoteAddr()
		host, _, _ := net.SplitHostPort(addr)
		if ip := net.ParseIP(host); ip != nil && strings.Count(host, ":") > 0 {
			foundIPv6 = true
			break
		}
	}
	if !foundIPv6 {
		t.Error("Failed to generate IPv6 address")
	}
}

// Copyright (c) 2021 TAOS Data, Inc.
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

package native

import (
	"testing"
)

// The driver is loaded at run time, so nothing checks at build time that a usable
// client is installed.  This test is that check: it runs wherever a client package
// is installed (the adapter's CI installs one), and skips where it is not, because
// the rest of the module can still be built and unit tested without a client.
//
// A real entry point cannot be called from here -- cgo is not available in test
// files -- but resolving one is the same lookup the generated forwarders do, and
// the calls themselves are covered by driver/wrapper's tests and by the adapter's
// own tests, which all run against this loader now.
func TestPreload(t *testing.T) {
	if err := Preload(); err != nil {
		t.Skipf("no usable TDengine client driver in this environment: %v", err)
	}

	if path := DriverPath(); path == "" {
		t.Fatal("driver is loaded but its path is empty")
	}
	if !Supported("taos_connect") {
		t.Errorf("driver %s does not provide taos_connect", DriverPath())
	}
	// taos_register_instance is one of the entry points the dispatcher did not
	// forward before, i.e. the reason the adapter used to link the driver as well.
	if !Supported("taos_register_instance") {
		t.Errorf("driver %s does not provide taos_register_instance", DriverPath())
	}
	if Supported("td_no_such_entry_point") {
		t.Error("a non-existent entry point resolved")
	}
	if n := NotProvidedCount(); n != 0 {
		t.Errorf("%d call(s) went to entry points the driver does not provide, last: %q", n, LastNotProvided())
	}
}

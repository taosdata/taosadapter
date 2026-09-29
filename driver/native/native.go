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

// Package native loads the TDengine native client driver (libtaosnative.so /
// libtaosnative.dylib / taosnative.dll) at run time and provides the public C API
// symbols the cgo code calls.
//
// taosAdapter used to get those symbols from the libtaos.so dispatcher by linking
// it, plus the driver itself for the entry points the dispatcher did not forward.
// That tied the build to whichever client package was installed (a nightly older
// than the sources failed to link) and left the answering library up to ELF symbol
// lookup order.  The adapter only ever wants the native driver, so it now loads it
// directly: see tdNativeApi.h for the mechanism and tdApiLoader.c for the search
// order, which mirrors the dispatcher's.
package native

/*
#cgo linux LDFLAGS: -ldl
// -ldl belongs here, not in the packages that call the API: tdApiLoader.c is part
// of this package and is what calls dlopen()/dlsym()/dladdr().  glibc >= 2.34 has
// them in libc and the flag is then a no-op, but before that it is what makes a
// binary that links *only* this package -- its own test binary, for one -- link.
#include "tdNativeApi.h"
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"unsafe"
)

func init() {
	// Load as soon as the package is linked, so the driver is available (and any
	// problem is recorded) before the first call.  The failure is reported by the
	// caller that can log it -- Preload() in the adapter's startup path -- because
	// package initialisation has no logger yet.
	_ = Preload()
}

// Preload loads the native client driver and returns the reason it could not be
// loaded.  Calls made before this still work: the driver is loaded on first use.
//
// It is safe to call from several goroutines at once: the C side loads the driver
// exactly once (pthread_once / InitOnceExecuteOnce) and later calls just report
// the result of that single attempt.
func Preload() error {
	if C.tdApiLoad() != 0 {
		return fmt.Errorf("load TDengine client driver failed: %s", LoadError())
	}
	return nil
}

// Loaded reports whether the native client driver is loaded.  It loads the driver
// on first use, like every other entry point here -- pass through Preload() when the
// difference between "loaded" and "just loaded" matters.
func Loaded() bool { return C.tdApiLoad() == 0 }

// DriverPath returns the absolute path of the loaded driver, or "" when the
// driver is not loaded (this call does not load it: use Preload() first, which is
// also what logs the path at start-up).
func DriverPath() string { return C.GoString(C.tdApiDriverPath()) }

// LoadError returns why the driver could not be loaded, or "" when it is loaded.
func LoadError() string { return C.GoString(C.tdApiLoadError()) }

// Supported reports whether the loaded driver provides the public entry point
// `name` (for example "taos_stmt2_bind_param_column").  It is what a caller needs
// when a client older than the adapter is installed: such a client is usable, but
// the entry points it does not have fail their own call instead of the build.
func Supported(name string) bool {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	return C.tdApiResolve(cName) != nil
}

// LastNotProvided returns the most recent entry point the driver did not provide,
// or "" when every call so far was resolved.
func LastNotProvided() string { return C.GoString(C.tdApiLastNotProvided()) }

// NotProvidedCount returns how many calls were made to entry points the driver
// does not provide.
func NotProvidedCount() int { return int(C.tdApiNotProvidedCount()) }

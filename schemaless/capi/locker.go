package capi

import (
	"runtime"

	"github.com/taosdata/taosadapter/thread"
)

var locker = thread.NewLocker(runtime.NumCPU() * 5)

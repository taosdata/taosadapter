package main

// @title taosAdapter
// @version 1.0
// @description taosAdapter restful API

// @host http://127.0.0.1:6041
// @query.collection.format multi

import (
	"runtime"

	"github.com/taosdata/taosadapter/system"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	r := system.Init()
	system.Start(r)
}

package main

// @title taosAdapter
// @version 1.0
// @description taosAdapter restful API

// @host http://127.0.0.1:6041
// @query.collection.format multi

import (
	"net/http"
	"runtime"

	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/system"
)

var logger = log.GetLogger("main")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	r := system.Init()
	system.Start(r, func(server *http.Server) {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("listen: %s\n", err)
		}
	})
}

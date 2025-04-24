package main

// @title taosAdapter
// @version 1.0
// @description taosAdapter restful API

// @host http://127.0.0.1:6041
// @query.collection.format multi

import (
	"net"
	"net/http"
	"fmt"

	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/system"
)

var logger = log.GetLogger("TCP")

func main() {
	fmt.Println("taosAdapter TCP server starting...")
	r := system.Init()
	system.Start(r, func(server *http.Server) {
		ln, err := net.Listen("tcp4", server.Addr)
		if err != nil {
			logger.Fatalf("listen: %s", err)
		}
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("listen: %s", err)
		}
	})
}

package main

import "github.com/gin-gonic/gin"

func setupRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.POST("/rest/sql", func(c *gin.Context) {
		c.Status(200)
	})
	r.POST("/opentsdb/v1/put/telnet/test", func(c *gin.Context) {
		c.Status(200)
	})
	return r
}

func main() {
	r := setupRouter()
	r.Run(":6041")
}

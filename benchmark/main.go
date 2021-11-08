package main

import "github.com/gin-gonic/gin"

func setupRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.POST("/rest/sql", func(c *gin.Context) {
		c.Status(200)
	})
	return r
}

func main() {
	r := setupRouter()
	err := r.Run(":6041")
	if err != nil {
		panic(err)
	}
}

package web

import "github.com/gin-gonic/gin"

func GetRequestID(c *gin.Context) uint32 {
	return c.MustGet("currentID").(uint32)
}

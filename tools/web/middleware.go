package web

import (
	"fmt"

	"github.com/gin-gonic/gin"
)

func GetRequestID(c *gin.Context) uint32 {
	return c.MustGet("currentID").(uint32)
}

func SetTaosErrorCode(c *gin.Context, code int) {
	c.Set("taos_error_code", fmt.Sprintf("0x%04x", code))
}

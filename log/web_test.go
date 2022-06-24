package log_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/log"
)

// @author: xftan
// @date: 2021/12/14 15:07
// @description: test gin log middleware
func TestGinLog(t *testing.T) {
	log.ConfigLog()
	router := gin.New()
	router.Use(log.GinLog())
	router.Use(log.GinRecoverLog())
	router.POST("/rest/sql", func(c *gin.Context) {
		c.Status(200)
	})
	router.POST("/panic", func(c *gin.Context) {
		panic("test")
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/rest/sql", nil)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/panic", nil)
	router.ServeHTTP(w, req)
	assert.Equal(t, 500, w.Code)
}

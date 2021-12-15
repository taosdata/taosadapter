package plugin

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

// @author: xftan
// @date: 2021/12/14 15:09
// @description: test auth middleware
func TestAuth(t *testing.T) {
	router := gin.Default()
	router.GET("/", Auth(func(c *gin.Context, code int, err error) {
		c.AbortWithStatusJSON(code, err)
	}), func(c *gin.Context) {
		u, p, err := GetAuth(c)
		if assert.NoError(t, err) {
			assert.Equal(t, u, "root")
			assert.Equal(t, p, "taosdata")
		}
		c.Status(200)
	})
	RegisterGenerateAuth(router)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/", nil)
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/", nil)
	req.SetBasicAuth("root", "taosdata")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/genauth/root/taosdata/aaa", nil)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	token := w.Body.Bytes()
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/", nil)
	tokenStr := base64.StdEncoding.EncodeToString(token)
	req.Header.Set("Authorization", "Basic "+tokenStr)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

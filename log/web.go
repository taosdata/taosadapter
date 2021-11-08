package log

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

func GinLog() gin.HandlerFunc {
	logger := GetLogger("web")
	id := uint32(0)
	return func(c *gin.Context) {
		currentID := atomic.AddUint32(&id, 1)
		startTime := time.Now()
		c.Set("currentID", currentID)
		c.Next()
		latencyTime := time.Now().Sub(startTime)
		reqMethod := c.Request.Method
		reqUri := c.Request.RequestURI
		statusCode := c.Writer.Status()
		clientIP := c.ClientIP()
		logger.WithField("sessionID", currentID).Infof("| %3d | %13v | %15s | %s | %s \n",
			statusCode,
			latencyTime,
			clientIP,
			reqMethod,
			reqUri,
		)
	}
}

type recoverLog struct {
	logger logrus.FieldLogger
}

func (r *recoverLog) Write(p []byte) (n int, err error) {
	logger.Errorln(p)
	return len(p), nil
}

func GinRecoverLog() gin.HandlerFunc {
	logger := GetLogger("web")
	return func(c *gin.Context) {
		id := c.MustGet("currentID").(uint32)
		writer := &recoverLog{logger: logger.WithField("id", id)}
		gin.RecoveryWithWriter(writer, func(c *gin.Context, err interface{}) {
			c.AbortWithStatus(http.StatusInternalServerError)
		})
	}
}

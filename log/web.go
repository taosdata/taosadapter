package log

import (
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/config"
)

func GinLog() gin.HandlerFunc {
	logger := GetLogger("web")
	id := uint32(0)

	return func(c *gin.Context) {
		RequestInFlight.Inc()
		currentID := atomic.AddUint32(&id, 1)
		startTime := time.Now()
		c.Set("currentID", currentID)
		c.Next()
		latencyTime := time.Now().Sub(startTime)
		reqMethod := c.Request.Method
		reqUri := c.Request.RequestURI
		statusCode := c.Writer.Status()
		clientIP := c.ClientIP()
		logger.WithField("sessionID", currentID).Infof("| %3d | %13v | %15s | %s | %s ",
			statusCode,
			latencyTime,
			clientIP,
			reqMethod,
			reqUri,
		)
		statusCodeStr := strconv.Itoa(statusCode)
		RequestSummery.WithLabelValues(reqMethod, reqUri).Observe(latencyTime.Seconds() * 1e3)
		TotalRequest.WithLabelValues(statusCodeStr, clientIP, reqMethod, reqUri).Inc()
		_, exist := c.Get("taos_error_code")
		if exist || (statusCode >= 400) {
			FailRequest.WithLabelValues(statusCodeStr, clientIP, reqMethod, reqUri).Inc()
		}
		RequestInFlight.Dec()
		if config.Conf.Log.EnableRecordHttpSql {
			sql, exist := c.Get("sql")
			if exist {
				sqlLogger.Infof("%d '%s' '%s' '%s' %s", currentID, reqUri, reqMethod, clientIP, sql)
			}
		}
	}
}

type recoverLog struct {
	logger logrus.FieldLogger
}

func (r *recoverLog) Write(p []byte) (n int, err error) {
	r.logger.Errorln(string(p))
	return len(p), nil
}

func GinRecoverLog() gin.HandlerFunc {
	logger := GetLogger("web")
	return func(c *gin.Context) {
		id := c.MustGet("currentID").(uint32)
		writer := &recoverLog{logger: logger.WithField("id", id)}
		gin.RecoveryWithWriter(writer)(c)
	}
}

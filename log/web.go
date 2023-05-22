package log

import (
	"net/url"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/config"
)

func GinLog() gin.HandlerFunc {
	logger := GetLogger("web")

	return func(c *gin.Context) {
		if !config.Conf.Monitor.Disable {
			RequestInFlight.Inc()
		}
		startTime := time.Now()
		c.Next()
		latencyTime := time.Since(startTime)
		reqMethod := c.Request.Method
		reqUri := url.QueryEscape(c.Request.RequestURI)
		statusCode := c.Writer.Status()
		clientIP := c.ClientIP()
		reqID, exist := c.Get(config.ReqIDKey)
		if exist {
			logger.WithField(config.ReqIDKey, reqID).Infof("| %3d | %13v | %15s | %s | %s ", statusCode, latencyTime, clientIP, reqMethod, reqUri)
		} else {
			logger.Infof("| %3d | %13v | %15s | %s | %s ", statusCode, latencyTime, clientIP, reqMethod, reqUri)
		}
		if config.Conf.Log.EnableRecordHttpSql {
			sql, exist := c.Get("sql")
			if exist {
				sqlLogger.Infof("%d '%s' '%s' '%s' %s", reqID, reqUri, reqMethod, clientIP, sql)
			}
		}
		statusCodeStr := strconv.Itoa(statusCode)
		if !config.Conf.Monitor.Disable {
			if config.Conf.Monitor.DisableClientIP {
				clientIP = "invisible"
			}
			RequestSummery.WithLabelValues(reqMethod, reqUri).Observe(latencyTime.Seconds() * 1e3)
			TotalRequest.WithLabelValues(statusCodeStr, clientIP, reqMethod, reqUri).Inc()
			_, exist := c.Get("taos_error_code")
			if exist || (statusCode >= 400) {
				FailRequest.WithLabelValues(statusCodeStr, clientIP, reqMethod, reqUri).Inc()
			}
			RequestInFlight.Dec()
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
		writer := &recoverLog{logger: logger}
		gin.RecoveryWithWriter(writer)(c)
	}
}

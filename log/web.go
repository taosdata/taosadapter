package log

import (
	"net/url"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/config"
)

func GinLog() gin.HandlerFunc {
	logger := GetLogger("web")

	return func(c *gin.Context) {
		startTime := time.Now()
		c.Next()
		latencyTime := time.Since(startTime)
		reqMethod := c.Request.Method
		reqUri := url.QueryEscape(c.Request.RequestURI)
		statusCode := c.Writer.Status()
		clientIP := c.RemoteIP()
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

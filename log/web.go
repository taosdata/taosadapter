package log

import (
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/config"
)

func GinLog() gin.HandlerFunc {
	logger := GetLogger("web")
	id := uint32(0)
	var (
		totalRequest = promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "taosadapter",
				Subsystem: "restful",
				Name:      "http_request_total",
				Help:      "The total number of processed http requests",
			}, []string{"status_code", "client_ip", "request_method", "request_uri"})

		failRequest = promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "taosadapter",
				Subsystem: "restful",
				Name:      "http_request_fail",
				Help:      "The number of failures of http request processing",
			}, []string{"status_code", "client_ip", "request_method", "request_uri", "taos_error_code"})

		requestInFlight = promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "taosadapter",
				Subsystem: "restful",
				Name:      "http_request_in_flight",
				Help:      "Current number of in-flight http requests",
			},
		)

		requestSummery = promauto.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace:  "taosadapter",
				Subsystem:  "restful",
				Name:       "http_request_summary_milliseconds",
				Help:       "Summary of latencies for http requests in millisecond",
				Objectives: map[float64]float64{0.1: 0.001, 0.2: 0.002, 0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
				MaxAge:     config.Conf.Monitor.WriteInterval,
			}, []string{"request_method", "request_uri"})
	)
	return func(c *gin.Context) {
		requestInFlight.Inc()
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
		requestSummery.WithLabelValues(reqMethod, reqUri).Observe(latencyTime.Seconds() * 1e3)
		totalRequest.WithLabelValues(statusCodeStr, clientIP, reqMethod, reqUri).Inc()
		code, exist := c.Get("taos_error_code")
		if exist || (statusCode >= 400) {
			taosErrorCode := ""
			if code != nil {
				taosErrorCode = code.(string)
			}
			failRequest.WithLabelValues(statusCodeStr, clientIP, reqMethod, reqUri, taosErrorCode).Inc()
		}
		requestInFlight.Dec()
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

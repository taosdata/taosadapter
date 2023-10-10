package monitor

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/monitor"
)

var logger = log.GetLogger("monitor")

const (
	NormalStatus = uint32(0)
	PauseStatus  = uint32(1)
)

var (
	pauseStatus = NormalStatus
	queryStatus = NormalStatus
)
var (
	cpuPercent = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "taosadapter",
			Subsystem: "system",
			Name:      "cpu_percent",
			Help:      "Percentage of all cpu used by the program",
		},
	)
	memPercent = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "taosadapter",
			Subsystem: "system",
			Name:      "mem_percent",
			Help:      "Percentage of all memory used by the program",
		},
	)
)

var identity string
var conn unsafe.Pointer

func StartMonitor() {
	if len(config.Conf.Monitor.Identity) != 0 {
		identity = config.Conf.Monitor.Identity
	} else {
		hostname, err := os.Hostname()
		if err != nil {
			logger.WithError(err).Panic("can not get hostname")
		}
		if len(hostname) > 39 {
			hostname = hostname[:39]
		}
		identity = fmt.Sprintf("%s:%d", hostname, config.Conf.Port)
	}
	systemStatus := make(chan monitor.SysStatus)
	go func() {
		for status := range systemStatus {
			if status.CpuError == nil {
				cpuPercent.Set(status.CpuPercent)
			}
			if status.MemError == nil {
				memPercent.Set(status.MemPercent)
				if status.MemPercent >= config.Conf.Monitor.PauseAllMemoryThreshold {
					if atomic.CompareAndSwapUint32(&pauseStatus, NormalStatus, PauseStatus) {
						logger.Warn("pause all")
					}
				} else {
					if atomic.CompareAndSwapUint32(&pauseStatus, PauseStatus, NormalStatus) {
						logger.Warn("resume all")
					}
				}
				if status.MemPercent >= config.Conf.Monitor.PauseQueryMemoryThreshold {
					if atomic.CompareAndSwapUint32(&queryStatus, NormalStatus, PauseStatus) {
						logger.Warn("pause query")
					}
				} else {
					if atomic.CompareAndSwapUint32(&queryStatus, PauseStatus, NormalStatus) {
						logger.Warn("resume query")
					}
				}
			}
		}
	}()
	monitor.SysMonitor.Register(systemStatus)
	monitor.Start(config.Conf.Monitor.CollectDuration, config.Conf.Monitor.InCGroup)
	if !config.Conf.Monitor.Disable && config.Conf.Monitor.WriteToTD {
		ticker := time.NewTicker(config.Conf.Monitor.WriteInterval)
		go func() {
			for range ticker.C {
				err := writeToTDLog()
				if err != nil {
					logger.WithError(err).Error("write to server error")
				}
			}
		}()
	}
	InitKeeper()
	StartUpload()
}

func QueryPaused() bool {
	return atomic.LoadUint32(&queryStatus) == PauseStatus || atomic.LoadUint32(&pauseStatus) == PauseStatus
}

func AllPaused() bool {
	return atomic.LoadUint32(&pauseStatus) == PauseStatus
}

const InsertStatement = "insert into"

const (
	createRequestTotal    = "create stable if not exists `taosadapter_restful_http_request_total` (`_ts` timestamp, `gauge` double) tags (`client_ip` nchar(40), `endpoint` nchar(45), `request_method` nchar(16), `request_uri` nchar(128), `status_code` nchar(4))"
	createRequestUpdate   = "create stable if not exists `taosadapter_restful_http_request_update` (`_ts` timestamp, `gauge` double) tags (`client_ip` nchar(40), `endpoint` nchar(45), `request_method` nchar(16), `request_uri` nchar(128))"
	createRequestSelect   = "create stable if not exists `taosadapter_restful_http_request_select` (`_ts` timestamp, `gauge` double) tags (`client_ip` nchar(40), `endpoint` nchar(45), `request_method` nchar(16), `request_uri` nchar(128))"
	createRequestFail     = "create stable if not exists `taosadapter_restful_http_request_fail` (`_ts` timestamp, `gauge` double) tags (`client_ip` nchar(40), `endpoint` nchar(45), `request_method` nchar(16), `request_uri` nchar(128), `status_code` nchar(4))"
	createRequestInFlight = "create stable if not exists `taosadapter_restful_http_request_in_flight` (`_ts` timestamp, `gauge` double) tags (`endpoint` nchar(45))"
	createRequestSummary  = "create stable if not exists `taosadapter_restful_http_request_summary_milliseconds` (`_ts` timestamp, `0.1` double, `0.2` double, `0.5` double, `0.9` double, `0.99` double, `count` double, `sum` double) tags (`endpoint` nchar(45), `request_method` nchar(16), `request_uri` nchar(128))"

	createMemPercent = "create stable if not exists `taosadapter_system_mem_percent` (`_ts` timestamp, `gauge` double) tags (`endpoint` nchar(45))"
	createCpuPercent = "create stable if not exists `taosadapter_system_cpu_percent` (`_ts` timestamp, `gauge` double) tags (`endpoint` nchar(45))"

	createWSQueryRequestTotal    = "create stable if not exists `taosadapter_ws_query_request_total` (`_ts` timestamp, `gauge` double) tags (`client_ip` nchar(40), `endpoint` nchar(45))"
	createWSQueryRequestUpdate   = "create stable if not exists `taosadapter_ws_query_request_update` (`_ts` timestamp, `gauge` double) tags (`client_ip` nchar(40), `endpoint` nchar(45))"
	createWSQueryRequestSelect   = "create stable if not exists `taosadapter_ws_query_request_select` (`_ts` timestamp, `gauge` double) tags (`client_ip` nchar(40), `endpoint` nchar(45))"
	createWSQueryRequestFail     = "create stable if not exists `taosadapter_ws_query_request_fail` (`_ts` timestamp, `gauge` double) tags (`client_ip` nchar(40), `endpoint` nchar(45))"
	createWSQueryRequestInFlight = "create stable if not exists `taosadapter_ws_query_request_in_flight` (`_ts` timestamp, `gauge` double) tags (`endpoint` nchar(45))"
)

var insightTableList = []string{
	createRequestTotal,
	createRequestUpdate,
	createRequestSelect,
	createRequestFail,
	createRequestInFlight,
	createRequestSummary,
	createMemPercent,
	createCpuPercent,
	createWSQueryRequestTotal,
	createWSQueryRequestUpdate,
	createWSQueryRequestSelect,
	createWSQueryRequestFail,
	createWSQueryRequestInFlight,
}

var tableInitialized bool
var builder = strings.Builder{}

func writeToTDLog() error {
	var err error
	if conn == nil {
		conn, err = wrapper.TaosConnect("", config.Conf.Monitor.User, config.Conf.Monitor.Password, "", 0)
		if err != nil {
			return err
		}
		err = async.GlobalAsync.TaosExecWithoutResult(conn, "create database if not exists log", 0)
		if err != nil {
			return err
		}
	}
	err = async.GlobalAsync.TaosExecWithoutResult(conn, "use log", 0)
	if err != nil {
		return err
	}
	if !tableInitialized {
		for _, tb := range insightTableList {
			if err = async.GlobalAsync.TaosExecWithoutResult(conn, tb, 0); err != nil {
				return err
			}
		}

		tableInitialized = true
	}
	data, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return err
	}

	var inserts []string
	now := time.Now().Format(time.RFC3339Nano)
	for i := 0; i < len(data); i++ {
		tbName := data[i].GetName()
		switch tbName {
		case "taosadapter_restful_http_request_total":
			metric := data[i].GetMetric()
			for _, m := range metric {
				value := m.GetGauge().GetValue()
				if value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0) {
					labels := m.GetLabel()

					subName := calculateTableName(labels)
					var (
						statusCode    int
						statusCodeStr string
						clientIP      string
						requestMethod string
						requestUri    string
					)

					for j := 0; j < len(labels); j++ {
						switch labels[j].GetName() {
						case "status_code":
							statusCodeStr = labels[j].GetValue()
							statusCode, _ = strconv.Atoi(statusCodeStr)
						case "client_ip":
							clientIP = labels[j].GetValue()
						case "request_method":
							requestMethod = labels[j].GetValue()
						case "request_uri":
							requestUri = labels[j].GetValue()
						}
					}
					inserts = append(inserts, fmt.Sprintf(" taosa_http_request_total_%s using taosadapter_restful_http_request_total tags('%s','%s','%s','%s','%d') values('%s',%f)", subName, clientIP, identity, requestMethod, requestUri, statusCode, now, value))
					log.TotalRequest.WithLabelValues(statusCodeStr, clientIP, requestMethod, requestUri).Sub(value)
				}
			}
		case "taosadapter_restful_http_request_update":
			metric := data[i].GetMetric()
			for _, m := range metric {
				value := m.GetGauge().GetValue()
				if value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0) {
					labels := m.GetLabel()

					subName := calculateTableName(labels)
					var (
						clientIP      string
						requestMethod string
						requestUri    string
					)

					for j := 0; j < len(labels); j++ {
						switch labels[j].GetName() {
						case "client_ip":
							clientIP = labels[j].GetValue()
						case "request_method":
							requestMethod = labels[j].GetValue()
						case "request_uri":
							requestUri = labels[j].GetValue()
						}
					}
					inserts = append(inserts, fmt.Sprintf(" taosa_http_request_update_%s using taosadapter_restful_http_request_update tags('%s','%s','%s','%s') values('%s',%f)", subName, clientIP, identity, requestMethod, requestUri, now, value))
					log.UpdateRequest.WithLabelValues(clientIP, requestMethod, requestUri).Sub(value)
				}
			}
		case "taosadapter_restful_http_request_select":
			metric := data[i].GetMetric()
			for _, m := range metric {
				value := m.GetGauge().GetValue()
				if value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0) {
					labels := m.GetLabel()

					subName := calculateTableName(labels)
					var (
						clientIP      string
						requestMethod string
						requestUri    string
					)

					for j := 0; j < len(labels); j++ {
						switch labels[j].GetName() {
						case "client_ip":
							clientIP = labels[j].GetValue()
						case "request_method":
							requestMethod = labels[j].GetValue()
						case "request_uri":
							requestUri = labels[j].GetValue()
						}
					}
					inserts = append(inserts, fmt.Sprintf(" taosa_http_request_select_%s using taosadapter_restful_http_request_select tags('%s','%s','%s','%s') values('%s',%f)", subName, clientIP, identity, requestMethod, requestUri, now, value))
					log.SelectRequest.WithLabelValues(clientIP, requestMethod, requestUri).Sub(value)
				}
			}
		case "taosadapter_restful_http_request_fail":
			metric := data[i].GetMetric()
			for _, m := range metric {
				value := m.GetGauge().GetValue()
				if value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0) {
					labels := m.GetLabel()
					subName := calculateTableName(labels)
					var (
						statusCode    int
						statusCodeStr string
						clientIP      string
						requestMethod string
						requestUri    string
					)

					for j := 0; j < len(labels); j++ {
						switch labels[j].GetName() {
						case "status_code":
							statusCodeStr = labels[j].GetValue()
							statusCode, _ = strconv.Atoi(labels[j].GetValue())
						case "client_ip":
							clientIP = labels[j].GetValue()
						case "request_method":
							requestMethod = labels[j].GetValue()
						case "request_uri":
							requestUri = labels[j].GetValue()
						}

					}

					inserts = append(inserts, fmt.Sprintf(" taosa_http_request_fail_%s using taosadapter_restful_http_request_fail tags('%s','%s','%s','%s','%d') values('%s',%f)", subName, clientIP, identity, requestMethod, requestUri, statusCode, now, value))
					log.FailRequest.WithLabelValues(statusCodeStr, clientIP, requestMethod, requestUri).Sub(value)
				}
			}
		case "taosadapter_restful_http_request_in_flight":
			metric := data[i].GetMetric()
			for _, m := range metric {
				value := m.GetGauge().GetValue()
				if value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0) {
					subName := calculateTableName(nil)
					inserts = append(inserts, fmt.Sprintf(" taosa_http_request_in_flight_%s using taosadapter_restful_http_request_in_flight tags('%s') values('%s',%f)", subName, identity, now, value))
				}
			}
		case "taosadapter_restful_http_request_summary_milliseconds":
			metric := data[i].GetMetric()
			for _, m := range metric {
				labels := m.GetLabel()
				subName := calculateTableName(labels)
				var (
					requestMethod string
					requestUri    string
				)

				for j := 0; j < len(labels); j++ {
					switch labels[j].GetName() {
					case "request_method":
						requestMethod = labels[j].GetValue()
					case "request_uri":
						requestUri = labels[j].GetValue()
					}
				}
				quantile := m.GetSummary().GetQuantile()
				var (
					quantile1  float64
					quantile2  float64
					quantile5  float64
					quantile9  float64
					quantile99 float64
				)
				for _, q := range quantile {
					v := q.GetValue()
					switch q.GetQuantile() {
					case 0.1:
						quantile1 = v
					case 0.2:
						quantile2 = v
					case 0.5:
						quantile5 = v
					case 0.9:
						quantile9 = v
					case 0.99:
						quantile99 = v
					}
				}
				if !math.IsNaN(quantile1) {
					inserts = append(inserts, fmt.Sprintf(" taosa_request_summary_%s using taosadapter_restful_http_request_summary_milliseconds tags('%s','%s','%s') values('%s',%f,%f,%f,%f,%f,%d,%f)", subName, identity, requestMethod, requestUri, now, quantile1, quantile2, quantile5, quantile9, quantile99, m.GetSummary().GetSampleCount(), m.GetSummary().GetSampleSum()))
				}
			}
		case "taosadapter_system_cpu_percent":
			metric := data[i].GetMetric()
			m := metric[len(metric)-1]

			inserts = append(inserts, fmt.Sprintf(" taos_system_cup_%s using taosadapter_system_cpu_percent tags('%s') values('%s',%f)", calculateTableName(m.GetLabel()), identity, now, m.GetGauge().GetValue()))
		case "taosadapter_system_mem_percent":
			metric := data[i].GetMetric()
			m := metric[len(metric)-1]

			inserts = append(inserts, fmt.Sprintf(" taos_system_mem_%s using taosadapter_system_mem_percent tags('%s') values('%s',%f)", calculateTableName(m.GetLabel()), identity, now, m.GetGauge().GetValue()))
		case "taosadapter_ws_query_request_total":
			metric := data[i].GetMetric()
			for _, m := range metric {
				value := m.GetGauge().GetValue()
				if value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0) {
					labels := m.GetLabel()

					subName := calculateTableName(labels)
					var (
						clientIP string
					)

					for j := 0; j < len(labels); j++ {
						switch labels[j].GetName() {
						case "client_ip":
							clientIP = labels[j].GetValue()
						}
					}
					inserts = append(inserts, fmt.Sprintf(" taosa_ws_query_request_total_%s using taosadapter_ws_query_request_total tags('%s','%s') values('%s',%f)", subName, clientIP, identity, now, value))
					log.WSTotalQueryRequest.WithLabelValues(clientIP).Sub(value)
				}
			}
		case "taosadapter_ws_query_request_update":
			metric := data[i].GetMetric()
			for _, m := range metric {
				value := m.GetGauge().GetValue()
				if value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0) {
					labels := m.GetLabel()

					subName := calculateTableName(labels)
					var (
						clientIP string
					)

					for j := 0; j < len(labels); j++ {
						switch labels[j].GetName() {
						case "client_ip":
							clientIP = labels[j].GetValue()
						}
					}
					inserts = append(inserts, fmt.Sprintf(" taosa_ws_query_request_update_%s using taosadapter_ws_query_request_update tags('%s','%s') values('%s',%f)", subName, clientIP, identity, now, value))
					log.WSUpdateQueryRequest.WithLabelValues(clientIP).Sub(value)
				}
			}
		case "taosadapter_ws_query_request_select":
			metric := data[i].GetMetric()
			for _, m := range metric {
				value := m.GetGauge().GetValue()
				if value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0) {
					labels := m.GetLabel()

					subName := calculateTableName(labels)
					var (
						clientIP string
					)

					for j := 0; j < len(labels); j++ {
						switch labels[j].GetName() {
						case "client_ip":
							clientIP = labels[j].GetValue()
						}
					}
					inserts = append(inserts, fmt.Sprintf(" taosa_ws_query_request_select_%s using taosadapter_ws_query_request_select tags('%s','%s') values('%s',%f)", subName, clientIP, identity, now, value))
					log.WSUpdateQueryRequest.WithLabelValues(clientIP).Sub(value)
				}
			}
		case "taosadapter_ws_query_request_fail":
			metric := data[i].GetMetric()
			for _, m := range metric {
				value := m.GetGauge().GetValue()
				if value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0) {
					labels := m.GetLabel()

					subName := calculateTableName(labels)
					var (
						clientIP string
					)

					for j := 0; j < len(labels); j++ {
						switch labels[j].GetName() {
						case "client_ip":
							clientIP = labels[j].GetValue()
						}
					}
					inserts = append(inserts, fmt.Sprintf(" taosa_ws_query_request_fail_%s using taosadapter_ws_query_request_fail tags('%s','%s') values('%s',%f)", subName, clientIP, identity, now, value))
					log.WSTotalQueryRequest.WithLabelValues(clientIP).Sub(value)
				}
			}
		case "taosadapter_ws_query_request_in_flight":
			metric := data[i].GetMetric()
			for _, m := range metric {
				value := m.GetGauge().GetValue()
				if value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0) {
					subName := calculateTableName(nil)
					inserts = append(inserts, fmt.Sprintf(" taosa_ws_query_request_in_flight_%s using taosadapter_ws_query_request_in_flight tags('%s') values('%s',%f)", subName, identity, now, value))
				}
			}
		}
	}
	defer builder.Reset()
	if len(inserts) > 0 {
		builder.WriteString(InsertStatement)
		for i := 0; i < len(inserts); i++ {
			if builder.Len()+len(inserts[i]) >= common.MaxTaosSqlLen {
				err = async.GlobalAsync.TaosExecWithoutResult(conn, builder.String(), 0)
				if err != nil {
					return err
				}
				builder.Reset()
				builder.WriteString(InsertStatement)
			} else {
				builder.WriteString(inserts[i])
			}
		}
		if builder.Len() > len(InsertStatement) {
			err = async.GlobalAsync.TaosExecWithoutResult(conn, builder.String(), 0)
			if err != nil {
				logger.WithField("sql", builder.String()).Errorln("taos exec error", err)
				return err
			}
		}
	}
	return nil
}

func calculateTableName(labels []*io_prometheus_client.LabelPair) string {
	tmp := make([]string, len(labels)+1)
	for i := 0; i < len(labels); i++ {
		tmp[i] = labels[i].String()
	}
	tmp[len(labels)] = fmt.Sprintf(`name:"identity" value:"%s"`, identity)
	sort.Strings(tmp)
	hashed := md5.Sum([]byte(strings.Join(tmp, ",")))
	return hex.EncodeToString(hashed[:])
}

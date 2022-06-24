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
	"github.com/taosdata/driver-go/v2/common"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/db/async"
	"github.com/taosdata/taosadapter/log"
	"github.com/taosdata/taosadapter/tools/monitor"
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
		if len(hostname) > 40 {
			hostname = hostname[:40]
		}
		identity = fmt.Sprintf("%s:%d", hostname, config.Conf.Port)
	}
	systemStatus := make(chan monitor.SysStatus)
	go func() {
		for {
			select {
			case status := <-systemStatus:
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
		}
	}()
	monitor.SysMonitor.Register(systemStatus)
	monitor.Start(config.Conf.Monitor.CollectDuration, config.Conf.Monitor.InCGroup)
	if config.Conf.Monitor.WriteToTD {
		ticker := time.NewTicker(config.Conf.Monitor.WriteInterval)
		go func() {
			for {
				select {
				case <-ticker.C:
					err := writeToTDLog()
					if err != nil {
						logger.WithError(err).Error("write to TDengine error")
					}
				}
			}
		}()
	}
}

func QueryPaused() bool {
	return atomic.LoadUint32(&queryStatus) == PauseStatus || atomic.LoadUint32(&pauseStatus) == PauseStatus
}

func AllPaused() bool {
	return atomic.LoadUint32(&pauseStatus) == PauseStatus
}

const (
	CreateRequestTotalSql = "create table if not exists taosadapter_restful_http_total(ts timestamp,count bigint) tags (endpoint binary(45),status_code int, client_ip binary(40), request_method binary(15), request_uri binary(128))"
	CreateRequestFailSql  = "create table if not exists taosadapter_restful_http_fail(ts timestamp,count bigint) tags (endpoint binary(45),status_code int, client_ip binary(40), request_method binary(15), request_uri binary(128))"
	CreateRequestInFlight = "create table if not exists taosadapter_restful_http_request_in_flight(ts timestamp,count bigint) tags (endpoint binary(45))"
	CreateRequestLatency  = "create table if not exists taosadapter_restful_http_request_latency(ts timestamp,quantile_1 double,quantile_2 double,quantile_5 double,quantile_9 double,quantile_99 double) tags (endpoint binary(45),request_method binary(15), request_uri binary(128))"
	CreateSystemPercent   = "create table if not exists taosadapter_system(ts timestamp,cpu_percent double,mem_percent double) tags (endpoint binary(45))"
	InsertStatement       = "insert into"
)

var createList = []string{
	CreateRequestTotalSql,
	CreateRequestFailSql,
	CreateRequestInFlight,
	CreateRequestLatency,
	CreateSystemPercent,
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
		err = async.GlobalAsync.TaosExecWithoutResult(conn, "create database if not exists log")
		if err != nil {
			return err
		}
	}
	err = async.GlobalAsync.TaosExecWithoutResult(conn, "use log")
	if err != nil {
		return err
	}
	if !tableInitialized {
		for i := 0; i < len(createList); i++ {
			err = async.GlobalAsync.TaosExecWithoutResult(conn, createList[i])
			if err != nil {
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
	cpuPercentValue := float64(0)
	memPercentValue := float64(0)
	receiveSystemValue := false
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
					inserts = append(inserts, fmt.Sprintf(" taosa_request_total_%s using taosadapter_restful_http_total tags('%s',%d,'%s','%s','%s') values('%s',%d)", subName, identity, statusCode, clientIP, requestMethod, requestUri, now, int(value)))
					log.TotalRequest.WithLabelValues(statusCodeStr, clientIP, requestMethod, requestUri).Sub(value)
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

					inserts = append(inserts, fmt.Sprintf(" taosa_request_fail_%s using taosadapter_restful_http_fail tags('%s',%d,'%s','%s','%s') values('%s',%d)", subName, identity, statusCode, clientIP, requestMethod, requestUri, now, int(value)))
					log.FailRequest.WithLabelValues(statusCodeStr, clientIP, requestMethod, requestUri).Sub(value)
				}
			}
		case "taosadapter_restful_http_request_in_flight":
			metric := data[i].GetMetric()
			for _, m := range metric {
				value := m.GetGauge().GetValue()
				if value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0) {
					subName := calculateTableName(nil)
					inserts = append(inserts, fmt.Sprintf(" taosa_request_in_flight_%s using taosadapter_restful_http_request_in_flight tags('%s') values('%s',%d)", subName, identity, now, int(value)))
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
					inserts = append(inserts, fmt.Sprintf(" taosa_request_latency_%s using taosadapter_restful_http_request_latency tags('%s','%s','%s') values('%s',%f,%f,%f,%f,%f)", subName, identity, requestMethod, requestUri, now, quantile1, quantile2, quantile5, quantile9, quantile99))
				}
			}
		case "taosadapter_system_cpu_percent":
			metric := data[i].GetMetric()
			for _, m := range metric {
				cpuPercentValue = m.GetGauge().GetValue()
			}
			receiveSystemValue = true

		case "taosadapter_system_mem_percent":
			metric := data[i].GetMetric()
			for _, m := range metric {
				memPercentValue = m.GetGauge().GetValue()
			}
			receiveSystemValue = true
		}
	}
	if receiveSystemValue {
		subName := calculateTableName(nil)
		inserts = append(inserts, fmt.Sprintf(" taosa_system_%s using taosadapter_system tags('%s') values('%s',%f,%f)", subName, identity, now, cpuPercentValue, memPercentValue))
	}
	defer builder.Reset()
	if len(inserts) > 0 {
		builder.WriteString(InsertStatement)
		for i := 0; i < len(inserts); i++ {
			if builder.Len()+len(inserts[i]) >= common.MaxTaosSqlLen {
				err = async.GlobalAsync.TaosExecWithoutResult(conn, builder.String())
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
			err = async.GlobalAsync.TaosExecWithoutResult(conn, builder.String())
			if err != nil {
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

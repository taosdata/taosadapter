package monitor_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/process"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/controller/ping"
	"github.com/taosdata/taosadapter/controller/rest"
	"github.com/taosdata/taosadapter/db"
	"github.com/taosdata/taosadapter/log"
	"github.com/taosdata/taosadapter/monitor"
	"github.com/taosdata/taosadapter/tools/ctest"
)

var router *gin.Engine

func TestMain(m *testing.M) {
	viper.Set("monitor.writeInterval", time.Second*5)
	config.Init()
	log.ConfigLog()
	db.PrepareConnection()
	gin.SetMode(gin.ReleaseMode)
	router = gin.New()
	router.Use(log.GinLog())
	var ctl rest.Restful
	ctl.Init(router)
	var pingCtl ping.Controller
	pingCtl.Init(router)
	monitor.StartMonitor()
	m.Run()
}

// @author: xftan
// @date: 2022/1/17 11:14
// @description: test monitor function
func TestMonitor(t *testing.T) {
	machineMemory, err := mem.VirtualMemoryWithContext(context.Background())
	if err != nil {
		t.Error(err)
		return
	}
	total := machineMemory.Total
	p, err := process.NewProcess(int32(os.Getpid()))
	processMemory, err := p.MemoryInfoWithContext(context.Background())
	if err != nil {
		t.Error(err)
		return
	}
	used := processMemory.RSS
	currentPercent := 100 * (float64(used) / float64(total))
	//+20%
	size := int(float64(total) * 0.2)
	//20% + 20% = 40%
	size2 := int(float64(total) * 0.2)
	config.Conf.Monitor.PauseQueryMemoryThreshold = currentPercent + 20
	config.Conf.Monitor.PauseAllMemoryThreshold = currentPercent + 40
	monitor.StartMonitor()
	{
		assert.False(t, monitor.QueryPaused())
		assert.False(t, monitor.AllPaused())
		w := httptest.NewRecorder()
		body := strings.NewReader("show databases")
		req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)

		w = httptest.NewRecorder()
		body = strings.NewReader("create database if not exists t1")
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)

		w = httptest.NewRecorder()
		req, _ = http.NewRequest(http.MethodGet, "/-/ping?action=query", body)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)

		w = httptest.NewRecorder()
		req, _ = http.NewRequest(http.MethodGet, "/-/ping", body)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	b1 := ctest.Malloc(size)
	time.Sleep(config.Conf.Monitor.CollectDuration)
	{
		assert.True(t, monitor.QueryPaused())
		assert.False(t, monitor.AllPaused())
		w := httptest.NewRecorder()
		body := strings.NewReader("show databases")
		req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 503, w.Code)

		w = httptest.NewRecorder()
		body = strings.NewReader("create database if not exists t1")
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)

		w = httptest.NewRecorder()
		req, _ = http.NewRequest(http.MethodGet, "/-/ping?action=query", body)
		router.ServeHTTP(w, req)
		assert.Equal(t, 503, w.Code)

		w = httptest.NewRecorder()
		req, _ = http.NewRequest(http.MethodGet, "/-/ping", body)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}
	b2 := ctest.Malloc(size2)
	time.Sleep(config.Conf.Monitor.CollectDuration)
	{
		assert.True(t, monitor.QueryPaused())
		assert.True(t, monitor.AllPaused())
		w := httptest.NewRecorder()
		body := strings.NewReader("show databases")
		req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 503, w.Code)

		w = httptest.NewRecorder()
		body = strings.NewReader("create database if not exists t1")
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 503, w.Code)

		w = httptest.NewRecorder()
		req, _ = http.NewRequest(http.MethodGet, "/-/ping?action=query", body)
		router.ServeHTTP(w, req)
		assert.Equal(t, 503, w.Code)

		w = httptest.NewRecorder()
		req, _ = http.NewRequest(http.MethodGet, "/-/ping", body)
		router.ServeHTTP(w, req)
		assert.Equal(t, 503, w.Code)
	}
	assert.True(t, monitor.QueryPaused())
	assert.True(t, monitor.AllPaused())
	ctest.Free(b1)
	ctest.Free(b2)
	time.Sleep(config.Conf.Monitor.CollectDuration)
	{
		assert.False(t, monitor.QueryPaused())
		assert.False(t, monitor.AllPaused())
		w := httptest.NewRecorder()
		body := strings.NewReader("show databases")
		req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)

		w = httptest.NewRecorder()
		body = strings.NewReader("create database if not exists t1")
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)

		w = httptest.NewRecorder()
		req, _ = http.NewRequest(http.MethodGet, "/-/ping?action=query", body)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)

		w = httptest.NewRecorder()
		req, _ = http.NewRequest(http.MethodGet, "/-/ping", body)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}
}

func TestWriteLog(t *testing.T) {
	type result struct {
		Rows int `json:"rows"`
	}
	{
		w := httptest.NewRecorder()
		body := strings.NewReader("show databases")
		req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
	}
	{
		w := httptest.NewRecorder()
		body := strings.NewReader("xxx")
		req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
	}
	time.Sleep(time.Second)
	{
		w := httptest.NewRecorder()
		body := strings.NewReader("xxx")
		req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
	}
	time.Sleep(config.Conf.Monitor.WriteInterval)
	checkTables := []string{
		"taosadapter_restful_http_total",
		"taosadapter_restful_http_fail",
		"taosadapter_restful_http_request_latency",
		"taosadapter_system",
	}
	for _, table := range checkTables {
		w := httptest.NewRecorder()
		body := strings.NewReader("select * from log." + table)
		req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		var r result
		err := json.NewDecoder(w.Body).Decode(&r)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, r.Rows, 1, table)
	}
}

package monitor_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/process"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/controller/rest"
	"github.com/taosdata/taosadapter/db"
	"github.com/taosdata/taosadapter/monitor"
)

var router *gin.Engine

func TestMain(m *testing.M) {
	config.Init()
	db.PrepareConnection()
	gin.SetMode(gin.ReleaseMode)
	router = gin.New()
	router.Use(func(context *gin.Context) {
		context.Set("currentID", uint32(0))
	})
	var ctl rest.Restful
	ctl.Init(router)
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
	config.Conf.Monitor.PauseQueryMemoryPercent = currentPercent + 20
	config.Conf.Monitor.PauseAllMemoryPercent = currentPercent + 40
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
	}

	var a = make([]byte, size)
	for i := 0; i < size; i++ {
		a[i] = 1
	}
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
	}

	b := make([]byte, size2)
	for i := 0; i < size2; i++ {
		b[i] = 1
	}
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
	}
	assert.True(t, monitor.QueryPaused())
	assert.True(t, monitor.AllPaused())
	a = nil
	b = nil
	runtime.GC()
	time.Sleep(config.Conf.Monitor.CollectDuration * 2)
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
	}
}

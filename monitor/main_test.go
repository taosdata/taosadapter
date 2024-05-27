package monitor_test

import (
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller/ping"
	"github.com/taosdata/taosadapter/v3/controller/rest"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
)

var router *gin.Engine

func TestMain(m *testing.M) {
	viper.Set("monitor.disable", false)
	viper.Set("monitor.disableCollectClientIP", false)
	viper.Set("monitor.writeToTD", true)
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

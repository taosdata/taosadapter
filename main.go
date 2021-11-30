package main

// @title taosAdapter
// @version 1.0
// @description taosAdapter restful API

// @host http://127.0.0.1:6041
// @query.collection.format multi

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	files "github.com/swaggo/files"
	swagger "github.com/swaggo/gin-swagger"
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/db"
	"github.com/taosdata/taosadapter/docs"
	"github.com/taosdata/taosadapter/log"
	"github.com/taosdata/taosadapter/plugin"
	_ "github.com/taosdata/taosadapter/plugin/collectd"
	_ "github.com/taosdata/taosadapter/plugin/influxdb"
	_ "github.com/taosdata/taosadapter/plugin/nodeexporter"
	_ "github.com/taosdata/taosadapter/plugin/opentsdb"
	_ "github.com/taosdata/taosadapter/plugin/opentsdbtelnet"
	_ "github.com/taosdata/taosadapter/plugin/statsd"
	"github.com/taosdata/taosadapter/rest"
	_ "go.uber.org/automaxprocs"
)

var logger = log.GetLogger("main")

func createRouter(debug bool, corsConf *config.CorsConfig, enableGzip bool) *gin.Engine {
	if debug {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}
	router := gin.New()
	router.Use(log.GinLog())
	router.Use(log.GinRecoverLog())
	if debug {
		docs.SwaggerInfo.Schemes = []string{"http", "https"}
		router.GET("/swagger/*any", swagger.WrapHandler(files.Handler))
		pprof.Register(router)
	}
	router.GET("-/ping", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})
	if enableGzip {
		router.Use(gzip.Gzip(gzip.DefaultCompression))
	}
	router.Use(cors.New(corsConf.GetConfig()))
	return router
}

func main() {
	config.Init()
	log.ConfigLog()
	db.PrepareConnection()
	logger.Info("start server:", log.ServerID)
	router := createRouter(config.Conf.Debug, &config.Conf.Cors, false)
	r := rest.Restful{}
	_ = r.Init(router)
	plugin.RegisterGenerateAuth(router)
	plugin.Init(router)
	plugin.Start()
	server := &http.Server{
		Addr:              ":" + strconv.Itoa(config.Conf.Port),
		Handler:           router,
		ReadHeaderTimeout: 20 * time.Second,
		ReadTimeout:       200 * time.Second,
		WriteTimeout:      90 * time.Second,
	}
	logger.Println("server on :", config.Conf.Port)
	if config.Conf.SSl.Enable {
		go func() {
			if err := server.ListenAndServeTLS(config.Conf.SSl.CertFile, config.Conf.SSl.KeyFile); err != nil && err != http.ErrServerClosed {
				logger.Fatalf("listen: %s\n", err)
			}
		}()
	} else {
		go func() {
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Fatalf("listen: %s\n", err)
			}
		}()
	}
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	<-quit
	logger.Println("Shutdown WebServer ...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		if err := server.Shutdown(ctx); err != nil {
			logger.Println("WebServer Shutdown error:", err)
		}
	}()
	logger.Println("Stop Plugins ...")
	ticker := time.NewTicker(time.Second * 5)
	done := make(chan struct{})
	go func() {
		r.Close()
		plugin.Stop()
		close(done)
	}()
	select {
	case <-done:
		break
	case <-ticker.C:
		break
	}
	logger.Println("Server exiting")
}

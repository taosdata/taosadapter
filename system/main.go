package system

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
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/controller"
	"github.com/taosdata/taosadapter/db"
	"github.com/taosdata/taosadapter/log"
	"github.com/taosdata/taosadapter/monitor"
	"github.com/taosdata/taosadapter/plugin"
)

var logger = log.GetLogger("main")

func Init() *gin.Engine {
	config.Init()
	log.ConfigLog()
	db.PrepareConnection()
	logger.Info("start server:", log.ServerID)
	router := createRouter(config.Conf.Debug, &config.Conf.Cors, true)
	controllers := controller.GetControllers()
	for _, webController := range controllers {
		webController.Init(router)
	}
	plugin.RegisterGenerateAuth(router)
	plugin.Init(router)
	plugin.Start()
	return router
}

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
		//docs.SwaggerInfo.Schemes = []string{"http", "https"}
		//router.GET("/swagger/*any", swagger.WrapHandler(files.Handler))
		pprof.Register(router)
	}
	if enableGzip {
		router.Use(gzip.Gzip(gzip.DefaultCompression, gzip.WithDecompressFn(gzip.DefaultDecompressHandle)))
	}
	router.Use(cors.New(corsConf.GetConfig()))
	return router
}

func Start(router *gin.Engine) {
	monitor.StartMonitor()
	server := &http.Server{
		Addr:    ":" + strconv.Itoa(config.Conf.Port),
		Handler: router,
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
	quit := make(chan os.Signal, 1)
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

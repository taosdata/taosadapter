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
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/plugin"
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

func Start(router *gin.Engine, startHttpServer func(server *http.Server)) {
	monitor.StartMonitor()
	server := &http.Server{
		Addr:    ":" + strconv.Itoa(config.Conf.Port),
		Handler: router,
	}
	logger.Println("server on :", config.Conf.Port)
	go startHttpServer(server)
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

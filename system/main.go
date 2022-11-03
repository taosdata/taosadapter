package system

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/kardianos/service"
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
	prg := newProgram(router, startHttpServer)
	svcConfig := &service.Config{
		Name:        "taosadapter",
		DisplayName: "taosadapter",
		Description: "taosAdapter is a TDengine’s companion tool and is a bridge/adapter between TDengine cluster and application.",
	}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		logger.Fatal(err)
	}
	err = s.Run()
	if err != nil {
		logger.Fatal(err)
	}
}

type program struct {
	router          *gin.Engine
	server          *http.Server
	startHttpServer func(server *http.Server)
}

func newProgram(router *gin.Engine, startHttpServer func(server *http.Server)) *program {
	server := &http.Server{
		Addr:    ":" + strconv.Itoa(config.Conf.Port),
		Handler: router,
	}
	return &program{router: router, server: server, startHttpServer: startHttpServer}
}

func (p *program) Start(s service.Service) error {
	if service.Interactive() {
		logger.Info("Running in terminal.")
	} else {
		logger.Info("Running under service manager.")
	}
	monitor.StartMonitor()
	logger.Println("server on :", config.Conf.Port)
	go p.startHttpServer(p.server)
	return nil
}

func (p *program) Stop(s service.Service) error {
	logger.Println("Shutdown WebServer ...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		if err := p.server.Shutdown(ctx); err != nil {
			logger.Println("WebServer Shutdown error:", err)
		}
	}()
	logger.Println("Stop Plugins ...")
	plugin.StopWithCtx(ctx)
	logger.Println("Server exiting")
	ctxLog, cancelLog := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelLog()
	logger.Println("Flushing Log")
	log.Close(ctxLog)
	return nil
}

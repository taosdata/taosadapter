package promql

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/taosdata/taosadapter/log"
	"github.com/taosdata/taosadapter/monitor"
	"github.com/taosdata/taosadapter/plugin"
	"github.com/taosdata/taosadapter/plugin/promql/query"
)

var logger = log.GetLogger("promql")

type Plugin struct {
	conf           Config
	promController *query.PromDataSource
}

func (p *Plugin) Init(r gin.IRouter) error {
	p.conf.setValue()
	if !p.conf.Enable {
		logger.Info("opentsdb_telnet disabled")
		return nil
	}
	p.promController = query.NewPromDataSource(query.DefaultQueryMaxSamples, p.conf.Timeout)
	p.promController.Init()
	r.Use(plugin.Auth(func(c *gin.Context, code int, err error) {
		c.AbortWithError(code, err)
		return
	}))
	r.Use(func(c *gin.Context) {
		if monitor.QueryPaused() {
			c.Header("Retry-After", "120")
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, "query memory exceeds threshold")
			return
		}
	})
	wrap := func(f apiFunc) func(c *gin.Context) {
		return func(c *gin.Context) {
			result := setUnavailStatusOnTSDBNotReady(f(c))
			if result.finalizer != nil {
				defer result.finalizer()
			}
			if result.err != nil {
				p.respondError(c.Writer, result.err, result.data)
				return
			}

			if result.data != nil {
				p.respond(c.Writer, result.data, result.warnings)
				return
			}
			c.Writer.WriteHeader(http.StatusNoContent)
		}
	}
	api := r.Group(":db")
	api.GET("query", wrap(p.query))
	api.POST("query", wrap(p.query))

	api.GET("query_range", wrap(p.queryRange))
	api.POST("query_range", wrap(p.queryRange))

	api.GET("/labels", wrap(p.labelNames))
	api.POST("/labels", wrap(p.labelNames))
	api.GET("/label/:name/values", wrap(p.labelValues))

	api.GET("/series", wrap(p.series))
	api.POST("/series", wrap(p.series))
	return nil
}

func (p *Plugin) Start() error {
	return nil
}

func (p *Plugin) Stop() error {
	return nil
}

func (p *Plugin) String() string {
	return "promql"
}

func (p *Plugin) Version() string {
	return "v1"
}

func init() {
	plugin.Register(&Plugin{})
}

package metrics

import (
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/taosdata/taosadapter/controller"
)

type Controller struct {
}

func (c Controller) Init(r gin.IRouter) {
	r.GET("metrics", gin.WrapH(promhttp.Handler()))
}

func init() {
	r := &Controller{}
	controller.AddController(r)
}

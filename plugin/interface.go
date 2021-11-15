package plugin

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/taosdata/taosadapter/log"
)

var logger = log.GetLogger("plugin")

type Plugin interface {
	Init(r gin.IRouter) error
	Start() error
	Stop() error
	String() string
	Version() string
}

var plugins = map[string]Plugin{}

func Register(plugin Plugin) {
	name := fmt.Sprintf("%s/%s", plugin.String(), plugin.Version())
	if _, ok := plugins[name]; ok {
		logger.Panicf("duplicate registration of plugin %s", name)
	}
	plugins[name] = plugin
}

func Init(r gin.IRouter) {
	for name, plugin := range plugins {
		logger.Infof("init plugin %s", name)
		router := r.Group(name)
		err := plugin.Init(router)
		if err != nil {
			logger.WithError(err).Panicf("init plugin %s", name)
		}
	}
	logger.Infoln("all plugin init finish")
}

func Start() {
	for name, plugin := range plugins {
		err := plugin.Start()
		if err != nil {
			logger.WithError(err).Panicf("start plugin %s", name)
		}
	}
	logger.Infoln("all plugin start finish")
}

func Stop() {
	for name, plugin := range plugins {
		err := plugin.Stop()
		if err != nil {
			logger.WithError(err).Warnf("stop plugin %s", name)
		}
	}
}

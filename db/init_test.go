package db

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/config"
)

func TestPrepareConnection(t *testing.T) {
	viper.Set("taosConfigDir", "/etc/taos/")
	config.Init()
	PrepareConnection()
}

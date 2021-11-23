package log

import (
	"testing"

	"github.com/taosdata/taosadapter/config"
)

func TestConfigLog(t *testing.T) {
	config.Init()
	ConfigLog()
	logger := GetLogger("test")
	logger.Info("test config log")
}

package log

import (
	"testing"

	"github.com/taosdata/taosadapter/config"
)

// @author: xftan
// @date: 2021/12/14 15:07
// @description: test config log
func TestConfigLog(t *testing.T) {
	config.Init()
	ConfigLog()
	logger := GetLogger("test")
	logger.Info("test config log")
}

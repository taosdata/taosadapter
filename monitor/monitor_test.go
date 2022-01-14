package monitor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/config"
)

func TestMain(m *testing.M) {
	config.Init()
}
func TestMonitor(t *testing.T) {
	StartMonitor()
	assert.False(t, QueryPaused())
	assert.False(t, AllPaused())
	config.Conf.Monitor.PauseQueryMemoryPercent = 0
	config.Conf.Monitor.PauseAllMemoryPercent = 0
	time.Sleep(config.Conf.Monitor.CollectDuration)
	assert.True(t, QueryPaused())
	assert.True(t, AllPaused())
}

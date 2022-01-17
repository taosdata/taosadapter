package config

import (
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Monitor struct {
	CollectDuration           time.Duration
	InCGroup                  bool
	PauseQueryMemoryThreshold float64
	PauseAllMemoryThreshold   float64
}

func initMonitor() {
	viper.SetDefault("monitor.collectDuration", time.Second*3)
	_ = viper.BindEnv("monitor.collectDuration", "TAOS_MONITOR_COLLECT_DURATION")
	pflag.Duration("monitor.collectDuration", time.Second*3, `Set monitor duration. Env "TAOS_MONITOR_COLLECT_DURATION"`)

	viper.SetDefault("monitor.incgroup", false)
	_ = viper.BindEnv("monitor.incgroup", "TAOS_MONITOR_INCGROUP")
	pflag.Bool("monitor.incgroup", false, `Whether running in cgroup. Env "TAOS_MONITOR_INCGROUP"`)

	viper.SetDefault("monitor.pauseQueryMemoryThreshold", 70)
	_ = viper.BindEnv("monitor.pauseQueryMemoryThreshold", "TAOS_MONITOR_PAUSE_QUERY_MEMORY_THRESHOLD")
	pflag.Float64("monitor.pauseQueryMemoryThreshold", 70, `Memory percentage threshold for pause query. Env "TAOS_MONITOR_PAUSE_QUERY_MEMORY_THRESHOLD"`)

	viper.SetDefault("monitor.pauseAllMemoryThreshold", 80)
	_ = viper.BindEnv("monitor.pauseAllMemoryThreshold", "TAOS_MONITOR_PAUSE_ALL_MEMORY_THRESHOLD")
	pflag.Float64("monitor.pauseAllMemoryThreshold", 80, `Memory percentage threshold for pause all. Env "TAOS_MONITOR_PAUSE_ALL_MEMORY_THRESHOLD"`)
}

func (p *Monitor) setValue() {
	p.CollectDuration = viper.GetDuration("monitor.collectDuration")
	p.InCGroup = viper.GetBool("monitor.incgroup")
	p.PauseQueryMemoryThreshold = viper.GetFloat64("monitor.pauseQueryMemoryThreshold")
	p.PauseAllMemoryThreshold = viper.GetFloat64("monitor.pauseAllMemoryThreshold")
}

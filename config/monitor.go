package config

import (
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Monitor struct {
	CollectDuration         time.Duration
	InCGroup                bool
	PauseQueryMemoryPercent float64
	PauseAllMemoryPercent   float64
}

func initMonitor() {
	viper.SetDefault("monitor.collectDuration", time.Second*3)
	_ = viper.BindEnv("monitor.collectDuration", "TAOS_MONITOR_COLLECT_DURATION")
	pflag.Duration("monitor.collectDuration", time.Second*3, `Set monitor duration. Env "TAOS_MONITOR_COLLECT_DURATION"`)

	viper.SetDefault("monitor.incgroup", false)
	_ = viper.BindEnv("monitor.incgroup", "TAOS_MONITOR_INCGROUP")
	pflag.Bool("monitor.incgroup", false, `Whether running in cgroup. Env "TAOS_MONITOR_INCGROUP"`)

	viper.SetDefault("monitor.pauseQueryMemoryPercent", 70)
	_ = viper.BindEnv("monitor.pauseQueryMemoryPercent", "TAOS_MONITOR_PAUSE_QUERY_MEMORY_PERCENT")
	pflag.Float64("monitor.pauseQueryMemoryPercent", 70, `Memory percentage threshold for pause query. Env "TAOS_MONITOR_PAUSE_QUERY_MEMORY_PERCENT"`)

	viper.SetDefault("monitor.pauseAllMemoryPercent", 80)
	_ = viper.BindEnv("monitor.pauseAllMemoryPercent", "TAOS_MONITOR_PAUSE_ALL_MEMORY_PERCENT")
	pflag.Float64("monitor.pauseAllMemoryPercent", 80, `Memory percentage threshold for pause all. Env "TAOS_MONITOR_PAUSE_ALL_MEMORY_PERCENT"`)
}

func (p *Monitor) setValue() {
	p.CollectDuration = viper.GetDuration("monitor.collectDuration")
	p.InCGroup = viper.GetBool("monitor.incgroup")
	p.PauseQueryMemoryPercent = viper.GetFloat64("monitor.pauseQueryMemoryPercent")
	p.PauseAllMemoryPercent = viper.GetFloat64("monitor.pauseAllMemoryPercent")
}

package config

import (
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Pool struct {
	MaxConnect  int
	MaxIdle     int
	IdleTimeout time.Duration
}

func initPool() {
	viper.SetDefault("pool.maxConnect", 0)
	_ = viper.BindEnv("pool.maxConnect", "TAOS_ADAPTER_POOL_MAX_CONNECT")
	pflag.Int("pool.maxConnect", 0, `max connections to server. Env "TAOS_ADAPTER_POOL_MAX_CONNECT"`)

	viper.SetDefault("pool.maxIdle", 0)
	_ = viper.BindEnv("pool.maxIdle", "TAOS_ADAPTER_POOL_MAX_IDLE")
	pflag.Int("pool.maxIdle", 0, `max idle connections to server. Env "TAOS_ADAPTER_POOL_MAX_IDLE"`)

	viper.SetDefault("pool.idleTimeout", time.Duration(0))
	_ = viper.BindEnv("pool.idleTimeout", "TAOS_ADAPTER_POOL_IDLE_TIMEOUT")
	pflag.Duration("pool.idleTimeout", time.Duration(0), `Set idle connection timeout. Env "TAOS_ADAPTER_POOL_IDLE_TIMEOUT"`)

}

func (p *Pool) setValue() {
	p.MaxConnect = viper.GetInt("pool.maxConnect")
	p.MaxIdle = viper.GetInt("pool.maxIdle")
	p.IdleTimeout = viper.GetDuration("pool.idleTimeout")
}

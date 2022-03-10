package promql

import (
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Config struct {
	Enable  bool
	Timeout time.Duration
}

func (c *Config) setValue() {
	c.Enable = viper.GetBool("prometheus.enable")
	c.Timeout = viper.GetDuration("promql.timeout")
}

func init() {
	_ = viper.BindEnv("promql.enable", "TAOS_ADAPTER_PROMQL_ENABLE")
	pflag.Bool("promql.enable", true, `enable promql. Env "TAOS_ADAPTER_PROMQL_ENABLE"`)
	viper.SetDefault("promql.enable", true)

	_ = viper.BindEnv("promql.timeout", "TAOS_ADAPTER_PROMQL_TIMEOUT")
	pflag.Duration("promql.timeout", 30*time.Second, `promql timeout. Env "TAOS_ADAPTER_PROMQL_TIMEOUT"`)
	viper.SetDefault("promql.timeout", 30*time.Second)
}

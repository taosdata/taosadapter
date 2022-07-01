package collectd

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/taosdata/driver-go/v2/common"
)

type Config struct {
	Enable   bool
	Port     int
	DB       string
	User     string
	Password string
	Worker   int
}

func (c *Config) setValue() {
	c.Enable = viper.GetBool("collectd.enable")
	c.Port = viper.GetInt("collectd.port")
	c.DB = viper.GetString("collectd.db")
	c.User = viper.GetString("collectd.user")
	c.Password = viper.GetString("collectd.password")
	c.Worker = viper.GetInt("collectd.worker")
}

func init() {
	_ = viper.BindEnv("collectd.enable", "TAOS_ADAPTER_COLLECTD_ENABLE")
	pflag.Bool("collectd.enable", false, `enable collectd. Env "TAOS_ADAPTER_COLLECTD_ENABLE"`)
	viper.SetDefault("collectd.enable", false)

	_ = viper.BindEnv("collectd.port", "TAOS_ADAPTER_COLLECTD_PORT")
	pflag.Int("collectd.port", 6045, `collectd server port. Env "TAOS_ADAPTER_COLLECTD_PORT"`)
	viper.SetDefault("collectd.port", 6045)

	_ = viper.BindEnv("collectd.db", "TAOS_ADAPTER_COLLECTD_DB")
	pflag.String("collectd.db", "collectd", `collectd db name. Env "TAOS_ADAPTER_COLLECTD_DB"`)
	viper.SetDefault("collectd.db", "collectd")

	_ = viper.BindEnv("collectd.user", "TAOS_ADAPTER_COLLECTD_USER")
	pflag.String("collectd.user", common.DefaultUser, `collectd user. Env "TAOS_ADAPTER_COLLECTD_USER"`)
	viper.SetDefault("collectd.user", common.DefaultUser)

	_ = viper.BindEnv("collectd.password", "TAOS_ADAPTER_COLLECTD_PASSWORD")
	pflag.String("collectd.password", common.DefaultPassword, `collectd password. Env "TAOS_ADAPTER_COLLECTD_PASSWORD"`)
	viper.SetDefault("collectd.password", common.DefaultPassword)

	_ = viper.BindEnv("collectd.worker", "TAOS_ADAPTER_COLLECTD_WORKER")
	pflag.Int("collectd.worker", 10, `collectd write worker. Env "TAOS_ADAPTER_COLLECTD_WORKER"`)
	viper.SetDefault("collectd.worker", 10)
}

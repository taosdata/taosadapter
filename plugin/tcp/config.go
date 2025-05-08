package tcp

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Config struct {
	Enable         bool
	Host           string
	Port           uint
	EnableTCP4Only bool
}

func (c *Config) setValue() {
	c.Enable = viper.GetBool("tcp.enable")
	c.Host = viper.GetString("tcp.host")
	c.Port = uint(viper.GetInt("tcp.port"))
	c.EnableTCP4Only = viper.GetBool("tcp.enable_tcp4_only")
}

func init() {
	_ = viper.BindEnv("tcp.enable", "TCP_ENABLE")
	pflag.Bool("tcp.enable", true, "Enable TCP")
	viper.SetDefault("tcp.enable", true)
	_ = viper.BindEnv("tcp.host", "TCP_HOST")
	pflag.String("tcp.host", "", "TCP_HOST")
	viper.SetDefault("tcp.host", "")
	_ = viper.BindEnv("tcp.port", "TCP_PORT")
	pflag.Uint("tcp.port", 6042, "TCP_PORT")
	viper.SetDefault("tcp.port", 6042)
	_ = viper.BindEnv("tcp.enable_tcp4_only", "TCP_ENABLE_TCP4_ONLY")
	pflag.Bool("tcp.enable_tcp4_only", false, "Enable TCP4 only")
	viper.SetDefault("tcp.enable_tcp4_only", false)
}

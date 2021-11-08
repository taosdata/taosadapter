package opentsdbtelnet

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/taosdata/driver-go/v2/common"
)

type Config struct {
	Enable            bool
	Port              int
	TCPKeepAlive      bool
	MaxTCPConnections int
	DB                string
	User              string
	Password          string
	Worker            int
}

func (c *Config) setValue() {
	c.Enable = viper.GetBool("opentsdb_telnet.enable")
	c.Port = viper.GetInt("opentsdb_telnet.port")
	c.MaxTCPConnections = viper.GetInt("opentsdb_telnet.maxTCPConnections")
	c.TCPKeepAlive = viper.GetBool("opentsdb_telnet.tcpKeepAlive")
	c.DB = viper.GetString("opentsdb_telnet.db")
	c.User = viper.GetString("opentsdb_telnet.user")
	c.Password = viper.GetString("opentsdb_telnet.password")
	c.Worker = viper.GetInt("opentsdb_telnet.worker")
}
func init() {
	_ = viper.BindEnv("opentsdb_telnet.enable", "TAOS_ADAPTER_OPENTSDB_TELNET_ENABLE")
	pflag.Bool("opentsdb_telnet.enable", false, `enable opentsdb telnet,warning: without auth info(default false). Env "TAOS_ADAPTER_OPENTSDB_TELNET_ENABLE"`)
	viper.SetDefault("opentsdb_telnet.enable", false)

	_ = viper.BindEnv("opentsdb_telnet.port", "TAOS_ADAPTER_OPENTSDB_TELNET_PORT")
	pflag.Int("opentsdb_telnet.port", 6046, `opentsdb telnet tcp port. Env "TAOS_ADAPTER_OPENTSDB_TELNET_PORT"`)
	viper.SetDefault("opentsdb_telnet.port", 6046)

	_ = viper.BindEnv("opentsdb_telnet.maxTCPConnections", "TAOS_ADAPTER_OPENTSDB_TELNET_MAX_TCP_CONNECTIONS")
	pflag.Int("opentsdb_telnet.maxTCPConnections", 250, `max tcp connections. Env "TAOS_ADAPTER_OPENTSDB_TELNET_MAX_TCP_CONNECTIONS"`)
	viper.SetDefault("opentsdb_telnet.maxTCPConnections", 250)

	_ = viper.BindEnv("opentsdb_telnet.tcpKeepAlive", "TAOS_ADAPTER_OPENTSDB_TELNET_TCP_KEEP_ALIVE")
	pflag.Bool("opentsdb_telnet.tcpKeepAlive", false, `enable tcp keep alive. Env "TAOS_ADAPTER_OPENTSDB_TELNET_TCP_KEEP_ALIVE"`)
	viper.SetDefault("opentsdb_telnet.tcpKeepAlive", false)

	_ = viper.BindEnv("opentsdb_telnet.db", "TAOS_ADAPTER_OPENTSDB_TELNET_DB")
	pflag.String("opentsdb_telnet.db", "opentsdb_telnet", `opentsdb_telnet db name. Env "TAOS_ADAPTER_OPENTSDB_TELNET_DB"`)
	viper.SetDefault("opentsdb_telnet.db", "opentsdb_telnet")

	_ = viper.BindEnv("opentsdb_telnet.user", "TAOS_ADAPTER_OPENTSDB_TELNET_USER")
	pflag.String("opentsdb_telnet.user", common.DefaultUser, `opentsdb_telnet user. Env "TAOS_ADAPTER_OPENTSDB_TELNET_USER"`)
	viper.SetDefault("opentsdb_telnet.user", common.DefaultUser)

	_ = viper.BindEnv("opentsdb_telnet.password", "TAOS_ADAPTER_OPENTSDB_TELNET_PASSWORD")
	pflag.String("opentsdb_telnet.password", common.DefaultPassword, `opentsdb_telnet password. Env "TAOS_ADAPTER_OPENTSDB_TELNET_PASSWORD"`)
	viper.SetDefault("opentsdb_telnet.password", common.DefaultPassword)

	_ = viper.BindEnv("opentsdb_telnet.worker", "TAOS_ADAPTER_OPENTSDB_TELNET_WORKER")
	pflag.Int("opentsdb_telnet.worker", 1000, `opentsdb_telnet write worker. Env "TAOS_ADAPTER_OPENTSDB_TELNET_WORKER"`)
	viper.SetDefault("opentsdb_telnet.worker", 1000)
}

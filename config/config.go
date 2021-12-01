package config

import (
	"fmt"
	"os"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/version"
)

type Config struct {
	Cors          CorsConfig
	TaosConfigDir string
	Debug         bool
	Port          int
	LogLevel      string
	EnableGzip    bool
	SSl           SSl
	Log           Log
	Pool          Pool
}

var (
	Conf *Config
)

func Init() {
	viper.SetConfigType("toml")
	viper.SetConfigName("taosadapter")
	viper.AddConfigPath("/etc/taos")
	cp := pflag.StringP("config", "c", "", "config path default /etc/taos/taosadapter.toml")
	v := pflag.Bool("version", false, "Print the version and exit")
	help := pflag.Bool("help", false, "Print this help message and exit")
	pflag.Parse()
	if *help {
		fmt.Fprintf(os.Stderr, "Usage of taosadapter v%s-%s:\n", version.Version, version.CommitID)
		pflag.PrintDefaults()
		os.Exit(0)
	}
	if *v {
		fmt.Printf("taosadapter v%s-%s\n", version.Version, version.CommitID)
		os.Exit(0)
	}
	if *cp != "" {
		viper.SetConfigFile(*cp)
	}
	viper.SetEnvPrefix("taosadapter")
	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		panic(err)
	}
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			fmt.Println("config file not found")
		} else {
			panic(err)
		}
	}
	Conf = &Config{
		Debug:         viper.GetBool("debug"),
		Port:          viper.GetInt("port"),
		LogLevel:      viper.GetString("logLevel"),
		TaosConfigDir: viper.GetString("taosConfigDir"),
		EnableGzip:    viper.GetBool("enableGzip"),
	}
	Conf.Log.setValue()
	Conf.Cors.setValue()
	Conf.SSl.setValue()
	Conf.Pool.setValue()
}

//arg > file > env
func init() {
	viper.SetDefault("debug", false)
	_ = viper.BindEnv("debug", "TAOS_ADAPTER_DEBUG")
	pflag.Bool("debug", false, `enable debug mode. Env "TAOS_ADAPTER_DEBUG"`)

	viper.SetDefault("port", 6041)
	_ = viper.BindEnv("port", "TAOS_ADAPTER_PORT")
	pflag.IntP("port", "P", 6041, `http port. Env "TAOS_ADAPTER_PORT"`)

	viper.SetDefault("logLevel", "info")
	_ = viper.BindEnv("logLevel", "TAOS_ADAPTER_LOG_LEVEL")
	pflag.String("logLevel", "info", `log level (panic fatal error warn warning info debug trace). Env "TAOS_ADAPTER_LOG_LEVEL"`)

	viper.SetDefault("taosConfigDir", "")
	_ = viper.BindEnv("taosConfigDir", "TAOS_ADAPTER_TAOS_CONFIG_FILE")
	pflag.String("taosConfigDir", "", `load taos client config path. Env "TAOS_ADAPTER_TAOS_CONFIG_FILE"`)

	viper.SetDefault("enableGzip", false)
	_ = viper.BindEnv("enableGzip", "TAOS_ADAPTER_ENABLE_GZIP")
	pflag.Bool("enableGzip", false, `enable http gzip response. Env "TAOS_ADAPTER_ENABLE_GZIP"`)

	initLog()
	initSSL()
	initCors()
	initPool()

	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		panic(err)
	}
}

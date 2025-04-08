package config

import (
	"fmt"
	"os"
	"runtime"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/v3/thread"
	"github.com/taosdata/taosadapter/v3/version"
	"go.uber.org/automaxprocs/maxprocs"
)

type Config struct {
	InstanceID          uint8
	Cors                CorsConfig
	TaosConfigDir       string
	MaxSyncMethodLimit  int
	MaxAsyncMethodLimit int
	Debug               bool
	Port                int
	LogLevel            string
	RestfulRowLimit     int
	HttpCodeServerError bool
	SMLAutoCreateDB     bool
	Log                 Log
	Pool                Pool
	Monitor             Monitor
	UploadKeeper        UploadKeeper
}

var (
	Conf *Config
)

func Init() {
	viper.SetConfigType("toml")
	viper.SetConfigName(fmt.Sprintf("%sadapter", version.CUS_PROMPT))
	var cp *string
	switch runtime.GOOS {
	case "windows":
		viper.AddConfigPath(fmt.Sprintf("C:\\%s\\cfg", version.CUS_NAME))
		cp = pflag.StringP("config", "c", "", fmt.Sprintf("config path default C:\\%s\\cfg\\%sadapter.toml", version.CUS_NAME, version.CUS_PROMPT))
	default:
		viper.AddConfigPath(fmt.Sprintf("/etc/%s", version.CUS_PROMPT))
		cp = pflag.StringP("config", "c", "", fmt.Sprintf("config path default /etc/%s/%sadapter.toml", version.CUS_PROMPT, version.CUS_PROMPT))
	}
	v := pflag.BoolP("version", "V", false, "Print the version and exit")
	help := pflag.Bool("help", false, "Print this help message and exit")
	pflag.Parse()
	if *help {
		_, _ = fmt.Fprintf(os.Stderr, "Usage of %sadapter v%s-%s:\n", version.CUS_PROMPT, version.Version, version.CommitID)
		pflag.PrintDefaults()
		os.Exit(0)
	}
	if *v {
		fmt.Printf("%sAdapter version: %s\n", version.CUS_PROMPT, version.Version)
		fmt.Printf("git: %s\n", version.CommitID)
		fmt.Printf("build: %s\n", version.BuildInfo)
		os.Exit(0)
	}
	if *cp != "" {
		viper.SetConfigFile(*cp)
	}
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
		TaosConfigDir:       viper.GetString("taosConfigDir"),
		Debug:               viper.GetBool("debug"),
		Port:                viper.GetInt("port"),
		LogLevel:            viper.GetString("logLevel"),
		RestfulRowLimit:     viper.GetInt("restfulRowLimit"),
		HttpCodeServerError: viper.GetBool("httpCodeServerError"),
		SMLAutoCreateDB:     viper.GetBool("smlAutoCreateDB"),
		InstanceID:          uint8(viper.GetInt("instanceId")),
		MaxSyncMethodLimit:  viper.GetInt("maxSyncConcurrentLimit"),
		MaxAsyncMethodLimit: viper.GetInt("maxAsyncConcurrentLimit"),
	}
	Conf.Log.setValue()
	Conf.Cors.setValue()
	Conf.Pool.setValue()
	Conf.Monitor.setValue()
	Conf.UploadKeeper.setValue()
	// set log level default value: info
	if Conf.LogLevel == "" {
		Conf.LogLevel = "info"
	}
	if viper.IsSet("log.level") {
		Conf.LogLevel = Conf.Log.Level
	} else {
		viper.Set("log.level", "")
	}
	if !viper.IsSet("logLevel") {
		viper.Set("logLevel", "")
	}
	maxAsyncMethodLimit := Conf.MaxAsyncMethodLimit
	if maxAsyncMethodLimit == 0 {
		maxAsyncMethodLimit = runtime.NumCPU()
	}
	thread.AsyncSemaphore = thread.NewSemaphore(maxAsyncMethodLimit)

	maxSyncMethodLimit := Conf.MaxSyncMethodLimit
	if maxSyncMethodLimit == 0 {
		maxSyncMethodLimit = runtime.NumCPU()
	}
	thread.SyncSemaphore = thread.NewSemaphore(maxSyncMethodLimit)
}

// arg > file > env
func init() {
	// get the number of CPU cores, and set GOMAXPROCS to match the number of CPU cores
	_, _ = maxprocs.Set()
	viper.SetDefault("debug", true)
	_ = viper.BindEnv("debug", "TAOS_ADAPTER_DEBUG")
	pflag.Bool("debug", true, `enable debug mode. Env "TAOS_ADAPTER_DEBUG"`)

	viper.SetDefault("httpCodeServerError", false)
	_ = viper.BindEnv("httpCodeServerError", "TAOS_ADAPTER_HTTP_CODE_SERVER_ERROR")
	pflag.Bool("httpCodeServerError", false, `Use a non-200 http status code when server returns an error. Env "TAOS_ADAPTER_HTTP_CODE_SERVER_ERROR"`)

	viper.SetDefault("port", 6041)
	_ = viper.BindEnv("port", "TAOS_ADAPTER_PORT")
	pflag.IntP("port", "P", 6041, `http port. Env "TAOS_ADAPTER_PORT"`)

	_ = viper.BindEnv("logLevel", "TAOS_ADAPTER_LOG_LEVEL")
	pflag.String("logLevel", "info", `log level (trace debug info warning error). Env "TAOS_ADAPTER_LOG_LEVEL"`)

	viper.SetDefault("taosConfigDir", "")
	_ = viper.BindEnv("taosConfigDir", "TAOS_ADAPTER_TAOS_CONFIG_FILE")
	pflag.String("taosConfigDir", "", `load taos client config path. Env "TAOS_ADAPTER_TAOS_CONFIG_FILE"`)

	viper.SetDefault("restfulRowLimit", -1)
	_ = viper.BindEnv("restfulRowLimit", "TAOS_ADAPTER_RESTFUL_ROW_LIMIT")
	pflag.Int("restfulRowLimit", -1, `restful returns the maximum number of rows (-1 means no limit). Env "TAOS_ADAPTER_RESTFUL_ROW_LIMIT"`)

	viper.SetDefault("smlAutoCreateDB", false)
	_ = viper.BindEnv("smlAutoCreateDB", "TAOS_ADAPTER_SML_AUTO_CREATE_DB")
	pflag.Bool("smlAutoCreateDB", false, `Whether to automatically create db when writing with schemaless. Env "TAOS_ADAPTER_SML_AUTO_CREATE_DB"`)

	viper.SetDefault("instanceId", 32)
	_ = viper.BindEnv("instanceId", "TAOS_ADAPTER_INSTANCE_ID")
	pflag.Int("instanceId", 32, `instance ID. Env "TAOS_ADAPTER_INSTANCE_ID"`)

	viper.SetDefault("maxSyncConcurrentLimit", 0)
	_ = viper.BindEnv("maxSyncConcurrentLimit", "TAOS_ADAPTER_MAX_SYNC_CONCURRENT_LIMIT")
	pflag.Int("maxSyncConcurrentLimit", 0, `The maximum number of concurrent calls allowed for the C synchronized method. 0 means use CPU core count. Env "TAOS_ADAPTER_MAX_SYNC_CONCURRENT_LIMIT"`)

	viper.SetDefault("maxAsyncConcurrentLimit", 0)
	_ = viper.BindEnv("maxAsyncConcurrentLimit", "TAOS_ADAPTER_MAX_ASYNC_CONCURRENT_LIMIT")
	pflag.Int("maxAsyncConcurrentLimit", 0, `The maximum number of concurrent calls allowed for the C asynchronous method. 0 means use CPU core count. Env "TAOS_ADAPTER_MAX_ASYNC_CONCURRENT_LIMIT"`)

	initLog()
	initCors()
	initPool()
	initMonitor()
	initUploadKeeper()
	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		panic(err)
	}
}

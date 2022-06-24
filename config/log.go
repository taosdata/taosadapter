package config

import (
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Log struct {
	Path          string
	RotationCount uint
	RotationTime  time.Duration
	RotationSize  uint

	EnableRecordHttpSql bool
	SqlRotationCount    uint
	SqlRotationTime     time.Duration
	SqlRotationSize     uint
}

func initLog() {
	viper.SetDefault("log.path", "/var/log/taos")
	_ = viper.BindEnv("log.path", "TAOS_ADAPTER_LOG_PATH")
	pflag.String("log.path", "/var/log/taos", `log path. Env "TAOS_ADAPTER_LOG_PATH"`)

	viper.SetDefault("log.rotationCount", 30)
	_ = viper.BindEnv("log.rotationCount", "TAOS_ADAPTER_LOG_ROTATION_COUNT")
	pflag.Uint("log.rotationCount", 30, `log rotation count. Env "TAOS_ADAPTER_LOG_ROTATION_COUNT"`)

	viper.SetDefault("log.rotationTime", time.Hour*24)
	_ = viper.BindEnv("log.rotationTime", "TAOS_ADAPTER_LOG_ROTATION_TIME")
	pflag.Duration("log.rotationTime", time.Hour*24, `log rotation time. Env "TAOS_ADAPTER_LOG_ROTATION_TIME"`)

	viper.SetDefault("log.rotationSize", "1GB")
	_ = viper.BindEnv("log.rotationSize", "TAOS_ADAPTER_LOG_ROTATION_SIZE")
	pflag.String("log.rotationSize", "1GB", `log rotation size(KB MB GB), must be a positive integer. Env "TAOS_ADAPTER_LOG_ROTATION_SIZE"`)

	viper.SetDefault("log.enableRecordHttpSql", false)
	_ = viper.BindEnv("log.enableRecordHttpSql", "TAOS_ADAPTER_LOG_ENABLE_RECORD_HTTP_SQL")
	pflag.Bool("log.enableRecordHttpSql", false, `whether to record http sql. Env "TAOS_ADAPTER_LOG_ENABLE_RECORD_HTTP_SQL"`)

	viper.SetDefault("log.sqlRotationCount", 2)
	_ = viper.BindEnv("log.sqlRotationCount", "TAOS_ADAPTER_LOG_SQL_ROTATION_COUNT")
	pflag.Uint("log.sqlRotationCount", 2, `record sql log rotation count. Env "TAOS_ADAPTER_LOG_SQL_ROTATION_COUNT"`)

	viper.SetDefault("log.sqlRotationTime", time.Hour*24)
	_ = viper.BindEnv("log.sqlRotationTime", "TAOS_ADAPTER_LOG_SQL_ROTATION_TIME")
	pflag.Duration("log.sqlRotationTime", time.Hour*24, `record sql log rotation time. Env "TAOS_ADAPTER_LOG_SQL_ROTATION_TIME"`)

	viper.SetDefault("log.sqlRotationSize", "1GB")
	_ = viper.BindEnv("log.sqlRotationSize", "TAOS_ADAPTER_LOG_SQL_ROTATION_SIZE")
	pflag.String("log.sqlRotationSize", "1GB", `record sql log rotation size(KB MB GB), must be a positive integer. Env "TAOS_ADAPTER_LOG_SQL_ROTATION_SIZE"`)
}

func (l *Log) setValue() {
	l.Path = viper.GetString("log.path")
	l.RotationCount = viper.GetUint("log.rotationCount")
	l.RotationTime = viper.GetDuration("log.rotationTime")
	l.RotationSize = viper.GetSizeInBytes("log.rotationSize")
	l.EnableRecordHttpSql = viper.GetBool("log.enableRecordHttpSql")
	l.SqlRotationCount = viper.GetUint("log.sqlRotationCount")
	l.SqlRotationTime = viper.GetDuration("log.sqlRotationTime")
	l.SqlRotationSize = viper.GetSizeInBytes("log.sqlRotationSize")
}

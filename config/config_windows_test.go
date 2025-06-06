//go:build windows
// +build windows

package config

import (
	"runtime"
	"testing"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/stretchr/testify/assert"
)

// @author: xftan
// @date: 2021/12/14 15:00
// @description: test init
func TestInit(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "default",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Init()
			assert.Equal(t, &Config{
				InstanceID: 32,
				Cors: CorsConfig{
					AllowAllOrigins:  true,
					AllowOrigins:     []string{},
					AllowHeaders:     []string{},
					ExposeHeaders:    []string{},
					AllowCredentials: false,
					AllowWebSockets:  false,
				},
				TaosConfigDir:       "",
				MaxSyncMethodLimit:  runtime.GOMAXPROCS(0),
				MaxAsyncMethodLimit: runtime.GOMAXPROCS(0),
				Debug:               true,
				Port:                6041,
				LogLevel:            "info",
				RestfulRowLimit:     -1,
				HttpCodeServerError: false,
				SMLAutoCreateDB:     false,
				Log: Log{
					Level:               "info",
					Path:                "C:\\TDengine\\log",
					RotationCount:       30,
					RotationTime:        time.Hour * 24,
					RotationSize:        1 * 1024 * 1024 * 1024, // 1G
					KeepDays:            30,
					Compress:            false,
					ReservedDiskSize:    1 * 1024 * 1024 * 1024,
					EnableRecordHttpSql: false,
					SqlRotationCount:    2,
					SqlRotationTime:     time.Hour * 24,
					SqlRotationSize:     1 * 1024 * 1024 * 1024,
				},
				Pool: Pool{
					MaxConnect:  runtime.GOMAXPROCS(0) * 2,
					MaxIdle:     0,
					IdleTimeout: 0,
					WaitTimeout: 60,
					MaxWait:     0,
				},
				Monitor: Monitor{
					Disable:                   true,
					CollectDuration:           3 * time.Second,
					InCGroup:                  false,
					PauseQueryMemoryThreshold: 70,
					PauseAllMemoryThreshold:   80,
					Identity:                  "",
				},
				UploadKeeper: UploadKeeper{
					Enable:        true,
					Url:           "http://127.0.0.1:6043/adapter_report",
					Interval:      15 * time.Second,
					Timeout:       5 * time.Second,
					RetryTimes:    3,
					RetryInterval: 5 * time.Second,
				},
			}, Conf)
			corsC := Conf.Cors.GetConfig()
			assert.Equal(
				t,
				cors.Config{
					AllowAllOrigins:        true,
					AllowOrigins:           nil,
					AllowOriginFunc:        (func(string) bool)(nil),
					AllowMethods:           []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"},
					AllowHeaders:           []string{"Origin", "Content-Length", "Content-Type", "Authorization"},
					AllowCredentials:       false,
					ExposeHeaders:          []string{"Authorization"},
					MaxAge:                 12 * time.Hour,
					AllowWildcard:          true,
					AllowBrowserExtensions: false,
					AllowWebSockets:        false,
					AllowFiles:             false,
				},
				corsC,
			)
		})
	}
}

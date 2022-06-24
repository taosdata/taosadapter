package config

import (
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
				Cors: CorsConfig{
					AllowAllOrigins:  true,
					AllowOrigins:     []string{},
					AllowHeaders:     []string{},
					ExposeHeaders:    []string{},
					AllowCredentials: false,
					AllowWebSockets:  false,
				},
				TaosConfigDir:   "",
				Debug:           false,
				Port:            6041,
				LogLevel:        "info",
				SSl:             SSl{Enable: false, CertFile: "", KeyFile: ""},
				RestfulRowLimit: -1,
				Log: Log{
					Path:                "/var/log/taos",
					RotationCount:       30,
					RotationTime:        time.Hour * 24,
					RotationSize:        1 * 1024 * 1024 * 1024, // 1G
					EnableRecordHttpSql: false,
					SqlRotationCount:    2,
					SqlRotationTime:     time.Hour * 24,
					SqlRotationSize:     1 * 1024 * 1024 * 1024,
				},
				Pool: Pool{
					MaxConnect:  4000,
					MaxIdle:     4000,
					IdleTimeout: time.Hour,
				},
				Monitor: Monitor{
					CollectDuration:           3 * time.Second,
					InCGroup:                  false,
					PauseQueryMemoryThreshold: 70,
					PauseAllMemoryThreshold:   80,
					Identity:                  "",
					WriteToTD:                 true,
					User:                      "root",
					Password:                  "taosdata",
					WriteInterval:             30 * time.Second,
				},
			}, Conf)
			corsC := Conf.Cors.GetConfig()
			assert.Equal(
				t,
				cors.Config{
					AllowAllOrigins:        true,
					AllowOrigins:           nil,
					AllowOriginFunc:        (func(string) bool)(nil),
					AllowMethods:           []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"},
					AllowHeaders:           []string{"Origin", "Content-Length", "Content-Type"},
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

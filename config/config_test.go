package config

import (
	"testing"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/stretchr/testify/assert"
)

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
					AllowAllOrigins:  false,
					AllowOrigins:     []string{},
					AllowHeaders:     []string{},
					ExposeHeaders:    []string{},
					AllowCredentials: false,
					AllowWebSockets:  false,
				},
				TaosConfigDir: "",
				Debug:         false,
				Port:          6041,
				LogLevel:      "info",
				SSl:           SSl{Enable: false, CertFile: "", KeyFile: ""},
				Log: Log{
					Path:          "/var/log/taos",
					RotationCount: 30,
					RotationTime:  time.Hour * 24,
					RotationSize:  1 * 1024 * 1024 * 1024, // 1G
				},
				Pool: Pool{
					MaxConnect:  4000,
					MaxIdle:     4000,
					IdleTimeout: time.Hour,
				},
			}, Conf)
			corsC := Conf.Cors.GetConfig()
			assert.Equal(
				t,
				cors.Config{
					AllowAllOrigins:        false,
					AllowOrigins:           []string{"http://127.0.0.1"},
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
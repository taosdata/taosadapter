package config

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	defaultHTTPReadHeaderTimeout = 10 * time.Second
	defaultHTTPReadTimeout       = 120 * time.Second
	defaultHTTPWriteTimeout      = 0
	defaultHTTPIdleTimeout       = 90 * time.Second
	defaultHTTPMaxHeaderBytes    = 1 << 20 // 1 MiB
)

type Http struct {
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	MaxHeaderBytes    int
}

func initHTTP(v *viper.Viper) {
	v.SetDefault("http.readHeaderTimeout", defaultHTTPReadHeaderTimeout)
	_ = v.BindEnv("http.readHeaderTimeout", "TAOS_ADAPTER_HTTP_READ_HEADER_TIMEOUT")

	v.SetDefault("http.readTimeout", defaultHTTPReadTimeout)
	_ = v.BindEnv("http.readTimeout", "TAOS_ADAPTER_HTTP_READ_TIMEOUT")

	v.SetDefault("http.writeTimeout", defaultHTTPWriteTimeout)
	_ = v.BindEnv("http.writeTimeout", "TAOS_ADAPTER_HTTP_WRITE_TIMEOUT")

	v.SetDefault("http.idleTimeout", defaultHTTPIdleTimeout)
	_ = v.BindEnv("http.idleTimeout", "TAOS_ADAPTER_HTTP_IDLE_TIMEOUT")

	v.SetDefault("http.maxHeaderBytes", defaultHTTPMaxHeaderBytes)
	_ = v.BindEnv("http.maxHeaderBytes", "TAOS_ADAPTER_HTTP_MAX_HEADER_BYTES")
}

func registerHTTPFlags() {
	pflag.Duration("http.readHeaderTimeout", defaultHTTPReadHeaderTimeout, `Maximum duration to read request headers. Env "TAOS_ADAPTER_HTTP_READ_HEADER_TIMEOUT"`)
	pflag.Duration("http.readTimeout", defaultHTTPReadTimeout, `Maximum duration to read the full request (headers + body). 0 disables timeout. Env "TAOS_ADAPTER_HTTP_READ_TIMEOUT"`)
	pflag.Duration("http.writeTimeout", defaultHTTPWriteTimeout, `Maximum duration before timing out response writes. 0 disables timeout. Env "TAOS_ADAPTER_HTTP_WRITE_TIMEOUT"`)
	pflag.Duration("http.idleTimeout", defaultHTTPIdleTimeout, `Maximum amount of time to wait for the next request when keep-alive is enabled. Env "TAOS_ADAPTER_HTTP_IDLE_TIMEOUT"`)
	pflag.Int("http.maxHeaderBytes", defaultHTTPMaxHeaderBytes, `Maximum size of request headers in bytes. Env "TAOS_ADAPTER_HTTP_MAX_HEADER_BYTES"`)
}

func (h *Http) setValue(v *viper.Viper) error {
	h.ReadHeaderTimeout = v.GetDuration("http.readHeaderTimeout")
	if h.ReadHeaderTimeout < 0 {
		return fmt.Errorf("http.readHeaderTimeout must be >= 0, got: %s", h.ReadHeaderTimeout)
	}
	h.ReadTimeout = v.GetDuration("http.readTimeout")
	if h.ReadTimeout < 0 {
		return fmt.Errorf("http.readTimeout must be >= 0, got: %s", h.ReadTimeout)
	}
	h.WriteTimeout = v.GetDuration("http.writeTimeout")
	if h.WriteTimeout < 0 {
		return fmt.Errorf("http.writeTimeout must be >= 0, got: %s", h.WriteTimeout)
	}
	h.IdleTimeout = v.GetDuration("http.idleTimeout")
	if h.IdleTimeout < 0 {
		return fmt.Errorf("http.idleTimeout must be >= 0, got: %s", h.IdleTimeout)
	}
	h.MaxHeaderBytes = v.GetInt("http.maxHeaderBytes")
	if h.MaxHeaderBytes <= 0 {
		return fmt.Errorf("http.maxHeaderBytes must be > 0, got: %d", h.MaxHeaderBytes)
	}
	return nil
}

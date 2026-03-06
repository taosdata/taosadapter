package config

import (
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPSetValueDefault(t *testing.T) {
	v := viper.New()
	initHTTP(v)
	h := &Http{}
	err := h.setValue(v)
	require.NoError(t, err)
	assert.Equal(t, &Http{
		ReadHeaderTimeout: defaultHTTPReadHeaderTimeout,
		ReadTimeout:       defaultHTTPReadTimeout,
		WriteTimeout:      defaultHTTPWriteTimeout,
		IdleTimeout:       defaultHTTPIdleTimeout,
		MaxHeaderBytes:    defaultHTTPMaxHeaderBytes,
	}, h)
}

func TestHTTPSetValueInvalid(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value interface{}
	}{
		{
			name:  "negative readHeaderTimeout",
			key:   "http.readHeaderTimeout",
			value: -1 * time.Second,
		},
		{
			name:  "negative readTimeout",
			key:   "http.readTimeout",
			value: -1 * time.Second,
		},
		{
			name:  "negative writeTimeout",
			key:   "http.writeTimeout",
			value: -1 * time.Second,
		},
		{
			name:  "negative idleTimeout",
			key:   "http.idleTimeout",
			value: -1 * time.Second,
		},
		{
			name:  "zero maxHeaderBytes",
			key:   "http.maxHeaderBytes",
			value: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := viper.New()
			initHTTP(v)
			v.Set(tt.key, tt.value)
			h := &Http{}
			err := h.setValue(v)
			require.Error(t, err)
		})
	}
}

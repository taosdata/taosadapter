package system

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
)

func TestNewProgramApplyHTTPServerConfig(t *testing.T) {
	oldConf := config.Conf
	t.Cleanup(func() {
		config.Conf = oldConf
	})

	config.Conf = &config.Config{
		Port: 6041,
		Http: &config.Http{
			ReadHeaderTimeout: 6 * time.Second,
			ReadTimeout:       70 * time.Second,
			WriteTimeout:      0,
			IdleTimeout:       50 * time.Second,
			MaxHeaderBytes:    256 * 1024,
		},
	}

	p := newProgram(gin.New(), func(server *http.Server) {}, nil)
	assert.Equal(t, ":6041", p.server.Addr)
	assert.Equal(t, 6*time.Second, p.server.ReadHeaderTimeout)
	assert.Equal(t, 70*time.Second, p.server.ReadTimeout)
	assert.Equal(t, time.Duration(0), p.server.WriteTimeout)
	assert.Equal(t, 50*time.Second, p.server.IdleTimeout)
	assert.Equal(t, 256*1024, p.server.MaxHeaderBytes)
}

func TestStart(t *testing.T) {
	r := Init()
	config.Conf.Port = 39999
	go func() {
		Start(r, func(server *http.Server) {
			ln, err := net.Listen("tcp", server.Addr)
			if err != nil {
				logger.Fatalf("listen: %s", err)
			}
			if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
				logger.Fatalf("listen: %s", err)
			}
		})
	}()
	time.Sleep(time.Second)
	success := false
	for i := 0; i < 3; i++ {
		resp, err := http.Get("http://127.0.0.1:39999/-/ping")
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		_ = resp.Body.Close()
		success = true
		break
	}
	if !success {
		t.Fatal("failed to start server")
	}
	err := testProg.Stop(nil)
	assert.NoError(t, err)
	time.Sleep(time.Second)
	_, err = http.Get("http://127.0.0.1:39999/-/ping")
	assert.Error(t, err)
}

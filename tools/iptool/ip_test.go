package iptool

import (
	"net"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetRealIPWithXRealIPHeader(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com", nil)
	req.Header.Set("X-Real-Ip", "192.168.1.1")

	ip := GetRealIP(req)

	assert.Equal(t, "192.168.1.1", ip.String())
}

func TestGetRealIPWithRemoteAddr(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com", nil)
	req.RemoteAddr = "192.168.1.2:1234"

	ip := GetRealIP(req)

	assert.Equal(t, "192.168.1.2", ip.String())
}

func TestGetRealIPWithNoIP(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com", nil)

	ip := GetRealIP(req)
	host, _, _ := net.SplitHostPort(req.RemoteAddr)
	assert.Equal(t, host, ip.String())
}

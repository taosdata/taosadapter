package iptool

import (
	"net"
	"net/http"
	"strings"
)

func GetRealIP(r *http.Request) net.IP {
	ip := r.Header.Get("X-Real-Ip")
	if ip != "" {
		return net.ParseIP(ip)
	}
	host, _, _ := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	return net.ParseIP(host)
}

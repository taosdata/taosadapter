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
	parsedIP := net.ParseIP(host)
	if parsedIP != nil {
		return parsedIP
	}
	// Fallback to resolving the host if the host has zone information
	ipAddr, err := net.ResolveIPAddr("ip", host)
	if err != nil {
		return nil
	}
	return ipAddr.IP
}

// Copyright (c) 2021 TAOS Data, Inc.
//
// SPDX-License-Identifier: MIT
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package system

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/config"
	metricscontroller "github.com/taosdata/taosadapter/v3/controller/metrics"
	"github.com/taosdata/taosadapter/v3/log"
)

// newMetricsRouter builds a router with the same middleware chain as production
// (see Init: createRouter(..., enableGzip=true)) and registers the real
// /metrics controller on it.
//
// config.Init registers global pflags and panics if called twice, and TestStart
// in this package already calls it via Init, so only initialise when config.Conf
// is still nil.
func newMetricsRouter() *gin.Engine {
	if config.Conf == nil {
		config.Init()
		log.ConfigLog()
	}
	router := createRouter(false, &config.CorsConfig{AllowAllOrigins: true}, true)
	metricscontroller.Controller{}.Init(router)
	return router
}

// gunzip decompresses exactly one gzip layer.
func gunzip(t *testing.T, b []byte) ([]byte, error) {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer func() { _ = zr.Close() }()
	return io.ReadAll(zr)
}

// isGzip reports whether b starts with the gzip magic number.
func isGzip(b []byte) bool {
	return len(b) >= 2 && b[0] == 0x1f && b[1] == 0x8b
}

// TestMetricsGzipAcceptEncodingGzip reproduces the bug where GET /metrics with
// "Accept-Encoding: gzip" returns a body that is gzip compressed twice, so
// browsers and Prometheus scrapers receive an undecodable .gz payload.
//
// Both the global gin gzip middleware (system/main.go) and promhttp's own
// content negotiation (controller/metrics/controller.go uses the default
// HandlerOpts, which offers gzip) compress the response. promhttp sets
// Content-Encoding with Set, overwriting the middleware's header, so only one
// "gzip" is advertised while two layers are actually applied.
func TestMetricsGzipAcceptEncodingGzip(t *testing.T) {
	router := newMetricsRouter()

	req := httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	req.RemoteAddr = "127.0.0.1:33001"
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()
	body := w.Body.Bytes()

	require.Equal(t, 200, resp.StatusCode)
	// Exactly one Content-Encoding: gzip is advertised to the client.
	assert.Equal(t, []string{"gzip"}, resp.Header.Values("Content-Encoding"))
	require.True(t, isGzip(body), "body should be gzip encoded, got magic %x", body[:min(4, len(body))])

	// A client honouring Content-Encoding: gzip decompresses exactly once.
	decoded, err := gunzip(t, body)
	require.NoError(t, err, "first gunzip must succeed")

	// After that single layer the payload must be the Prometheus text
	// exposition format. Today it is still gzip, which is the bug.
	assert.False(t, isGzip(decoded),
		"body is gzip compressed twice: after one gunzip the payload is still gzip (magic %x)",
		decoded[:min(4, len(decoded))])
	assert.True(t, strings.Contains(string(decoded), "promhttp_metric_handler_requests_total"),
		"after one gunzip the payload should be Prometheus text exposition format, got %q",
		string(decoded[:min(64, len(decoded))]))
}

// TestMetricsGzipWithQueryString guards the exclusion matching. The middleware
// matches against req.URL.Path, not RequestURI, so a query string must not
// defeat the exclusion and reintroduce double compression.
func TestMetricsGzipWithQueryString(t *testing.T) {
	router := newMetricsRouter()

	req := httptest.NewRequest("GET", "/metrics?family=go", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	req.RemoteAddr = "127.0.0.1:33003"
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()
	body := w.Body.Bytes()

	require.Equal(t, 200, resp.StatusCode)
	require.True(t, isGzip(body), "body should be gzip encoded")

	decoded, err := gunzip(t, body)
	require.NoError(t, err, "first gunzip must succeed")
	assert.False(t, isGzip(decoded), "body must not be gzip compressed twice when a query string is present")
	assert.True(t, strings.Contains(string(decoded), "promhttp_metric_handler_requests_total"),
		"after one gunzip the payload should be Prometheus text exposition format")
}

// TestGzipStillAppliedOnOtherRoutes ensures excluding /metrics did not disable
// gzip for the rest of the API.
func TestGzipStillAppliedOnOtherRoutes(t *testing.T) {
	router := newMetricsRouter()
	payload := strings.Repeat("taosadapter gzip payload ", 200)
	router.GET("/gzip-probe", func(c *gin.Context) {
		c.String(200, payload)
	})

	req := httptest.NewRequest("GET", "/gzip-probe", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	req.RemoteAddr = "127.0.0.1:33004"
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()
	body := w.Body.Bytes()

	require.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, []string{"gzip"}, resp.Header.Values("Content-Encoding"))
	require.True(t, isGzip(body), "other routes should still be gzip compressed")

	decoded, err := gunzip(t, body)
	require.NoError(t, err)
	assert.Equal(t, payload, string(decoded), "single gzip layer should decode to the original payload")
}

// TestMetricsGzipNoAcceptEncoding documents that the endpoint is correct when
// the client does not ask for gzip, which is why plain `curl` looks fine while
// browsers and scrapers (which send Accept-Encoding: gzip) get a .gz file.
func TestMetricsGzipNoAcceptEncoding(t *testing.T) {
	router := newMetricsRouter()

	req := httptest.NewRequest("GET", "/metrics", nil)
	req.RemoteAddr = "127.0.0.1:33002"
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()
	body := w.Body.Bytes()

	require.Equal(t, 200, resp.StatusCode)
	assert.Empty(t, resp.Header.Values("Content-Encoding"))
	assert.False(t, isGzip(body), "body should be plaintext")
	assert.True(t, strings.Contains(string(body), "promhttp_metric_handler_requests_total"),
		"body should be Prometheus text exposition format, got %q", string(body[:min(64, len(body))]))
}

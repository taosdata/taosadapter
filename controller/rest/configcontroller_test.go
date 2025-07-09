package rest

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

func TestChangeConfig(t *testing.T) {
	baseLevel := log.GetLogLevel().String()
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPut, "/config", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code, w.Body.String())

	// wrong password
	user := "root"
	password := "wrong"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPut, "/config", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code, w.Body.String())

	// whitelist error
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPut, "/config", nil)
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 403, w.Code, w.Body.String())

	// no body
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body := strings.NewReader("")
	req, _ = http.NewRequest(http.MethodPut, "/config", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code, w.Body.String())

	// wrong json
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body = strings.NewReader("xxx")
	req, _ = http.NewRequest(http.MethodPut, "/config", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code, w.Body.String())

	// wrong log.level
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body = strings.NewReader(`{"log.level":"wrong"}`)
	req, _ = http.NewRequest(http.MethodPut, "/config", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code, w.Body.String())

	// success
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()

	body = strings.NewReader(`{"log.level":"warn"}`)
	req, _ = http.NewRequest(http.MethodPut, "/config", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
	assert.Equal(t, logrus.WarnLevel, log.GetLogLevel())
	t.Log(log.GetLogLevel())

	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`{"log.level":"%s"}`, baseLevel))
	req, _ = http.NewRequest(http.MethodPut, "/config", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
}

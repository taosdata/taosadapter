package rest

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVgID(t *testing.T) {
	//
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists test_vgid")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.Header.Set("Authorization", "Basic:cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, "/rest/vgid?db=test_vgid&table=test", nil)
	req.Header.Set("Authorization", "Basic:cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatal("get vgID fail ", w.Code, w.Body)
	}
}

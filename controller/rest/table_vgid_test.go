package rest

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestVgID(t *testing.T) {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/rest/vgid?db=test&table=test'", nil)
	req.Header.Set("Authorization", "Basic:cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatal("get vgID fail")
	}
}

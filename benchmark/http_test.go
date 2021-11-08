package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkRoute(b *testing.B) {
	router := setupRouter()
	w := httptest.NewRecorder()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest("POST", "/rest/sql", nil)
		router.ServeHTTP(w, req)
		assert.Equal(b, 200, w.Code)
	}
}

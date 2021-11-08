package async

import (
	"testing"
)

func BenchmarkName(b *testing.B) {
	pool := NewHandlerPool(1)
	for i := 0; i < b.N; i++ {
		h := pool.Get()
		pool.Put(h)
	}
}

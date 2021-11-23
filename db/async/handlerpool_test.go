package async

import (
	"testing"
	"time"
)

func BenchmarkName(b *testing.B) {
	pool := NewHandlerPool(1)
	for i := 0; i < b.N; i++ {
		h := pool.Get()
		pool.Put(h)
	}
}

func TestNewHandlerPool(t *testing.T) {
	type args struct {
		count int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test",
			args: args{
				count: 100,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewHandlerPool(tt.args.count)
			l := make([]*Handler, tt.args.count)
			for i := 0; i < tt.args.count; i++ {
				l[i] = got.Get()
			}
			for _, handler := range l {
				got.Put(handler)
			}
		})
	}
}

func TestHandlerPool_Get(t *testing.T) {
	pool := NewHandlerPool(1)
	h := pool.Get()
	go func() {
		time.Sleep(time.Millisecond)
		pool.Put(h)
	}()
	h2 := pool.Get()
	pool.Put(h2)
}

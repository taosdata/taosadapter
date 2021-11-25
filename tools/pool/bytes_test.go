package pool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBytesPoolGet(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "test",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := BytesPoolGet()
			defer BytesPoolPut(b)
			b.WriteString("hello")
			b.WriteByte(',')
			b.WriteString("world")
			assert.Equal(t, "hello,world", b.String())
		})
	}
}

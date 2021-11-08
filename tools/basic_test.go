package tools

import "testing"

func BenchmarkDecodeBasic(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _, err := DecodeBasic("cm9vdDp0YW9zZGF0YQ==")
		if err != nil {
			b.Error(err)
			return
		}
	}
}

package thread

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/monitor/metrics"
)

// @author: xftan
// @date: 2021/12/14 15:16
// @description: test NewLocker
func TestNewLocker(t *testing.T) {
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
				count: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(_ *testing.T) {
			locker := NewLocker(tt.args.count)
			locker.Lock()
			a := 1
			_ = a
			locker.Unlock()
		})
	}
}

func TestSetGauge(t *testing.T) {
	locker := NewLocker(1)
	g := metrics.NewGauge("test")
	locker.SetGauge(g)
	locker.Lock()
	assert.Equal(t, float64(1), g.Value())
	locker.Unlock()
	assert.Equal(t, float64(0), g.Value())
}

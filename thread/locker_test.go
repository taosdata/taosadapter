package thread

import (
	"testing"
)

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
		t.Run(tt.name, func(t *testing.T) {
			locker := NewLocker(tt.args.count)
			locker.Lock()
			locker.Unlock()
		})
	}
}

func TestDefaultLocker(t *testing.T) {
	Lock()
	t.Log("success")
	defer Unlock()
}

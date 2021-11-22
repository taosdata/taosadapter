package web

import (
	"testing"

	"github.com/gin-gonic/gin"
)

func TestGetRequestID(t *testing.T) {
	c := &gin.Context{}
	c.Set("currentID", uint32(1024))
	type args struct {
		c *gin.Context
	}
	tests := []struct {
		name string
		args args
		want uint32
	}{
		{
			name: "test",
			args: args{
				c: c,
			},
			want: 1024,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetRequestID(tt.args.c); got != tt.want {
				t.Errorf("GetRequestID() = %v, want %v", got, tt.want)
			}
		})
	}
}

package web

import (
	"testing"

	"github.com/gin-gonic/gin"
)

// @author: xftan
// @date: 2021/12/14 15:17
// @description: test gin middleware get request ID
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

func TestSetTaosErrorCode(t *testing.T) {
	type args struct {
		c    *gin.Context
		code int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test",
			args: args{
				c:    &gin.Context{},
				code: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SetTaosErrorCode(tt.args.c, tt.args.code)
		})
	}
}

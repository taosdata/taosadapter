package plugin

import (
	"testing"

	"github.com/gin-gonic/gin"
)

type fakePlugin struct {
}

func (f *fakePlugin) Init(r gin.IRouter) error {
	return nil
}

func (f *fakePlugin) Start() error {
	return nil
}

func (f *fakePlugin) Stop() error {
	return nil
}

func (f *fakePlugin) String() string {
	return "fake"
}

func (f *fakePlugin) Version() string {
	return "v1"
}

func TestRegister(t *testing.T) {
	Register(&fakePlugin{})
	r := gin.Default()
	Init(r)
	Start()
	Stop()
}

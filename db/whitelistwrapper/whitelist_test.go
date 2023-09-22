package whitelistwrapper

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
)

func TestGetWhiteList(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer wrapper.TaosClose(conn)
	c := make(chan *WhitelistResult, 1)
	handler := cgo.NewHandle(c)
	TaosFetchWhitelistA(conn, handler)
	data := <-c
	assert.Equal(t, int32(0), data.ErrCode)
	assert.Equal(t, 1, len(data.IPNets))
	assert.Equal(t, "0.0.0.0/0", data.IPNets[0].String())
}

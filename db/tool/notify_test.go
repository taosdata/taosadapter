package tool

import (
	"context"
	"net"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	tErrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
)

func TestWhiteListHandle(t *testing.T) {
	c, h := getWhiteListHandle()

	assert.NotNil(t, c)
	assert.NotNil(t, h)
	select {
	case <-c:
		t.Errorf("Expected channel to be empty, but it's not")
	default:
		// Channel is empty, which is expected
	}
	putWhiteListHandle(h)

	c2, h2 := getWhiteListHandle()
	assert.NotNil(t, c2)
	assert.NotNil(t, h2)
	assert.Equal(t, c, c2)
	assert.Equal(t, h, h2)

	putWhiteListHandle(h2)

	c2 <- nil

	c3, h3 := getWhiteListHandle()
	assert.NotNil(t, c3)
	assert.NotNil(t, h3)
	assert.Equal(t, c, c3)
	assert.Equal(t, h, h3)

	select {
	case <-c3:
		t.Errorf("Expected channel to be empty, but it's not")
	default:
		// Channel is empty, which is expected
	}
	l := make([]cgo.Handle, 10001)
	for i := 0; i < 10001; i++ {
		_, l[i] = getWhiteListHandle()
	}
	assert.Equal(t, 0, len(whiteListHandleChan))
	for i := 0; i < 10001; i++ {
		putWhiteListHandle(l[i])
	}
	assert.Equal(t, 10000, len(whiteListHandleChan))
	for i := 0; i < 10000; i++ {
		_, h = getWhiteListHandle()
		assert.Equal(t, l[i], h)
	}
	assert.Equal(t, 0, len(whiteListHandleChan))
}

func TestRegisterChangeWhiteListHandle(t *testing.T) {
	c, h := GetRegisterChangeWhiteListHandle()

	assert.NotNil(t, c)
	assert.NotNil(t, h)
	select {
	case <-c:
		t.Errorf("Expected channel to be empty, but it's not")
	default:
		// Channel is empty, which is expected
	}
	PutRegisterChangeWhiteListHandle(h)

	c2, h2 := GetRegisterChangeWhiteListHandle()
	assert.NotNil(t, c2)
	assert.NotNil(t, h2)
	assert.Equal(t, c, c2)
	assert.Equal(t, h, h2)

	PutRegisterChangeWhiteListHandle(h2)

	c2 <- 1

	c3, h3 := GetRegisterChangeWhiteListHandle()
	assert.NotNil(t, c3)
	assert.NotNil(t, h3)
	assert.Equal(t, c, c3)
	assert.Equal(t, h, h3)

	select {
	case <-c3:
		t.Errorf("Expected channel to be empty, but it's not")
	default:
		// Channel is empty, which is expected
	}
	l := make([]cgo.Handle, 10001)
	for i := 0; i < 10001; i++ {
		_, l[i] = GetRegisterChangeWhiteListHandle()
	}
	assert.Equal(t, 0, len(registerChangeWhiteListHandleChan))
	for i := 0; i < 10001; i++ {
		PutRegisterChangeWhiteListHandle(l[i])
	}
	assert.Equal(t, 10000, len(registerChangeWhiteListHandleChan))
	for i := 0; i < 10000; i++ {
		_, h = GetRegisterChangeWhiteListHandle()
		assert.Equal(t, l[i], h)
	}
	assert.Equal(t, 0, len(registerChangeWhiteListHandleChan))
}

func TestRegisterDropUserHandle(t *testing.T) {
	c, h := GetRegisterDropUserHandle()

	assert.NotNil(t, c)
	assert.NotNil(t, h)
	select {
	case <-c:
		t.Errorf("Expected channel to be empty, but it's not")
	default:
		// Channel is empty, which is expected
	}
	PutRegisterDropUserHandle(h)

	c2, h2 := GetRegisterDropUserHandle()
	assert.NotNil(t, c2)
	assert.NotNil(t, h2)
	assert.Equal(t, c, c2)
	assert.Equal(t, h, h2)

	PutRegisterDropUserHandle(h2)

	c2 <- struct{}{}

	c3, h3 := GetRegisterDropUserHandle()
	assert.NotNil(t, c3)
	assert.NotNil(t, h3)
	assert.Equal(t, c, c3)
	assert.Equal(t, h, h3)

	select {
	case <-c3:
		t.Errorf("Expected channel to be empty, but it's not")
	default:
		// Channel is empty, which is expected
	}
	l := make([]cgo.Handle, 10001)
	for i := 0; i < 10001; i++ {
		_, l[i] = GetRegisterDropUserHandle()
	}
	assert.Equal(t, 0, len(registerDropUserHandleChan))
	for i := 0; i < 10001; i++ {
		PutRegisterDropUserHandle(l[i])
	}
	assert.Equal(t, 10000, len(registerDropUserHandleChan))
	for i := 0; i < 10000; i++ {
		_, h = GetRegisterDropUserHandle()
		assert.Equal(t, l[i], h)
	}
	assert.Equal(t, 0, len(registerDropUserHandleChan))
}
func TestRegisterChangePassHandle(t *testing.T) {
	c, h := GetRegisterChangePassHandle()

	assert.NotNil(t, c)
	assert.NotNil(t, h)
	select {
	case <-c:
		t.Errorf("Expected channel to be empty, but it's not")
	default:
		// Channel is empty, which is expected
	}
	PutRegisterChangePassHandle(h)

	c2, h2 := GetRegisterChangePassHandle()
	assert.NotNil(t, c2)
	assert.NotNil(t, h2)
	assert.Equal(t, c, c2)
	assert.Equal(t, h, h2)

	PutRegisterChangePassHandle(h2)

	c2 <- 1

	c3, h3 := GetRegisterChangePassHandle()
	assert.NotNil(t, c3)
	assert.NotNil(t, h3)
	assert.Equal(t, c, c3)
	assert.Equal(t, h, h3)

	select {
	case <-c3:
		t.Errorf("Expected channel to be empty, but it's not")
	default:
		// Channel is empty, which is expected
	}
	l := make([]cgo.Handle, 10001)
	for i := 0; i < 10001; i++ {
		_, l[i] = GetRegisterChangePassHandle()
	}
	assert.Equal(t, 0, len(registerChangePassHandleChan))
	for i := 0; i < 10001; i++ {
		PutRegisterChangePassHandle(l[i])
	}
	assert.Equal(t, 10000, len(registerChangePassHandleChan))
	for i := 0; i < 10000; i++ {
		_, h = GetRegisterChangePassHandle()
		assert.Equal(t, l[i], h)
	}
	assert.Equal(t, 0, len(registerChangePassHandleChan))
}

func TestGetWhitelist(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	defer wrapper.TaosClose(conn)
	assert.NoError(t, err)
	ipNets, err := GetWhitelist(conn)
	assert.NoError(t, err)
	assert.NotNil(t, ipNets)
	t.Log(ipNets)
	_, ipNet, _ := net.ParseCIDR("0.0.0.0/0")
	assert.Equal(t, []*net.IPNet{ipNet}, ipNets)
	ipNets, err = GetWhitelist(nil)
	assert.Error(t, err)
	assert.Nil(t, ipNets)
}

func TestCheckWhitelist(t *testing.T) {
	_, ipNet, _ := net.ParseCIDR("127.0.0.1/32")
	ipNets := []*net.IPNet{ipNet}
	contains := CheckWhitelist(ipNets, net.ParseIP("127.0.0.1"))
	assert.True(t, contains)
	contains = CheckWhitelist(ipNets, net.ParseIP("192.168.1.1"))
	assert.False(t, contains)
}

func TestRegisterChangeWhitelist(t *testing.T) {
	c, h := GetRegisterChangeWhiteListHandle()
	defer PutRegisterChangeWhiteListHandle(h)
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer wrapper.TaosClose(conn)
	done := make(chan struct{})
	go func() {
		select {
		case data := <-c:
			t.Log(data)
		case <-done:
		}
	}()
	err = RegisterChangeWhitelist(conn, h)
	assert.NoError(t, err)
	time.Sleep(time.Second * 5)
	close(done)
}

func TestRegisterChangePass(t *testing.T) {
	c, h := GetRegisterChangePassHandle()
	defer PutRegisterChangePassHandle(h)
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer wrapper.TaosClose(conn)
	err = exec(conn, "create user test_notify pass 'notify'")
	assert.NoError(t, err)
	defer func() {
		// ignore error
		_ = exec(conn, "drop user test_notify")
	}()
	conn2, err := wrapper.TaosConnect("", "test_notify", "notify", "", 0)
	assert.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	err = RegisterChangePass(conn2, h)
	assert.NoError(t, err)
	select {
	case data := <-c:
		t.Error("unexpected notify callback", data)
	case <-ctx.Done():
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel2()
	err = exec(conn, "alter user test_notify pass 'test'")
	assert.NoError(t, err)
	select {
	case data := <-c:
		t.Log(data)
	case <-ctx2.Done():
		t.Error("wait for notify callback timeout")
	}
}

func TestRegisterDropUser(t *testing.T) {
	c, h := GetRegisterDropUserHandle()
	defer PutRegisterDropUserHandle(h)
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer wrapper.TaosClose(conn)
	err = exec(conn, "create user test_drop_user pass 'notify'")
	assert.NoError(t, err)
	defer func() {
		// ignore error
		_ = exec(conn, "drop user test_drop_user")
	}()
	conn2, err := wrapper.TaosConnect("", "test_drop_user", "notify", "", 0)
	assert.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	err = RegisterDropUser(conn2, h)
	assert.NoError(t, err)
	select {
	case data := <-c:
		t.Error("unexpected notify callback", data)
	case <-ctx.Done():
	}
	err = exec(conn, "drop user test_drop_user")
	assert.NoError(t, err)
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel2()
	select {
	case data := <-c:
		t.Log(data)
	case <-ctx2.Done():
		t.Error("wait for notify callback timeout")
	}
}

func exec(conn unsafe.Pointer, sql string) error {
	result := wrapper.TaosQuery(conn, sql)
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		return tErrors.NewError(code, errStr)
	}
	wrapper.TaosFreeResult(result)
	return nil
}

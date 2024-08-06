package tool

import (
	"net"
	"unsafe"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/db/whitelistwrapper"
	"github.com/taosdata/taosadapter/v3/thread"
)

var whiteListHandleChan = make(chan cgo.Handle, 10000)

func getWhiteListHandle() (chan *whitelistwrapper.WhitelistResult, cgo.Handle) {
	select {
	case handle := <-whiteListHandleChan:
		c := handle.Value().(chan *whitelistwrapper.WhitelistResult)
		// cleanup channel
		for {
			select {
			case <-c:
			default:
				return c, handle
			}
		}
	default:
		c := make(chan *whitelistwrapper.WhitelistResult, 1)
		return c, cgo.NewHandle(c)
	}
}

func putWhiteListHandle(handle cgo.Handle) {
	select {
	case whiteListHandleChan <- handle:
	default:
		handle.Delete()
	}
}

func GetWhitelist(conn unsafe.Pointer) ([]*net.IPNet, error) {
	c, handler := getWhiteListHandle()
	defer putWhiteListHandle(handler)
	thread.Lock()
	whitelistwrapper.TaosFetchWhitelistA(conn, handler)
	thread.Unlock()
	data := <-c
	if data.ErrCode != 0 {
		err := errors.NewError(int(data.ErrCode), wrapper.TaosErrorStr(nil))
		return nil, err
	}
	return data.IPNets, nil
}

func CheckWhitelist(whitelist []*net.IPNet, ip net.IP) bool {
	for _, ipNet := range whitelist {
		if ipNet.Contains(ip) {
			return true
		}
	}
	return false
}

// whitelist change
var registerChangeWhiteListHandleChan = make(chan cgo.Handle, 10000)

func GetRegisterChangeWhiteListHandle() (chan int64, cgo.Handle) {
	select {
	case handle := <-registerChangeWhiteListHandleChan:
		c := handle.Value().(chan int64)
		// cleanup channel
		for {
			select {
			case <-c:
			default:
				return c, handle
			}
		}
	default:
		c := make(chan int64, 1)
		return c, cgo.NewHandle(c)
	}
}

func PutRegisterChangeWhiteListHandle(handle cgo.Handle) {
	select {
	case registerChangeWhiteListHandleChan <- handle:
	default:
		handle.Delete()
	}
}

func RegisterChangeWhitelist(conn unsafe.Pointer, handle cgo.Handle) error {
	errCode := wrapper.TaosSetNotifyCB(conn, handle, common.TAOS_NOTIFY_WHITELIST_VER)
	if errCode != 0 {
		return errors.NewError(int(errCode), wrapper.TaosErrorStr(nil))
	}
	return nil
}

// drop user
var registerDropUserHandleChan = make(chan cgo.Handle, 10000)

func GetRegisterDropUserHandle() (chan struct{}, cgo.Handle) {
	select {
	case handle := <-registerDropUserHandleChan:
		c := handle.Value().(chan struct{})
		// cleanup channel
		for {
			select {
			case <-c:
			default:
				return c, handle
			}
		}
	default:
		c := make(chan struct{}, 1)
		return c, cgo.NewHandle(c)
	}
}

func PutRegisterDropUserHandle(handle cgo.Handle) {
	select {
	case registerDropUserHandleChan <- handle:
	default:
		handle.Delete()
	}
}

func RegisterDropUser(conn unsafe.Pointer, handle cgo.Handle) error {
	errCode := wrapper.TaosSetNotifyCB(conn, handle, common.TAOS_NOTIFY_USER_DROPPED)
	if errCode != 0 {
		return errors.NewError(int(errCode), wrapper.TaosErrorStr(nil))
	}
	return nil
}

// change password
var registerChangePassHandleChan = make(chan cgo.Handle, 10000)

func GetRegisterChangePassHandle() (chan int32, cgo.Handle) {
	select {
	case handle := <-registerChangePassHandleChan:
		// cleanup channel
		c := handle.Value().(chan int32)
		for {
			select {
			case <-c:
			default:
				return c, handle
			}
		}
	default:
		c := make(chan int32, 1)
		return c, cgo.NewHandle(c)
	}
}

func PutRegisterChangePassHandle(handle cgo.Handle) {
	select {
	case registerChangePassHandleChan <- handle:
	default:
		handle.Delete()
	}
}

func RegisterChangePass(conn unsafe.Pointer, handle cgo.Handle) error {
	errCode := wrapper.TaosSetNotifyCB(conn, handle, common.TAOS_NOTIFY_PASSVER)
	if errCode != 0 {
		return errors.NewError(int(errCode), wrapper.TaosErrorStr(nil))
	}
	return nil
}

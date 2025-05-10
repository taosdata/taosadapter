package tool

import (
	"net"
	"strings"
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/thread"
)

const PoolSize = 10000

var whiteListHandleChan = make(chan cgo.Handle, PoolSize)

func getWhiteListHandle() (chan *wrapper.WhitelistResult, cgo.Handle) {
	select {
	case handle := <-whiteListHandleChan:
		c := handle.Value().(chan *wrapper.WhitelistResult)
		// cleanup channel
		for {
			select {
			case <-c:
			default:
				return c, handle
			}
		}
	default:
		c := make(chan *wrapper.WhitelistResult, 1)
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
	taosFetchWhiteListA(conn, handler)
	data := <-c
	if data.ErrCode != 0 {
		err := errors.NewError(int(data.ErrCode), wrapper.TaosErrorStr(nil))
		return nil, err
	}
	return data.IPNets, nil
}

func taosFetchWhiteListA(conn unsafe.Pointer, handle cgo.Handle) {
	thread.AsyncSemaphore.Acquire()
	defer func() {
		thread.AsyncSemaphore.Release()
	}()
	wrapper.TaosFetchWhitelistA(conn, handle)
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
var registerChangeWhiteListHandleChan = make(chan cgo.Handle, PoolSize)

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
var registerDropUserHandleChan = make(chan cgo.Handle, PoolSize)

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
var registerChangePassHandleChan = make(chan cgo.Handle, PoolSize)

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

func IpNetSliceToString(ipNets []*net.IPNet) string {
	builder := strings.Builder{}
	for i, ipNet := range ipNets {
		builder.WriteString(ipNet.String())
		if i != len(ipNets)-1 {
			builder.WriteString(",")
		}
	}
	return builder.String()
}

package tool

import (
	"net"
	"unsafe"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/thread"
)

func GetWhitelist(conn unsafe.Pointer) ([]*net.IPNet, error) {
	c := make(chan *wrapper.WhitelistResult, 1)
	handler := cgo.NewHandle(c)
	thread.Lock()
	wrapper.TaosFetchWhitelistA(conn, handler)
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

func RegisterChangeWhitelist(conn unsafe.Pointer, versionChan chan int64) error {
	errCode := wrapper.TaosSetNotifyCB(conn, cgo.NewHandle(versionChan), common.TAOS_NOTIFY_WHITELIST_VER)
	if errCode != 0 {
		return errors.NewError(int(errCode), wrapper.TaosErrorStr(nil))
	}
	return nil
}

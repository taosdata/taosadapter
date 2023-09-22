package commonpool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v3/common"
	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/thread"
	"github.com/taosdata/taosadapter/v3/tools/connectpool"
	"golang.org/x/sync/singleflight"
)

type ConnectorPool struct {
	notifyChan    chan int32
	whitelistChan chan int64
	user          string
	password      string
	pool          *connectpool.ConnectPool
	logger        *logrus.Entry
	once          sync.Once
	ctx           context.Context
	cancel        context.CancelFunc
	ipNetsLock    sync.RWMutex
	ipNets        []*net.IPNet
}

func NewConnectorPool(user, password string) (*ConnectorPool, error) {
	cp := &ConnectorPool{
		user:          user,
		password:      password,
		notifyChan:    make(chan int32, 1),
		whitelistChan: make(chan int64, 1),
		logger:        log.GetLogger("connect_pool").WithField("user", user),
	}
	maxConnect := config.Conf.Pool.MaxConnect
	if maxConnect == 0 {
		maxConnect = runtime.GOMAXPROCS(0) * 2
	}
	poolConfig := &connectpool.Config{
		InitialCap: 1,
		MaxCap:     maxConnect,
		Factory:    cp.factory,
		Close:      cp.close,
	}
	p, err := connectpool.NewConnectPool(poolConfig)

	if err != nil {
		return nil, err
	}
	cp.pool = p
	v, _ := p.Get()
	// notify
	cp.ctx, cp.cancel = context.WithCancel(context.Background())
	errCode := wrapper.TaosSetNotifyCB(v, cgo.NewHandle(cp.notifyChan), common.TAOS_NOTIFY_PASSVER)
	if errCode != 0 {
		errStr := wrapper.TaosErrorStr(nil)
		p.Put(v)
		p.Release()
		return nil, tErrors.NewError(int(errCode), errStr)
	}
	// whitelist
	c := make(chan *wrapper.WhitelistResult, 1)
	handler := cgo.NewHandle(c)
	// fetch whitelist
	thread.Lock()
	wrapper.TaosFetchWhitelistA(v, handler)
	thread.Unlock()
	data := <-c
	if data.ErrCode != 0 {
		errStr := wrapper.TaosErrorStr(nil)
		p.Put(v)
		p.Release()
		return nil, tErrors.NewError(int(data.ErrCode), errStr)
	}
	cp.ipNets = data.IPNets
	// register whitelist modify callback
	errCode = wrapper.TaosSetNotifyCB(v, cgo.NewHandle(cp.whitelistChan), common.TAOS_NOTIFY_WHITELIST_VER)
	if errCode != 0 {
		errStr := wrapper.TaosErrorStr(nil)
		p.Put(v)
		p.Release()
		return nil, tErrors.NewError(int(errCode), errStr)
	}
	p.Put(v)
	go func() {
		select {
		case <-cp.notifyChan:
			// password changed
			connectionLocker.Lock()
			defer connectionLocker.Unlock()
			cp.Release()
			return
		case <-cp.whitelistChan:
			// whitelist changed
			cp.logger.Info("whitelist change")
			c := make(chan *wrapper.WhitelistResult, 1)
			handler := cgo.NewHandle(c)
			// fetch whitelist
			thread.Lock()
			wrapper.TaosFetchWhitelistA(v, handler)
			thread.Unlock()
			data := <-c
			if data.ErrCode != 0 {
				// fetch whitelist error
				cp.logger.WithError(tErrors.NewError(int(data.ErrCode), wrapper.TaosErrorStr(nil))).Error("fetch whitelist error! release connection!")
				connectionLocker.Lock()
				defer connectionLocker.Unlock()
				// release connection pool
				cp.Release()
				return
			}
			cp.ipNetsLock.Lock()
			cp.ipNets = data.IPNets
			tmp := make([]string, len(cp.ipNets))
			for _, ipNet := range cp.ipNets {
				tmp = append(tmp, ipNet.String())
			}
			cp.logger.WithField("whitelist", strings.Join(tmp, ",")).Debugln("whitelist change")
			cp.ipNetsLock.Unlock()
		case <-cp.ctx.Done():
			return
		}
	}()

	return cp, nil
}

func (cp *ConnectorPool) factory() (unsafe.Pointer, error) {
	thread.Lock()
	defer thread.Unlock()
	return wrapper.TaosConnect("", cp.user, cp.password, "", 0)
}

func (cp *ConnectorPool) close(v unsafe.Pointer) error {
	if v != nil {
		thread.Lock()
		defer thread.Unlock()
		wrapper.TaosClose(v)
	}
	return nil
}

var AuthFailureError = tErrors.NewError(httperror.TSDB_CODE_MND_AUTH_FAILURE, "Authentication failure")

func (cp *ConnectorPool) Get() (unsafe.Pointer, error) {
	v, err := cp.pool.Get()
	if err != nil {
		if err == connectpool.ErrClosed {
			cp.logger.Warn("connect poll closed return Authentication failure")
			return nil, AuthFailureError
		}
		return nil, err
	}
	return v, nil
}

func (cp *ConnectorPool) Put(c unsafe.Pointer) error {
	wrapper.TaosResetCurrentDB(c)
	return cp.pool.Put(c)
}

func (cp *ConnectorPool) Close(c unsafe.Pointer) error {
	return cp.pool.Close(c)
}

func (cp *ConnectorPool) verifyPassword(password string) bool {
	return password == cp.password
}

func (cp *ConnectorPool) verifyIP(ip net.IP) bool {
	cp.ipNetsLock.RLock()
	defer cp.ipNetsLock.RUnlock()
	for _, ipNet := range cp.ipNets {
		if ipNet.Contains(ip) {
			return true
		}
	}
	return false
}

func (cp *ConnectorPool) Release() {
	cp.once.Do(func() {
		cp.cancel()
		v, exist := connectionMap.Load(cp.user)
		if exist && v == cp {
			connectionMap.Delete(cp.user)
		}
		close(cp.notifyChan)
		close(cp.whitelistChan)
		cp.pool.Release()
		cp.logger.Warnln("connector released")
	})
}

var connectionMap = sync.Map{}
var connectionLocker sync.Mutex

type Conn struct {
	TaosConnection unsafe.Pointer
	pool           *ConnectorPool
}

func (c *Conn) Put() error {
	return c.pool.Put(c.TaosConnection)
}

var singleGroup singleflight.Group
var ErrWhitelistForbidden error = errors.New("whitelist prohibits current IP access")

func GetConnection(user, password string, clientIp net.IP) (*Conn, error) {
	cp, err := getConnectionPool(user, password)
	if err != nil {
		return nil, err
	}
	return getConnectDirect(cp, clientIp)
}

func getConnectionPool(user, password string) (*ConnectorPool, error) {
	p, exist := connectionMap.Load(user)
	if exist {
		connectionPool := p.(*ConnectorPool)
		if connectionPool.verifyPassword(password) {
			return connectionPool, nil
		} else {
			cp, err, _ := singleGroup.Do(fmt.Sprintf("%s:%s", user, password), func() (interface{}, error) {
				return getConnectorPoolSafe(user, password)
			})
			if err != nil {
				return nil, err
			}
			return cp.(*ConnectorPool), nil
		}
	} else {
		cp, err, _ := singleGroup.Do(fmt.Sprintf("%s:%s", user, password), func() (interface{}, error) {
			return getConnectorPoolSafe(user, password)
		})
		if err != nil {
			return nil, err
		}
		return cp.(*ConnectorPool), nil
	}
}

func VerifyClientIP(user, password string, clientIP net.IP) (authed bool, valid bool, connectionPoolExits bool) {
	p, exist := connectionMap.Load(user)
	if !exist {
		return
	}
	connectionPoolExits = true
	if !p.(*ConnectorPool).verifyPassword(password) {
		return
	}
	authed = true
	if !p.(*ConnectorPool).verifyIP(clientIP) {
		return
	}
	valid = true
	return
}

func getConnectDirect(connectionPool *ConnectorPool, clientIP net.IP) (*Conn, error) {
	if !connectionPool.verifyIP(clientIP) {
		return nil, ErrWhitelistForbidden
	}
	c, err := connectionPool.Get()
	if err != nil {
		return nil, err
	}
	return &Conn{
		TaosConnection: c,
		pool:           connectionPool,
	}, nil
}

func getConnectorPoolSafe(user, password string) (*ConnectorPool, error) {
	connectionLocker.Lock()
	defer connectionLocker.Unlock()
	p, exist := connectionMap.Load(user)
	if exist {
		connectionPool := p.(*ConnectorPool)
		if connectionPool.verifyPassword(password) {
			return connectionPool, nil
		} else {
			newPool, err := NewConnectorPool(user, password)
			if err != nil {
				return nil, err
			}
			connectionPool.Release()
			connectionMap.Store(user, newPool)
			return newPool, nil
		}
	} else {
		newPool, err := NewConnectorPool(user, password)
		if err != nil {
			return nil, err
		}
		connectionMap.Store(user, newPool)
		return newPool, nil
	}
}

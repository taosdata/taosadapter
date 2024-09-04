package commonpool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"unsafe"

	"github.com/sirupsen/logrus"
	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/connectpool"
	"golang.org/x/sync/singleflight"
)

type ConnectorPool struct {
	changePassChan        chan int32
	whitelistChan         chan int64
	dropUserChan          chan struct{}
	user                  string
	password              string
	pool                  *connectpool.ConnectPool
	logger                *logrus.Entry
	once                  sync.Once
	ctx                   context.Context
	cancel                context.CancelFunc
	ipNetsLock            sync.RWMutex
	ipNets                []*net.IPNet
	changePassHandle      cgo.Handle
	whitelistChangeHandle cgo.Handle
	dropUserHandle        cgo.Handle
}

func NewConnectorPool(user, password string) (*ConnectorPool, error) {
	changePassChan, changePassHandle := tool.GetRegisterChangePassHandle()
	whitelistChangeChan, whitelistChangeHandle := tool.GetRegisterChangeWhiteListHandle()
	dropUserChan, dropUserHandle := tool.GetRegisterDropUserHandle()
	cp := &ConnectorPool{
		user:                  user,
		password:              password,
		changePassChan:        changePassChan,
		changePassHandle:      changePassHandle,
		whitelistChan:         whitelistChangeChan,
		whitelistChangeHandle: whitelistChangeHandle,
		dropUserChan:          dropUserChan,
		dropUserHandle:        dropUserHandle,
		logger:                log.GetLogger("CNP").WithField("user", user),
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
	// notify modify
	cp.ctx, cp.cancel = context.WithCancel(context.Background())
	err = tool.RegisterChangePass(v, cp.changePassHandle)
	if err != nil {
		p.Put(v)
		p.Release()
		cp.putHandle()
		return nil, err
	}
	// notify drop
	err = tool.RegisterDropUser(v, cp.dropUserHandle)
	if err != nil {
		p.Put(v)
		p.Release()
		cp.putHandle()
		return nil, err
	}
	// whitelist
	ipNets, err := tool.GetWhitelist(v)
	if err != nil {
		p.Put(v)
		p.Release()
		cp.putHandle()
		return nil, err
	}
	cp.ipNets = ipNets
	// register whitelist modify callback
	err = tool.RegisterChangeWhitelist(v, cp.whitelistChangeHandle)
	if err != nil {
		p.Put(v)
		p.Release()
		cp.putHandle()
		return nil, err
	}
	p.Put(v)
	go func() {
		defer func() {
			cp.logger.Warn("connector pool exit")
			cp.putHandle()
		}()
		for {
			select {
			case <-cp.changePassChan:
				// password changed
				cp.logger.Info("password changed")
				connectionLocker.Lock()
				cp.Release()
				connectionLocker.Unlock()
				return
			case <-cp.dropUserChan:
				// user dropped
				cp.logger.Info("user dropped")
				connectionLocker.Lock()
				cp.Release()
				connectionLocker.Unlock()
				return
			case <-cp.whitelistChan:
				// whitelist changed
				cp.logger.Info("whitelist change")
				ipNets, err = tool.GetWhitelist(v)
				if err != nil {
					// fetch whitelist error
					cp.logger.WithError(err).Error("fetch whitelist error! release connection!")
					connectionLocker.Lock()
					// release connection pool
					cp.Release()
					connectionLocker.Unlock()
					return
				}
				cp.ipNetsLock.Lock()
				cp.ipNets = ipNets

				cp.logger.Debugf("whitelist change, whitelist:%s", tool.IpNetSliceToString(cp.ipNets))
				cp.ipNetsLock.Unlock()
			case <-cp.ctx.Done():
				return
			}
		}
	}()

	return cp, nil
}

func (cp *ConnectorPool) factory() (unsafe.Pointer, error) {
	conn, err := syncinterface.TaosConnect("", cp.user, cp.password, "", 0, cp.logger, log.IsDebug())
	if err != nil {
		cp.logger.Errorf("connect to taos failed: %s", err.Error())
	}
	return conn, err
}

func (cp *ConnectorPool) close(v unsafe.Pointer) error {
	if v != nil {
		syncinterface.TaosClose(v, cp.logger, log.IsDebug())
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
		cp.pool.Release()
		cp.logger.Warn("connector released")
	})
}

func (cp *ConnectorPool) putHandle() {
	tool.PutRegisterChangePassHandle(cp.changePassHandle)
	tool.PutRegisterChangeWhiteListHandle(cp.whitelistChangeHandle)
	tool.PutRegisterDropUserHandle(cp.dropUserHandle)
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

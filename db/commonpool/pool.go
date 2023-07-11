package commonpool

import (
	"fmt"
	"runtime"
	"sync"
	"unsafe"

	"github.com/sirupsen/logrus"
	tErrors "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/log"
	"github.com/taosdata/taosadapter/thread"
	"github.com/taosdata/taosadapter/tools/connectpool"
	"golang.org/x/sync/singleflight"
)

type ConnectorPool struct {
	user     string
	password string
	pool     *connectpool.ConnectPool
	logger   *logrus.Entry
	once     sync.Once
}

func NewConnectorPool(user, password string) (*ConnectorPool, error) {
	cp := &ConnectorPool{
		user:     user,
		password: password,
		logger:   log.GetLogger("connect_pool").WithField("user", user),
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

const TSDB_CODE_MND_AUTH_FAILURE = 0x0357

var AuthFailureError = tErrors.NewError(TSDB_CODE_MND_AUTH_FAILURE, "Authentication failure")

func (cp *ConnectorPool) Get() (unsafe.Pointer, error) {
	v, err := cp.pool.Get()
	if err != nil {
		if err == connectpool.ErrClosed {
			cp.logger.Warn("connect poll closed return Authentication failure")
			return nil, AuthFailureError
		}
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

func (cp *ConnectorPool) Release() {
	cp.once.Do(func() {
		v, exist := connectionMap.Load(cp.user)
		if exist && v == cp {
			connectionMap.Delete(cp.user)
		}
		cp.pool.Release()
		cp.logger.Warnln("connector released")
	})
}

func (cp *ConnectorPool) verifyPassword(password string) bool {
	return password == cp.password
}

var connectionMap = sync.Map{}

type Conn struct {
	TaosConnection unsafe.Pointer
	pool           *ConnectorPool
}

func (c *Conn) Put() error {
	return c.pool.Put(c.TaosConnection)
}

var singleGroup singleflight.Group

func GetConnection(user, password string) (*Conn, error) {
	p, exist := connectionMap.Load(user)
	if exist {
		connectionPool := p.(*ConnectorPool)
		if connectionPool.verifyPassword(password) {
			return getConnectDirect(connectionPool)
		} else {
			cp, err, _ := singleGroup.Do(fmt.Sprintf("%s:%s", user, password), func() (interface{}, error) {
				return getConnectorPoolSafe(user, password)
			})
			if err != nil {
				return nil, err
			}
			return getConnectDirect(cp.(*ConnectorPool))
		}
	} else {
		cp, err, _ := singleGroup.Do(fmt.Sprintf("%s:%s", user, password), func() (interface{}, error) {
			return getConnectorPoolSafe(user, password)
		})
		if err != nil {
			return nil, err
		}
		return getConnectDirect(cp.(*ConnectorPool))
	}
}

func getConnectDirect(connectionPool *ConnectorPool) (*Conn, error) {
	c, err := connectionPool.Get()
	if err != nil {
		return nil, err
	}
	return &Conn{
		TaosConnection: c,
		pool:           connectionPool,
	}, nil
}

var connectionLocker sync.Mutex

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

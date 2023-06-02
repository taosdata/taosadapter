package commonpool

import (
	"context"
	"runtime"
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
)

type ConnectorPool struct {
	notifyChan chan int32
	user       string
	password   string
	pool       *connectpool.ConnectPool
	logger     *logrus.Entry
	once       sync.Once
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewConnectorPool(user, password string) (*ConnectorPool, error) {
	cp := &ConnectorPool{
		user:       user,
		password:   password,
		notifyChan: make(chan int32, 1),
		logger:     log.GetLogger("connect_pool").WithField("user", user),
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
	cp.ctx, cp.cancel = context.WithCancel(context.Background())
	errCode := wrapper.TaosSetNotifyCB(v, cgo.NewHandle(cp.notifyChan), common.TAOS_NOTIFY_PASSVER)
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
			connectionLocker.Lock()
			defer connectionLocker.Unlock()
			cp.Release()
			return
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

func (cp *ConnectorPool) Release() {
	cp.once.Do(func() {
		cp.cancel()
		v, exist := connectionMap.Load(cp.user)
		if exist && v == cp {
			connectionMap.Delete(cp.user)
		}
		close(cp.notifyChan)
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

func GetConnection(user, password string) (*Conn, error) {
	p, exist := connectionMap.Load(user)
	if exist {
		connectionPool := p.(*ConnectorPool)
		if connectionPool.verifyPassword(password) {
			return getConnectDirect(connectionPool)
		} else {
			return getConnectionSafe(user, password)
		}
	} else {
		return getConnectionSafe(user, password)
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

func getConnectionSafe(user, password string) (*Conn, error) {
	connectionLocker.Lock()
	defer connectionLocker.Unlock()
	p, exist := connectionMap.Load(user)
	if exist {
		connectionPool := p.(*ConnectorPool)
		if connectionPool.verifyPassword(password) {
			return getConnectDirect(connectionPool)
		} else {
			newPool, err := NewConnectorPool(user, password)
			if err != nil {
				return nil, err
			}
			connectionPool.Release()
			connectionMap.Store(user, newPool)
			return getConnectDirect(newPool)
		}
	} else {
		newPool, err := NewConnectorPool(user, password)
		if err != nil {
			return nil, err
		}
		connectionMap.Store(user, newPool)
		return getConnectDirect(newPool)
	}
}

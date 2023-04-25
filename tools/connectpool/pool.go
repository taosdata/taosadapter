package connectpool

import (
	"errors"
	"sync"
	"unsafe"
)

var (
	ErrMaxActiveConnReached = errors.New("MaxActiveConnReached")
	ErrClosed               = errors.New("pool is closed")
)

type Config struct {
	InitialCap int
	MaxCap     int
	Factory    func() (unsafe.Pointer, error)
	Close      func(pointer unsafe.Pointer) error
}

type connReq struct {
	idleConn unsafe.Pointer
}

type ConnectPool struct {
	mu           sync.RWMutex
	conns        chan unsafe.Pointer
	factory      func() (unsafe.Pointer, error)
	close        func(pointer unsafe.Pointer) error
	maxActive    int
	openingConns int
	connReqs     []chan connReq
	released     bool
	releasedOnce sync.Once
}

func NewConnectPool(poolConfig *Config) (*ConnectPool, error) {
	if poolConfig.MaxCap <= 0 {
		return nil, errors.New("MaxCap must larger than 0")
	}
	if poolConfig.Factory == nil {
		return nil, errors.New("invalid factory func settings")
	}
	if poolConfig.Close == nil {
		return nil, errors.New("invalid close func settings")
	}

	c := &ConnectPool{
		conns:        make(chan unsafe.Pointer, poolConfig.MaxCap),
		factory:      poolConfig.Factory,
		close:        poolConfig.Close,
		maxActive:    poolConfig.MaxCap,
		openingConns: 0,
		released:     false,
	}

	for i := 0; i < poolConfig.InitialCap; i++ {
		conn, err := c.factory()
		if err != nil {
			c.Release()
			return nil, err
		}
		c.openingConns++
		c.conns <- conn
	}

	return c, nil
}

func (c *ConnectPool) getConns() chan unsafe.Pointer {
	c.mu.Lock()
	if c.released {
		c.mu.Unlock()
		return nil
	}
	conns := c.conns
	c.mu.Unlock()
	return conns
}

func (c *ConnectPool) Get() (unsafe.Pointer, error) {
	conns := c.getConns()
	if conns == nil {
		return nil, ErrClosed
	}
	for {
		select {
		case wrapConn := <-conns:
			if wrapConn == nil { // conn closed
				return nil, ErrClosed
			}
			return wrapConn, nil
		default:
			c.mu.Lock()
			if c.openingConns >= c.maxActive {
				req := make(chan connReq, 1)
				c.connReqs = append(c.connReqs, req)
				c.mu.Unlock()
				ret, ok := <-req
				if !ok {
					return nil, ErrMaxActiveConnReached
				}
				return ret.idleConn, nil
			}
			if c.factory == nil {
				c.mu.Unlock()
				return nil, ErrClosed
			}
			conn, err := c.factory()
			if err != nil {
				c.mu.Unlock()
				return nil, err
			}
			c.openingConns++
			c.mu.Unlock()
			return conn, nil
		}
	}
}

func (c *ConnectPool) Put(conn unsafe.Pointer) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	c.mu.Lock()

	if c.conns == nil {
		c.mu.Unlock()
		return c.Close(conn)
	}

	if l := len(c.connReqs); l > 0 {
		req := c.connReqs[0]
		copy(c.connReqs, c.connReqs[1:])
		c.connReqs = c.connReqs[:l-1]
		req <- connReq{
			idleConn: conn,
		}
		c.mu.Unlock()
		return nil
	} else {
		if c.released {
			c.mu.Unlock()
			c.Close(conn)
			c.doRelease()
			if c.openingConns == 0 {
				c.close = nil
			}
			return nil
		}
		select {
		case c.conns <- conn:
			c.mu.Unlock()
			return nil
		default:
			c.mu.Unlock()
			return c.Close(conn)
		}
	}
}

func (c *ConnectPool) Close(conn unsafe.Pointer) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.close == nil {
		return nil
	}
	c.openingConns--
	return c.close(conn)
}

func (c *ConnectPool) Release() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.released {
		return
	}
	c.released = true
	if len(c.connReqs) == 0 {
		c.doRelease()
	}
}

func (c *ConnectPool) doRelease() {
	c.releasedOnce.Do(func() {
		c.factory = nil
		if c.conns == nil {
			return
		}
		close(c.conns)
		for conn := range c.conns {
			c.openingConns--
			c.close(conn)
		}
		c.conns = nil
	})
}

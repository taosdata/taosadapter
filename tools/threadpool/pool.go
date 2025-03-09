package threadpool

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type Stack struct {
	elements []unsafe.Pointer
	sync.Mutex
}

func NewStack(maxSize int) *Stack {
	return &Stack{
		elements: make([]unsafe.Pointer, 0, maxSize),
	}
}

func (s *Stack) Push(value unsafe.Pointer) {
	s.Lock()
	s.elements = append(s.elements, value)
	s.Unlock()
}

func (s *Stack) Pop() (unsafe.Pointer, bool) {
	s.Lock()
	if len(s.elements) == 0 {
		s.Unlock()
		return nil, false
	}
	index := len(s.elements) - 1
	value := s.elements[index]
	s.elements = s.elements[:index]
	s.Unlock()
	return value, true
}

func (s *Stack) Clear() []unsafe.Pointer {
	s.Lock()
	elements := s.elements
	s.elements = nil
	s.Unlock()
	return elements
}

type Config struct {
	InitialCap int
	MaxCap     int
	MaxWait    int
	Factory    func() (unsafe.Pointer, error)
	Close      func(unsafe.Pointer)
}

type ThreadPool struct {
	threadsStack  *Stack
	factory       func() (unsafe.Pointer, error)
	close         func(unsafe.Pointer)
	maxActive     int32
	createdThread atomic.Int32
	maxWait       int
	threadReqs    chan chan unsafe.Pointer
}

func NewThreadPool(poolConfig *Config) (*ThreadPool, error) {
	if poolConfig.MaxCap <= 0 {
		return nil, errors.New("MaxCap must larger than 0")
	}
	if poolConfig.Factory == nil {
		return nil, errors.New("invalid factory func settings")
	}
	c := &ThreadPool{
		threadsStack: NewStack(poolConfig.MaxCap),
		factory:      poolConfig.Factory,
		close:        poolConfig.Close,
		maxActive:    int32(poolConfig.MaxCap),
		maxWait:      poolConfig.MaxWait,
		threadReqs:   make(chan chan unsafe.Pointer, poolConfig.MaxWait),
	}

	for i := 0; i < poolConfig.InitialCap; i++ {
		thread, err := c.factory()
		if err != nil {
			for {
				th, _ := c.threadsStack.Pop()
				if th == nil {
					break
				}
				c.close(th)
			}
			return nil, err
		}
		c.createdThread.Add(1)
		c.threadsStack.Push(thread)
	}

	return c, nil
}

var (
	ErrMaxWait = errors.New("exceeded thread pool max wait")
)

func (c *ThreadPool) Get() (unsafe.Pointer, error) {
	s1 := time.Now()
	thread, ok := c.threadsStack.Pop()
	fmt.Println("get thread cost:", time.Since(s1))
	if ok {
		return thread, nil
	}

	thread, err := c.createThread()
	if err != nil {
		return nil, err
	}
	if thread != nil {
		return thread, nil
	}
	if thread, ok = c.threadsStack.Pop(); ok {
		return thread, nil
	}
	req := make(chan unsafe.Pointer, 1)
	select {
	case c.threadReqs <- req:
	default:
		return nil, ErrMaxWait
	}
	thread = <-req
	return thread, nil
}

func (c *ThreadPool) createThread() (thread unsafe.Pointer, err error) {
	for {
		old := c.createdThread.Load()
		if old >= c.maxActive {
			return nil, nil
		}
		newVal := old + 1
		if c.createdThread.CompareAndSwap(old, newVal) {
			break
		}
	}
	thread, err = c.factory()
	if err != nil {
		c.createdThread.Add(-1)
		return nil, err
	}
	return thread, nil
}

func (c *ThreadPool) Put(thread unsafe.Pointer) {
	select {
	case req := <-c.threadReqs:
		req <- thread
	default:
		c.threadsStack.Push(thread)
	}
}

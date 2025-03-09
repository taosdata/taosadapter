package thread

import "github.com/taosdata/taosadapter/v3/tools/threadpool"

type Locker struct {
	c chan struct{}
}

var SyncLocker *Locker
var AsyncLocker *Locker

func NewLocker(count int) *Locker {
	return &Locker{c: make(chan struct{}, count)}
}

func (l *Locker) Lock() {
	l.c <- struct{}{}
}

func (l *Locker) Unlock() {
	<-l.c
}

var TSCThreadPool *threadpool.ThreadPool

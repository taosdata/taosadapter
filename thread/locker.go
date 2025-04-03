package thread

import "github.com/taosdata/taosadapter/v3/monitor/metrics"

type Locker struct {
	c     chan struct{}
	gauge *metrics.Gauge
}

var SyncLocker *Locker
var AsyncLocker *Locker

func NewLocker(count int) *Locker {
	return &Locker{c: make(chan struct{}, count)}
}

func (l *Locker) SetGauge(gauge *metrics.Gauge) {
	l.gauge = gauge
}

func (l *Locker) Lock() {
	l.c <- struct{}{}
	if l.gauge != nil {
		l.gauge.Inc()
	}
}

func (l *Locker) Unlock() {
	<-l.c
	if l.gauge != nil {
		l.gauge.Dec()
	}
}

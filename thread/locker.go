package thread

type Semaphore struct {
	c chan struct{}
}

var SyncSemaphore *Semaphore
var AsyncSemaphore *Semaphore

func NewSemaphore(count int) *Semaphore {
	return &Semaphore{c: make(chan struct{}, count)}
}

func (l *Semaphore) Acquire() {
	l.c <- struct{}{}
}

func (l *Semaphore) Release() {
	<-l.c
}

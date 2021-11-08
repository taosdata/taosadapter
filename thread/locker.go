package thread

import (
	"runtime"
)

var c chan struct{}

func Lock() {
	c <- struct{}{}
}

func Unlock() {
	<-c
}

func init() {
	c = make(chan struct{}, runtime.NumCPU())
}

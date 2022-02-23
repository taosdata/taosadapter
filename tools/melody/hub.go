package melody

import (
	"sync"
	"sync/atomic"
)

var nodata = struct{}{}

type hub struct {
	sessions   map[*Session]struct{}
	broadcast  chan *envelope
	register   chan *Session
	unregister chan *Session
	exit       chan *envelope
	status     uint32
	open       bool
	rwMutex    *sync.RWMutex
}

func newHub() *hub {
	return &hub{
		sessions:   make(map[*Session]struct{}),
		broadcast:  make(chan *envelope),
		register:   make(chan *Session),
		unregister: make(chan *Session),
		exit:       make(chan *envelope),
		status:     StatusNormal,
		rwMutex:    &sync.RWMutex{},
	}
}

func (h *hub) run() {
	for {
		select {
		case s := <-h.register:
			h.rwMutex.Lock()
			h.sessions[s] = nodata
			h.rwMutex.Unlock()
		case s := <-h.unregister:
			if _, ok := h.sessions[s]; ok {
				h.rwMutex.Lock()
				delete(h.sessions, s)
				h.rwMutex.Unlock()
			}
		case m := <-h.broadcast:
			h.rwMutex.RLock()
			for s := range h.sessions {
				if m.filter != nil {
					if m.filter(s) {
						s.writeMessage(m)
					}
				} else {
					s.writeMessage(m)
				}
			}
			h.rwMutex.RUnlock()
		case m := <-h.exit:
			h.rwMutex.Lock()
			for s := range h.sessions {
				s.writeMessage(m)
				delete(h.sessions, s)
				s.Close()
			}
			atomic.StoreUint32(&h.status, StatusStop)
			h.rwMutex.Unlock()
			return
		}
	}
}

func (h *hub) closed() bool {
	return atomic.LoadUint32(&h.status) == StatusStop
}

func (h *hub) len() int {
	h.rwMutex.RLock()
	defer h.rwMutex.RUnlock()
	return len(h.sessions)
}

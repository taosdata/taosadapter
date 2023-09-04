package ws

import (
	"bytes"
	"container/list"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/thread"
)

type QueryResult struct {
	index       uint64
	TaosResult  unsafe.Pointer
	FieldsCount int
	Header      *wrapper.RowsHeader
	Lengths     []int
	Size        int
	Block       unsafe.Pointer
	precision   int
	buffer      *bytes.Buffer
	sync.Mutex
}

func (r *QueryResult) free() {
	r.Lock()
	defer r.Unlock()

	r.Block = nil
	if r.TaosResult != nil {
		thread.Lock()
		wrapper.TaosFreeResult(r.TaosResult)
		thread.Unlock()
	}
}

type QueryResultHolder struct {
	index   uint64
	results *list.List
	sync.RWMutex
}

func NewQueryResultHolder() *QueryResultHolder {
	return &QueryResultHolder{results: list.New()}
}

func (h *QueryResultHolder) Add(result *QueryResult) uint64 {
	h.Lock()
	defer h.Unlock()

	atomic.AddUint64(&h.index, 1)
	result.index = h.index
	h.results.PushBack(result)

	return result.index
}

func (h *QueryResultHolder) Get(index uint64) *QueryResult {
	h.RLock()
	defer h.RUnlock()

	node := h.results.Front()
	for {
		if node == nil {
			return nil
		}
		result := node.Value.(*QueryResult)
		if result.index == index {
			return result
		}
		node = node.Next()
	}
}

func (h *QueryResultHolder) FreeResultByID(index uint64) {
	h.Lock()
	defer h.Unlock()
	node := h.results.Front()
	for {
		if node == nil {
			return
		}
		result := node.Value.(*QueryResult)
		if result.index == index {
			h.results.Remove(node)
			result.free()
			return
		}
		node = node.Next()
	}
}

func (h *QueryResultHolder) FreeResult(item *QueryResult) {
	h.Lock()
	defer h.Unlock()

	node := h.results.Front()
	for {
		if node == nil {
			return
		}
		result := node.Value.(*QueryResult)
		if result == item {
			h.results.Remove(node)
			result.free()
			return
		}
		node = node.Next()
	}
}

func (h *QueryResultHolder) FreeAll() {
	h.Lock()
	defer h.Unlock()

	node := h.results.Front()
	for {
		if node == nil {
			return
		}
		next := node.Next()
		result := node.Value.(*QueryResult)
		result.free()
		h.results.Remove(node)
		node = next
	}
}

type StmtItem struct {
	index uint64
	stmt  unsafe.Pointer
	sync.RWMutex
}

func (s *StmtItem) free() {
	s.Lock()
	defer s.Unlock()

	if s.stmt != nil {
		thread.Lock()
		wrapper.TaosStmtClose(s.stmt)
		thread.Unlock()
	}
}

type StmtHolder struct {
	index   uint64
	results *list.List
	sync.RWMutex
}

func NewStmtHolder() *StmtHolder {
	return &StmtHolder{results: list.New()}
}

func (h *StmtHolder) Add(item *StmtItem) uint64 {
	h.Lock()
	defer h.Unlock()

	atomic.AddUint64(&h.index, 1)
	item.index = h.index
	h.results.PushBack(item)

	return item.index
}

func (h *StmtHolder) Get(index uint64) *StmtItem {
	h.RLock()
	defer h.RUnlock()

	node := h.results.Front()
	for {
		if node == nil {
			return nil
		}
		result := node.Value.(*StmtItem)
		if result.index == index {
			return result
		}
		node = node.Next()
	}
}

func (h *StmtHolder) FreeResultByID(index uint64) {
	h.Lock()
	defer h.Unlock()
	node := h.results.Front()
	for {
		if node == nil {
			return
		}
		result := node.Value.(*StmtItem)
		if result.index == index {
			h.results.Remove(node)
			result.free()
			return
		}
		node = node.Next()
	}
}

func (h *StmtHolder) FreeResult(item *StmtItem) {
	h.Lock()
	defer h.Unlock()

	node := h.results.Front()
	for {
		if node == nil {
			return
		}
		result := node.Value.(*StmtItem)
		if result == item {
			h.results.Remove(node)
			result.free()
			return
		}
		node = node.Next()
	}
}

func (h *StmtHolder) FreeAll() {
	h.Lock()
	defer h.Unlock()

	node := h.results.Front()
	for {
		if node == nil {
			return
		}
		next := node.Next()
		result := node.Value.(*StmtItem)
		result.free()
		h.results.Remove(node)
		node = next
	}
}

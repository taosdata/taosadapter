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
	inStmt      bool
	sync.Mutex
}

func (r *QueryResult) free() {
	r.Lock()
	defer r.Unlock()

	r.Block = nil
	if r.TaosResult == nil {
		return
	}

	if r.inStmt { // stmt result is no need to free
		return
	}

	thread.Lock()
	wrapper.TaosFreeResult(r.TaosResult)
	thread.Unlock()
	r.TaosResult = nil
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
		if node == nil || node.Value == nil {
			return nil
		}

		if result := node.Value.(*QueryResult); result.index == index {
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
		if node == nil || node.Value == nil {
			return
		}

		if result := node.Value.(*QueryResult); result.index == index {
			result.free()
			h.results.Remove(node)
			return
		}
		node = node.Next()
	}
}

func (h *QueryResultHolder) FreeAll() {
	h.Lock()
	defer h.Unlock()

	if h.results.Len() == 0 {
		return
	}

	node := h.results.Front()
	for {
		if node == nil || node.Value == nil {
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
	index    uint64
	stmt     unsafe.Pointer
	isInsert bool
	sync.Mutex
}

func (s *StmtItem) free() {
	s.Lock()
	defer s.Unlock()

	if s.stmt == nil {
		return
	}

	thread.Lock()
	wrapper.TaosStmtClose(s.stmt)
	thread.Unlock()
	s.stmt = nil
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

func (h *StmtHolder) FreeStmtByID(index uint64) {
	h.Lock()
	defer h.Unlock()

	if h.results.Len() == 0 {
		return
	}

	node := h.results.Front()
	for {
		if node == nil || node.Value == nil {
			return
		}
		result := node.Value.(*StmtItem)
		if result.index == index {
			result.free()
			h.results.Remove(node)
			return
		}
		node = node.Next()
	}
}

func (h *StmtHolder) FreeAll() {
	h.Lock()
	defer h.Unlock()

	if h.results.Len() == 0 {
		return
	}

	node := h.results.Front()
	for {
		if node == nil || node.Value == nil {
			return
		}
		next := node.Next()
		result := node.Value.(*StmtItem)
		result.free()
		h.results.Remove(node)
		node = next
	}
}

package ws

import (
	"container/list"
	"errors"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/monitor/recordsql"
	"github.com/taosdata/taosadapter/v3/tools/limiter"
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
	buf         []byte
	inStmt      bool
	record      *recordsql.Record
	limiter     *limiter.Limiter
	sync.Mutex
}

func (r *QueryResult) free(logger *logrus.Entry) {
	r.Lock()
	defer r.Unlock()

	r.Block = nil
	if r.TaosResult == nil {
		return
	}
	if r.record != nil {
		r.record.SetFreeTime(time.Now())
		recordsql.PutSQLRecord(r.record)
		r.record = nil
	}
	if r.limiter != nil {
		r.limiter.Release()
	}
	if r.inStmt { // stmt result is no need to free
		logger.Trace("stmt result is no need to free")
		r.TaosResult = nil
		return
	}
	logger.Tracef("free result:%d", r.index)
	async.FreeResultAsync(r.TaosResult, logger, log.IsDebug())
	r.TaosResult = nil
	monitor.WSWSSqlResultCount.Dec()
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
	result.index = atomic.AddUint64(&h.index, 1)
	h.results.PushBack(result)
	if !result.inStmt {
		monitor.WSWSSqlResultCount.Inc()
	}
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

func (h *QueryResultHolder) FreeResultByID(index uint64, logger *logrus.Entry) {
	h.Lock()
	defer h.Unlock()

	node := h.results.Front()
	for {
		if node == nil || node.Value == nil {
			return
		}

		if result := node.Value.(*QueryResult); result.index == index {
			result.free(logger)
			h.results.Remove(node)
			return
		}
		node = node.Next()
	}
}

func (h *QueryResultHolder) FreeAll(logger *logrus.Entry) {
	h.Lock()
	defer h.Unlock()
	defer func() {
		h.results = h.results.Init()
	}()
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
		result.free(logger)
		h.results.Remove(node)
		node = next
	}
}

type StmtItem struct {
	index    uint64
	stmt     unsafe.Pointer
	isInsert bool
	isStmt2  bool
	result   unsafe.Pointer
	handler  cgo.Handle
	caller   *async.Stmt2CallBackCaller
	sync.Mutex
}

func (s *StmtItem) free(logger *logrus.Entry) {
	s.Lock()
	defer s.Unlock()

	if s.stmt == nil {
		return
	}
	if s.isStmt2 {
		syncinterface.TaosStmt2Close(s.stmt, logger, log.IsDebug())
		async.GlobalStmt2CallBackCallerPool.Put(s.handler)
		monitor.WSWSStmt2Count.Dec()
	} else {
		syncinterface.TaosStmtClose(s.stmt, logger, log.IsDebug())
		monitor.WSWSStmtCount.Dec()
	}

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

	item.index = atomic.AddUint64(&h.index, 1)
	h.results.PushBack(item)
	if item.isStmt2 {
		monitor.WSWSStmt2Count.Inc()
	} else {
		monitor.WSWSStmtCount.Inc()
	}
	return item.index
}

func (h *StmtHolder) Get(index uint64) *StmtItem {
	item := h.getByIndex(index)
	if item != nil && item.isStmt2 {
		return nil
	}
	return item
}

func (h *StmtHolder) getByIndex(index uint64) *StmtItem {
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

func (h *StmtHolder) GetStmt2(index uint64) *StmtItem {
	item := h.getByIndex(index)
	if item != nil && !item.isStmt2 {
		return nil
	}
	return item
}

func (h *StmtHolder) FreeStmtByID(index uint64, isStmt2 bool, logger *logrus.Entry) error {
	h.Lock()
	defer h.Unlock()

	if h.results.Len() == 0 {
		return nil
	}

	node := h.results.Front()
	for {
		if node == nil || node.Value == nil {
			return nil
		}
		result := node.Value.(*StmtItem)
		if result.index == index {
			if result.isStmt2 != isStmt2 {
				return errors.New("stmt type not match")
			}
			result.free(logger)
			h.results.Remove(node)
			return nil
		}
		node = node.Next()
	}
}

func (h *StmtHolder) FreeAll(logger *logrus.Entry) {
	h.Lock()
	defer h.Unlock()
	defer func() {
		h.results = h.results.Init()
	}()
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
		result.free(logger)
		h.results.Remove(node)
		node = next
	}
}

package rest

/*
#include <taos.h>
*/
import "C"
import (
	"bytes"
	"container/list"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/huskar-t/melody"
	"github.com/sirupsen/logrus"
	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/db/async"
	"github.com/taosdata/taosadapter/httperror"
	"github.com/taosdata/taosadapter/thread"
	"github.com/taosdata/taosadapter/tools/jsontype"
	"github.com/taosdata/taosadapter/tools/web"
)

const TaosSessionKey = "taos"
const (
	ClientVersion = "version"
	WSConnect     = "conn"
	WSQuery       = "query"
	WSFetch       = "fetch"
	WSFetchBlock  = "fetch_block"
	WSFreeResult  = "free_result"
	WSWriteMeta   = "write_meta"
)

type Taos struct {
	conn         unsafe.Pointer
	resultLocker sync.RWMutex
	Results      *list.List
	resultIndex  uint64
	closed       bool
	sync.Mutex
}

func NewTaos() *Taos {
	return &Taos{Results: list.New()}
}

type Result struct {
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

func (r *Result) FreeResult() {
	r.Lock()
	r.FieldsCount = 0
	r.Header = nil
	r.Lengths = nil
	r.Size = 0
	r.precision = 0
	r.Block = nil
	if r.TaosResult != nil {
		thread.Lock()
		wrapper.TaosFreeResult(r.TaosResult)
		thread.Unlock()
	}
	r.Unlock()
}

func (t *Taos) addResult(result *Result) {
	index := atomic.AddUint64(&t.resultIndex, 1)
	result.index = index
	t.resultLocker.Lock()
	t.Results.PushBack(result)
	t.resultLocker.Unlock()
}

func (t *Taos) getResult(index uint64) *list.Element {
	t.resultLocker.RLock()
	defer t.resultLocker.RUnlock()
	root := t.Results.Front()
	if root == nil {
		return nil
	}
	rootIndex := root.Value.(*Result).index
	if rootIndex == index {
		return root
	}
	item := root.Next()
	for {
		if item == nil || item == root {
			return nil
		}
		if item.Value.(*Result).index == index {
			return item
		}
		item = item.Next()
	}
}

func (t *Taos) removeResult(item *list.Element) {
	t.resultLocker.Lock()
	t.Results.Remove(item)
	t.resultLocker.Unlock()
}

type WSConnectReq struct {
	ReqID    uint64 `json:"req_id"`
	User     string `json:"user"`
	Password string `json:"password"`
	DB       string `json:"db"`
}

type WSConnectResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
}

func (t *Taos) connect(session *melody.Session, req *WSConnectReq) {
	t.Lock()
	defer t.Unlock()
	if t.conn != nil {
		wsErrorMsg(session, 0xffff, "taos duplicate connections", WSConnect, req.ReqID)
		return
	}
	thread.Lock()
	conn, err := wrapper.TaosConnect("", req.User, req.Password, req.DB, 0)
	thread.Unlock()
	if err != nil {
		wsError(session, err, WSConnect, req.ReqID)
		return
	}
	t.conn = conn
	wsWriteJson(session, &WSConnectResp{
		Action: WSConnect,
		ReqID:  req.ReqID,
	})
}

type WSQueryReq struct {
	ReqID uint64 `json:"req_id"`
	SQL   string `json:"sql"`
}

type WSQueryResult struct {
	Code          int                `json:"code"`
	Message       string             `json:"message"`
	Action        string             `json:"action"`
	ReqID         uint64             `json:"req_id"`
	ID            uint64             `json:"id"`
	IsUpdate      bool               `json:"is_update"`
	AffectedRows  int                `json:"affected_rows"`
	FieldsCount   int                `json:"fields_count"`
	FieldsNames   []string           `json:"fields_names"`
	FieldsTypes   jsontype.JsonUint8 `json:"fields_types"`
	FieldsLengths []int64            `json:"fields_lengths"`
	Precision     int                `json:"precision"`
}

func (t *Taos) query(session *melody.Session, req *WSQueryReq) {
	if t.conn == nil {
		wsErrorMsg(session, 0xffff, "taos not connected", WSQuery, req.ReqID)
		return
	}
	handler := async.GlobalAsync.HandlerPool.Get()
	defer async.GlobalAsync.HandlerPool.Put(handler)
	result, _ := async.GlobalAsync.TaosQuery(t.conn, req.SQL, handler)
	code := wrapper.TaosError(result.Res)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosErrorStr(result.Res)
		thread.Lock()
		wrapper.TaosFreeResult(result.Res)
		thread.Unlock()
		wsErrorMsg(session, code, errStr, WSQuery, req.ReqID)
		return
	}
	isUpdate := wrapper.TaosIsUpdateQuery(result.Res)
	queryResult := &WSQueryResult{Action: WSQuery, ReqID: req.ReqID}
	if isUpdate {
		var affectRows int
		affectRows = wrapper.TaosAffectedRows(result.Res)
		queryResult.IsUpdate = true
		queryResult.AffectedRows = affectRows
		thread.Lock()
		wrapper.TaosFreeResult(result.Res)
		thread.Unlock()
		wsWriteJson(session, queryResult)
		return
	} else {
		fieldsCount := wrapper.TaosNumFields(result.Res)
		queryResult.FieldsCount = fieldsCount
		rowsHeader, _ := wrapper.ReadColumn(result.Res, fieldsCount)
		queryResult.FieldsNames = rowsHeader.ColNames
		queryResult.FieldsLengths = rowsHeader.ColLength
		queryResult.FieldsTypes = rowsHeader.ColTypes
		precision := wrapper.TaosResultPrecision(result.Res)
		queryResult.Precision = precision
		result := &Result{
			TaosResult:  result.Res,
			FieldsCount: fieldsCount,
			Header:      rowsHeader,
			precision:   precision,
		}
		t.addResult(result)
		queryResult.ID = result.index
		wsWriteJson(session, queryResult)
	}
}

type WSWriteMetaResp struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	Action    string `json:"action"`
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}

func (t *Taos) writeMeta(session *melody.Session, reqID, messageID uint64, length uint32, metaType uint16, data unsafe.Pointer) {
	t.Lock()
	defer t.Unlock()
	if t.conn == nil {
		wsTMQErrorMsg(session, 0xffff, "taos not connected", WSWriteMeta, reqID, &messageID)
		return
	}
	handler := async.GlobalAsync.HandlerPool.Get()
	defer async.GlobalAsync.HandlerPool.Put(handler)
	meta := wrapper.BuildRawMeta(length, metaType, data)
	thread.Lock()
	wrapper.TaosWriteRawMeta(t.conn, meta)
	thread.Unlock()
	resp := &WSWriteMetaResp{Action: WSWriteMeta, ReqID: reqID, MessageID: messageID}
	wsWriteJson(session, resp)
}

type WSFetchReq struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

type WSFetchResp struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	Action    string `json:"action"`
	ReqID     uint64 `json:"req_id"`
	ID        uint64 `json:"id"`
	Completed bool   `json:"completed"`
	Lengths   []int  `json:"lengths"`
	Rows      int    `json:"rows"`
}

func (t *Taos) fetch(session *melody.Session, req *WSFetchReq) {
	if t.conn == nil {
		wsErrorMsg(session, 0xffff, "taos not connected", WSFetch, req.ReqID)
		return
	}
	resultItem := t.getResult(req.ID)
	if resultItem == nil {
		wsErrorMsg(session, 0xffff, "result is nil", WSFetch, req.ReqID)
		return
	}
	resultS := resultItem.Value.(*Result)
	handler := async.GlobalAsync.HandlerPool.Get()
	defer async.GlobalAsync.HandlerPool.Put(handler)
	result, _ := async.GlobalAsync.TaosFetchRawBlockA(resultS.TaosResult, handler)
	if result.N == 0 {
		t.FreeResult(resultItem)
		wsWriteJson(session, &WSFetchResp{
			Action:    WSFetch,
			ReqID:     req.ReqID,
			ID:        req.ID,
			Completed: true,
		})
		return
	}
	if result.N < 0 {
		errStr := wrapper.TaosErrorStr(result.Res)
		t.FreeResult(resultItem)
		wsErrorMsg(session, result.N&0xffff, errStr, WSFetch, req.ReqID)
		return
	}
	resultS.Lengths = wrapper.FetchLengths(resultS.TaosResult, resultS.FieldsCount)
	block := wrapper.TaosGetRawBlock(resultS.TaosResult)
	resultS.Block = block
	resultS.Size = result.N

	wsWriteJson(session, &WSFetchResp{
		Action:  WSFetch,
		ReqID:   req.ReqID,
		ID:      req.ID,
		Lengths: resultS.Lengths,
		Rows:    result.N,
	})
}

type WSFetchBlockReq struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

func (t *Taos) fetchBlock(session *melody.Session, req *WSFetchBlockReq) {
	if t.conn == nil {
		wsErrorMsg(session, 0xffff, "taos not connected", WSFetchBlock, req.ReqID)
		return
	}
	resultItem := t.getResult(req.ID)
	if resultItem == nil {
		wsErrorMsg(session, 0xffff, "result is nil", WSFetchBlock, req.ReqID)
		return
	}
	resultS := resultItem.Value.(*Result)
	if resultS.Block == nil {
		wsErrorMsg(session, 0xffff, "block is nil", WSFetchBlock, req.ReqID)
		return
	}
	resultS.Lock()
	blockLength := int(*(*int32)(resultS.Block))
	if resultS.buffer == nil {
		resultS.buffer = new(bytes.Buffer)
	} else {
		resultS.buffer.Reset()
	}
	resultS.buffer.Grow(blockLength + 8)
	writeUint64(resultS.buffer, req.ID)
	for offset := 0; offset < blockLength; offset++ {
		resultS.buffer.WriteByte(*((*byte)(unsafe.Pointer(uintptr(resultS.Block) + uintptr(offset)))))
	}
	b := resultS.buffer.Bytes()
	resultS.Unlock()
	session.WriteBinary(b)
}

type WSFreeResultReq struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

func (t *Taos) freeResult(session *melody.Session, req *WSFreeResultReq) {
	if t.conn == nil {
		return
	}
	resultItem := t.getResult(req.ID)
	if resultItem == nil {
		return
	}
	resultS, ok := resultItem.Value.(*Result)
	if ok && resultS != nil {
		t.removeResult(resultItem)
		resultS.FreeResult()
	}
}

type Writer struct {
	session *melody.Session
}

func (w *Writer) Write(p []byte) (int, error) {
	err := w.session.Write(p)
	return 0, err
}

func (t *Taos) FreeResult(element *list.Element) {
	if element == nil {
		return
	}
	r := element.Value.(*Result)
	if r != nil {
		r.FreeResult()
	}
	t.removeResult(element)
}

func (t *Taos) freeAllResult() {
	t.resultLocker.Lock()
	defer t.resultLocker.Unlock()
	root := t.Results.Front()
	if root == nil {
		return
	}
	root.Value.(*Result).FreeResult()
	item := root.Next()
	for {
		if item == nil || item == root {
			return
		}
		item.Value.(*Result).FreeResult()
		item = item.Next()
	}
}

func (t *Taos) Close() {
	t.Lock()
	defer t.Unlock()
	if t.closed {
		return
	}
	t.closed = true
	t.freeAllResult()
	if t.conn != nil {
		wrapper.TaosClose(t.conn)
		t.conn = nil
	}
}

type WSAction struct {
	Action string          `json:"action"`
	Args   json.RawMessage `json:"args"`
}

type WSVersionResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	Version string `json:"version"`
}

func (ctl *Restful) InitWS() {
	ctl.wsM = melody.New()
	ctl.wsM.Config.MaxMessageSize = 4 * 1024 * 1024

	ctl.wsM.HandleConnect(func(session *melody.Session) {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws connect")
		session.Set(TaosSessionKey, NewTaos())
	})

	ctl.wsM.HandleMessage(func(session *melody.Session, data []byte) {
		if ctl.wsM.IsClosed() {
			return
		}
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("get ws message data:", string(data))
		var action WSAction
		err := json.Unmarshal(data, &action)
		if err != nil {
			logger.WithError(err).Errorln("unmarshal ws request")
			return
		}
		switch action.Action {
		case ClientVersion:
			session.Write(ctl.wsVersionResp)
		case WSConnect:
			var wsConnect WSConnectReq
			err = json.Unmarshal(action.Args, &wsConnect)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal connect request args")
				return
			}
			t := session.MustGet(TaosSessionKey)
			t.(*Taos).connect(session, &wsConnect)
		case WSQuery:
			var wsQuery WSQueryReq
			err = json.Unmarshal(action.Args, &wsQuery)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal query args")
				return
			}
			t := session.MustGet(TaosSessionKey)
			t.(*Taos).query(session, &wsQuery)
		case WSFetch:
			var wsFetch WSFetchReq
			err = json.Unmarshal(action.Args, &wsFetch)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal fetch args")
				return
			}
			t := session.MustGet(TaosSessionKey)
			t.(*Taos).fetch(session, &wsFetch)
		case WSFetchBlock:
			var fetchBlock WSFetchBlockReq
			err = json.Unmarshal(action.Args, &fetchBlock)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal fetch_block args")
				return
			}
			t := session.MustGet(TaosSessionKey)
			t.(*Taos).fetchBlock(session, &fetchBlock)
		case WSFreeResult:
			var fetchJson WSFreeResultReq
			err = json.Unmarshal(action.Args, &fetchJson)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal fetch_json args")
				return
			}
			t := session.MustGet(TaosSessionKey)
			t.(*Taos).freeResult(session, &fetchJson)
		default:
			logger.WithError(err).Errorln("unknown action :" + action.Action)
			return
		}
	})

	ctl.wsM.HandleMessageBinary(func(session *melody.Session, data []byte) {
		p0 := *(*uintptr)(unsafe.Pointer(&data))
		reqID := *(*uint64)(unsafe.Pointer(p0))
		messageID := *(*uint64)(unsafe.Pointer(p0 + uintptr(8)))
		action := *(*uint64)(unsafe.Pointer(p0 + uintptr(16)))
		length := *(*uint32)(unsafe.Pointer(p0 + uintptr(24)))
		metaType := *(*uint16)(unsafe.Pointer(p0 + uintptr(28)))
		b := unsafe.Pointer(p0 + uintptr(30))
		switch action {
		case TMQRawMetaMessage:
			t := session.MustGet(TaosSessionKey)
			t.(*Taos).writeMeta(session, reqID, messageID, length, metaType, b)
		}
	})

	ctl.wsM.HandleClose(func(session *melody.Session, i int, s string) error {
		message := melody.FormatCloseMessage(i, "")
		session.WriteControl(websocket.CloseMessage, message, time.Now().Add(time.Second))
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws close", i, s)
		t, exist := session.Get(TaosSessionKey)
		if exist && t != nil {
			t.(*Taos).Close()
		}
		return nil
	})

	ctl.wsM.HandleError(func(session *melody.Session, err error) {
		logger := session.MustGet("logger").(*logrus.Entry)
		_, is := err.(*websocket.CloseError)
		if is {
			logger.WithError(err).Debugln("ws close in error")
		} else {
			logger.WithError(err).Errorln("ws error")
		}
		t, exist := session.Get(TaosSessionKey)
		if exist && t != nil {
			t.(*Taos).Close()
		}
	})

	ctl.wsM.HandleDisconnect(func(session *melody.Session) {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws disconnect")
		t, exist := session.Get(TaosSessionKey)
		if exist && t != nil {
			t.(*Taos).Close()
		}
	})
}

func (ctl *Restful) ws(c *gin.Context) {
	id := web.GetRequestID(c)
	loggerWithID := logger.WithField("sessionID", id).WithField("wsType", "query")
	_ = ctl.wsM.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": loggerWithID})
}

type WSErrorResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
}

func wsErrorMsg(session *melody.Session, code int, message string, action string, reqID uint64) {
	b, _ := json.Marshal(&WSErrorResp{
		Code:    code & 0xffff,
		Message: message,
		Action:  action,
		ReqID:   reqID,
	})
	session.Write(b)
}
func wsError(session *melody.Session, err error, action string, reqID uint64) {
	e, is := err.(*tErrors.TaosError)
	if is {
		wsErrorMsg(session, int(e.Code)&0xffff, e.ErrStr, action, reqID)
	} else {
		wsErrorMsg(session, 0xffff, err.Error(), action, reqID)
	}
}
func wsWriteJson(session *melody.Session, data interface{}) {
	b, _ := json.Marshal(data)
	session.Write(b)
}

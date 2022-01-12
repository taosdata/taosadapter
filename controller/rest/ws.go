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
	"github.com/huskar-t/melody"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v2/common"
	tErrors "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/db/async"
	"github.com/taosdata/taosadapter/db/commonpool"
	"github.com/taosdata/taosadapter/httperror"
	"github.com/taosdata/taosadapter/thread"
	"github.com/taosdata/taosadapter/tools/ctools"
	"github.com/taosdata/taosadapter/tools/jsonbuilder"
	"github.com/taosdata/taosadapter/tools/web"
)

const TaosSessionKey = "taos"

type Taos struct {
	Session      *melody.Session
	Conn         *commonpool.Conn
	resultLocker sync.RWMutex
	Results      *list.List
	resultIndex  uint64
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
	createTime  time.Time
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

func (t *Taos) AddResult(result *Result) {
	index := atomic.AddUint64(&t.resultIndex, 1)
	result.index = index
	t.resultLocker.Lock()
	t.Results.PushBack(result)
	t.resultLocker.Unlock()
}

func (t *Taos) GetResult(index uint64) *list.Element {
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

type ConnectResp struct {
	WSError
}

func (t *Taos) connect(session *melody.Session, user string, password string) {
	t.Lock()
	defer t.Unlock()
	if t.Conn != nil {
		wsErrorMsg(session, 0xffff, "taos duplicate connections")
		return
	}
	conn, err := commonpool.GetConnection(user, password)
	if err != nil {
		wsError(session, err)
		return
	}
	t.Conn = conn
	wsWriteJson(session, &ConnectResp{})
}

type WSQueryResult struct {
	WSError
	ID            uint64   `json:"id"`
	IsUpdate      bool     `json:"is_update"`
	AffectedRows  int      `json:"affected_rows"`
	FieldsCount   int      `json:"fields_count"`
	FieldsNames   []string `json:"fields_names"`
	FieldsTypes   []int    `json:"fields_types"`
	FieldsLengths []int    `json:"fields_lengths"`
	Precision     int      `json:"precision"`
}

func (t *Taos) query(session *melody.Session, sql string) {
	if t.Conn == nil {
		wsErrorMsg(session, 0xffff, "taos not connected")
		return
	}
	handler := async.GlobalAsync.HandlerPool.Get()
	defer async.GlobalAsync.HandlerPool.Put(handler)
	result, _ := async.GlobalAsync.TaosQuery(t.Conn.TaosConnection, sql, handler)
	code := wrapper.TaosError(result.Res)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosErrorStr(result.Res)
		wsErrorMsg(session, code, errStr)
		thread.Lock()
		wrapper.TaosFreeResult(result.Res)
		thread.Unlock()
		return
	}
	isUpdate := wrapper.TaosIsUpdateQuery(result.Res)
	queryResult := &WSQueryResult{}
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
		queryResult.FieldsLengths = make([]int, fieldsCount)
		queryResult.FieldsTypes = make([]int, fieldsCount)
		for i := 0; i < fieldsCount; i++ {
			queryResult.FieldsLengths[i] = int(rowsHeader.ColLength[i])
			queryResult.FieldsTypes[i] = int(rowsHeader.ColTypes[i])
		}
		precision := wrapper.TaosResultPrecision(result.Res)
		queryResult.Precision = precision
		result := &Result{
			TaosResult:  result.Res,
			FieldsCount: fieldsCount,
			Header:      rowsHeader,
			precision:   precision,
			createTime:  time.Now(),
		}
		t.AddResult(result)
		queryResult.ID = result.index
		wsWriteJson(session, queryResult)
	}
}

type WSFetchResp struct {
	WSError
	Completed bool  `json:"completed"`
	Lengths   []int `json:"lengths"`
	Rows      int   `json:"rows"`
}

func (t *Taos) fetch(session *melody.Session, id uint64) {
	if t.Conn == nil {
		wsErrorMsg(session, 0xffff, "taos not connected")
		return
	}
	resultItem := t.GetResult(id)
	if resultItem == nil {
		wsErrorMsg(session, 0xffff, "result is nil")
		return
	}
	resultS := resultItem.Value.(*Result)
	handler := async.GlobalAsync.HandlerPool.Get()
	defer async.GlobalAsync.HandlerPool.Put(handler)
	result, _ := async.GlobalAsync.TaosFetchRowsA(resultS.TaosResult, handler)
	if result.N == 0 {
		t.FreeResult(resultItem)
		wsWriteJson(session, &WSFetchResp{
			Completed: true,
		})
		return
	}
	if result.N < 0 {
		errStr := wrapper.TaosErrorStr(result.Res)
		t.FreeResult(resultItem)
		wsWriteJson(session, &WSFetchResp{
			WSError: WSError{
				Code:    result.N & 0xffff,
				Message: errStr,
			},
		})
		return
	}
	resultS.Lengths = wrapper.FetchLengths(resultS.TaosResult, resultS.FieldsCount)
	block := wrapper.TaosResultBlock(resultS.TaosResult)
	resultS.Block = block
	resultS.Size = result.N

	wsWriteJson(session, &WSFetchResp{
		Lengths: resultS.Lengths,
		Rows:    result.N,
	})
}

func (t *Taos) fetchBlock(session *melody.Session, id uint64) {
	if t.Conn == nil {
		wsErrorMsg(session, 0xffff, "taos not connected")
		return
	}
	resultItem := t.GetResult(id)
	if resultItem == nil {
		wsErrorMsg(session, 0xffff, "result is nil")
		return
	}
	resultS := resultItem.Value.(*Result)
	if resultS.Block == nil {
		wsErrorMsg(session, 0xffff, "block is nil")
		return
	}
	resultS.Lock()
	totalDataLength := 0
	for _, length := range resultS.Lengths {
		totalDataLength += length
	}
	totalDataLength *= resultS.Size
	if resultS.buffer == nil {
		resultS.buffer = new(bytes.Buffer)
	} else {
		resultS.buffer.Reset()
	}
	resultS.buffer.Grow(totalDataLength + 8)
	writeUint64(resultS.buffer, id)
	for column := 0; column < resultS.FieldsCount; column++ {
		length := resultS.Lengths[column] * resultS.Size
		colPointer := *(*uintptr)(unsafe.Pointer(uintptr(unsafe.Pointer(*(*C.TAOS_ROW)(resultS.Block))) + uintptr(column)*wrapper.PointerSize))
		for offset := 0; offset < length; offset++ {
			resultS.buffer.WriteByte(*((*byte)(unsafe.Pointer(colPointer + uintptr(offset)))))
		}
	}
	b := resultS.buffer.Bytes()
	resultS.Unlock()
	session.WriteBinary(b)
}

func (t *Taos) fetchJson(session *melody.Session, id uint64) {
	if t.Conn == nil {
		wsErrorMsg(session, 0xffff, "taos not connected")
		return
	}
	resultItem := t.GetResult(id)
	if resultItem == nil {
		wsErrorMsg(session, 0xffff, "result is nil")
		return
	}
	resultS := resultItem.Value.(*Result)
	if resultS.Block == nil {
		wsErrorMsg(session, 0xffff, "block is nil")
		return
	}
	//dest := make([][]driver.Value, t.Result.Size)
	builder := jsonbuilder.BorrowStream(&Writer{session: session})
	builder.WriteArrayStart()
	for rowID := 0; rowID < resultS.Size; rowID++ {
		builder.WriteArrayStart()
		for columnID := 0; columnID < resultS.FieldsCount; columnID++ {
			ctools.JsonWriteBlockValue(builder, resultS.Block, resultS.Header.ColTypes[columnID], columnID, rowID, resultS.Lengths[columnID], resultS.precision, func(builder *jsonbuilder.Stream, ts int64, precision int) {
				switch precision {
				case common.PrecisionNanoSecond:
					builder.WriteInt64(ts)
				case common.PrecisionMicroSecond:
					builder.WriteInt64(ts * 1e3)
				case common.PrecisionMilliSecond:
					builder.WriteInt64(ts * 1e6)
				default:
					builder.WriteNil()
				}
			})
			if columnID != resultS.FieldsCount-1 {
				builder.WriteMore()
			}
		}
		builder.WriteArrayEnd()
		if rowID != resultS.Size-1 {
			builder.WriteMore()
		}
	}
	builder.WriteArrayEnd()
	builder.Flush()
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
	t.freeAllResult()
	if t.Conn != nil {
		t.Conn.Put()
	}
}

type WSAction struct {
	Action string          `json:"action"`
	Args   json.RawMessage `json:"args"`
}

type WSError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (ctl *Restful) InitWS() {
	ctl.m = melody.New()

	ctl.m.HandleConnect(func(session *melody.Session) {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws connect")
		session.Set(TaosSessionKey, NewTaos())
	})

	ctl.m.HandleMessage(func(session *melody.Session, data []byte) {
		if ctl.m.IsClosed() {
			return
		}
		var action WSAction
		err := json.Unmarshal(data, &action)
		if err != nil {
			b, _ := json.Marshal(&WSError{
				Code:    0xffff,
				Message: "json unmarshal:" + err.Error(),
			})
			session.Write(b)
			return
		}
		switch action.Action {
		case "conn":
			var wsConnect WSConnect
			err = json.Unmarshal(action.Args, &wsConnect)
			if err != nil {
				wsErrorMsg(session, 0xffff, "json unmarshal:"+err.Error())
				return
			}
			t := session.MustGet(TaosSessionKey)
			t.(*Taos).connect(session, wsConnect.User, wsConnect.Password)
		case "query":
			t := session.MustGet(TaosSessionKey)
			if t == nil {
				wsErrorMsg(session, 0xffff, "taos is nil")
				return
			}
			var wsQuery WSQuery
			err = json.Unmarshal(action.Args, &wsQuery)
			if err != nil {
				wsErrorMsg(session, 0xffff, "json unmarshal:"+err.Error())
				return
			}
			t.(*Taos).query(session, wsQuery.SQL)
		case "fetch":
			t := session.MustGet(TaosSessionKey)
			if t == nil {
				wsErrorMsg(session, 0xffff, "taos is nil")
				return
			}
			var wsFetch WSFetch
			err = json.Unmarshal(action.Args, &wsFetch)
			if err != nil {
				wsErrorMsg(session, 0xffff, "json unmarshal:"+err.Error())
				return
			}
			t.(*Taos).fetch(session, wsFetch.ID)
		case "fetch_block":
			t := session.MustGet(TaosSessionKey)
			if t == nil {
				wsErrorMsg(session, 0xffff, "taos is nil")
				return
			}
			var fetchBlock WSFetchBlock
			err = json.Unmarshal(action.Args, &fetchBlock)
			if err != nil {
				wsErrorMsg(session, 0xffff, "json unmarshal:"+err.Error())
				return
			}
			t.(*Taos).fetchBlock(session, fetchBlock.ID)
		case "fetch_json":
			t := session.MustGet(TaosSessionKey)
			if t == nil {
				wsErrorMsg(session, 0xffff, "taos is nil")
				return
			}
			var fetchJson WSFetchJson
			err = json.Unmarshal(action.Args, &fetchJson)
			if err != nil {
				wsErrorMsg(session, 0xffff, "json unmarshal:"+err.Error())
				return
			}
			t.(*Taos).fetchJson(session, fetchJson.ID)
		}
	})

	ctl.m.HandleClose(func(session *melody.Session, i int, s string) error {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws close", i, s)
		t, exist := session.Get(TaosSessionKey)
		if exist && t != nil {
			t.(*Taos).Close()
		}
		return nil
	})

	ctl.m.HandleError(func(session *melody.Session, err error) {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.WithError(err).Errorln("ws error")
		t, exist := session.Get(TaosSessionKey)
		if exist && t != nil {
			t.(*Taos).Close()
		}
	})

	ctl.m.HandleDisconnect(func(session *melody.Session) {
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
	loggerWithID := logger.WithField("sessionID", id)
	_ = ctl.m.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": loggerWithID})
}

type WSConnect struct {
	User     string `json:"user"`
	Password string `json:"password"`
}
type WSQuery struct {
	SQL string `json:"sql"`
}
type WSFetch struct {
	ID uint64 `json:"id"`
}
type WSFetchBlock struct {
	ID uint64 `json:"id"`
}
type WSFetchJson struct {
	ID uint64 `json:"id"`
}

func wsErrorMsg(session *melody.Session, code int, message string) {
	b, _ := json.Marshal(&WSError{
		Code:    code,
		Message: message,
	})
	session.Write(b)
}

func wsError(session *melody.Session, err error) {
	e, is := err.(*tErrors.TaosError)
	if is {
		wsErrorMsg(session, int(e.Code), e.ErrStr)
	} else {
		wsErrorMsg(session, 0xffff, err.Error())
	}
}

func wsWriteJson(session *melody.Session, data interface{}) {
	b, _ := json.Marshal(data)
	session.Write(b)
}

func writeUint64(buffer *bytes.Buffer, v uint64) {
	buffer.WriteByte(byte(v))
	buffer.WriteByte(byte(v >> 8))
	buffer.WriteByte(byte(v >> 16))
	buffer.WriteByte(byte(v >> 24))
	buffer.WriteByte(byte(v >> 32))
	buffer.WriteByte(byte(v >> 40))
	buffer.WriteByte(byte(v >> 48))
	buffer.WriteByte(byte(v >> 56))
}

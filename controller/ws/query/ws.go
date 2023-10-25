package query

import (
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/huskar-t/melody"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v3/common/parser"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/thread"
	"github.com/taosdata/taosadapter/v3/tools"
	"github.com/taosdata/taosadapter/v3/tools/jsontype"
)

type QueryController struct {
	queryM *melody.Melody
}

func NewQueryController() *QueryController {
	queryM := melody.New()
	queryM.UpGrader.EnableCompression = true
	queryM.Config.MaxMessageSize = 0

	queryM.HandleConnect(func(session *melody.Session) {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws connect")
		session.Set(TaosSessionKey, NewTaos(session))
	})

	queryM.HandleMessage(func(session *melody.Session, data []byte) {
		if queryM.IsClosed() {
			return
		}
		go func() {
			ctx := context.WithValue(context.Background(), wstool.StartTimeKey, time.Now().UnixNano())
			logger := session.MustGet("logger").(*logrus.Entry)
			logger.Debugln("get ws message data:", string(data))
			var action WSAction
			err := json.Unmarshal(data, &action)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal ws request")
				return
			}
			switch action.Action {
			case wstool.ClientVersion:
				session.Write(wstool.VersionResp)
			case WSConnect:
				var wsConnect WSConnectReq
				err = json.Unmarshal(action.Args, &wsConnect)
				if err != nil {
					logger.WithError(err).Errorln("unmarshal connect request args")
					return
				}
				t := session.MustGet(TaosSessionKey)
				t.(*Taos).connect(ctx, session, &wsConnect)
			case WSQuery:
				var wsQuery WSQueryReq
				err = json.Unmarshal(action.Args, &wsQuery)
				if err != nil {
					logger.WithError(err).WithField(config.ReqIDKey, wsQuery.ReqID).Errorln("unmarshal query args")
					return
				}
				t := session.MustGet(TaosSessionKey)
				t.(*Taos).query(ctx, session, &wsQuery)
			case WSFetch:
				var wsFetch WSFetchReq
				err = json.Unmarshal(action.Args, &wsFetch)
				if err != nil {
					logger.WithError(err).WithField(config.ReqIDKey, wsFetch.ReqID).Errorln("unmarshal fetch args")
					return
				}
				t := session.MustGet(TaosSessionKey)
				t.(*Taos).fetch(ctx, session, &wsFetch)
			case WSFetchBlock:
				var fetchBlock WSFetchBlockReq
				err = json.Unmarshal(action.Args, &fetchBlock)
				if err != nil {
					logger.WithError(err).WithField(config.ReqIDKey, fetchBlock.ReqID).Errorln("unmarshal fetch_block args")
					return
				}
				t := session.MustGet(TaosSessionKey)
				t.(*Taos).fetchBlock(ctx, session, &fetchBlock)
			case WSFreeResult:
				var fetchJson WSFreeResultReq
				err = json.Unmarshal(action.Args, &fetchJson)
				if err != nil {
					logger.WithError(err).WithField(config.ReqIDKey, fetchJson.ReqID).Errorln("unmarshal fetch_json args")
					return
				}
				t := session.MustGet(TaosSessionKey)
				t.(*Taos).freeResult(session, &fetchJson)
			default:
				logger.WithError(err).Errorln("unknown action :" + action.Action)
				return
			}
		}()

	})

	queryM.HandleMessageBinary(func(session *melody.Session, data []byte) {
		if queryM.IsClosed() {
			return
		}
		go func() {
			ctx := context.WithValue(context.Background(), wstool.StartTimeKey, time.Now().UnixNano())
			logger := session.MustGet("logger").(*logrus.Entry)
			logger.Debugln("get ws block message data:", data)
			p0 := unsafe.Pointer(&data[0])
			reqID := *(*uint64)(p0)
			messageID := *(*uint64)(tools.AddPointer(p0, uintptr(8)))
			action := *(*uint64)(tools.AddPointer(p0, uintptr(16)))
			switch action {
			case TMQRawMessage:
				length := *(*uint32)(tools.AddPointer(p0, uintptr(24)))
				metaType := *(*uint16)(tools.AddPointer(p0, uintptr(28)))
				b := tools.AddPointer(p0, uintptr(30))
				t := session.MustGet(TaosSessionKey)
				t.(*Taos).writeRaw(ctx, session, reqID, messageID, length, metaType, b)
			case RawBlockMessage:
				numOfRows := *(*int32)(tools.AddPointer(p0, uintptr(24)))
				tableNameLength := *(*uint16)(tools.AddPointer(p0, uintptr(28)))
				tableName := make([]byte, tableNameLength)
				for i := 0; i < int(tableNameLength); i++ {
					tableName[i] = *(*byte)(tools.AddPointer(p0, uintptr(30+i)))
				}
				b := tools.AddPointer(p0, uintptr(30+tableNameLength))
				t := session.MustGet(TaosSessionKey)
				t.(*Taos).writeRawBlock(ctx, session, reqID, int(numOfRows), string(tableName), b)
			case RawBlockMessageWithFields:
				numOfRows := *(*int32)(tools.AddPointer(p0, uintptr(24)))
				tableNameLength := int(*(*uint16)(tools.AddPointer(p0, uintptr(28))))
				tableName := make([]byte, tableNameLength)
				for i := 0; i < tableNameLength; i++ {
					tableName[i] = *(*byte)(tools.AddPointer(p0, uintptr(30+i)))
				}
				b := tools.AddPointer(p0, uintptr(30+tableNameLength))
				blockLength := int(parser.RawBlockGetLength(b))
				numOfColumn := int(parser.RawBlockGetNumOfCols(b))
				fieldsBlock := tools.AddPointer(p0, uintptr(30+tableNameLength+blockLength))
				t := session.MustGet(TaosSessionKey)
				t.(*Taos).writeRawBlockWithFields(ctx, session, reqID, int(numOfRows), string(tableName), b, fieldsBlock, numOfColumn)
			}
		}()
	})

	queryM.HandleClose(func(session *melody.Session, i int, s string) error {
		//message := melody.FormatCloseMessage(i, "")
		//session.WriteControl(websocket.CloseMessage, message, time.Now().Add(time.Second))
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws close", i, s)
		t, exist := session.Get(TaosSessionKey)
		if exist && t != nil {
			t.(*Taos).Close()
		}
		return nil
	})

	queryM.HandleError(func(session *melody.Session, err error) {
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

	queryM.HandleDisconnect(func(session *melody.Session) {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws disconnect")
		t, exist := session.Get(TaosSessionKey)
		if exist && t != nil {
			t.(*Taos).Close()
		}
	})
	return &QueryController{queryM: queryM}
}

func (s *QueryController) Init(ctl gin.IRouter) {
	ctl.GET("rest/ws", func(c *gin.Context) {
		logger := log.GetLogger("ws").WithField("wsType", "query")
		_ = s.queryM.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": logger})
	})
}

type Taos struct {
	conn         unsafe.Pointer
	resultLocker sync.RWMutex
	Results      *list.List
	resultIndex  uint64
	closed       bool
	ipStr        string
	sync.Mutex
}

func NewTaos(session *melody.Session) *Taos {
	host, _, _ := net.SplitHostPort(strings.TrimSpace(session.Request.RemoteAddr))
	ipAddr := net.ParseIP(host)
	return &Taos{Results: list.New(), ipStr: ipAddr.String()}
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
	Timing  int64  `json:"timing"`
}

func (t *Taos) connect(ctx context.Context, session *melody.Session, req *WSConnectReq) {
	logger := wstool.GetLogger(session).WithField("action", WSConnect)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.Lock()
	logger.Debugln("get global lock cost:", log.GetLogDuration(isDebug, s))
	defer t.Unlock()
	if t.conn != nil {
		wsErrorMsg(ctx, session, 0xffff, "duplicate connections", WSConnect, req.ReqID)
		return
	}
	s = log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	conn, err := wrapper.TaosConnect("", req.User, req.Password, req.DB, 0)
	logger.Debugln("connect cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if err != nil {
		wstool.WSError(ctx, session, err, WSConnect, req.ReqID)
		return
	}
	t.conn = conn
	wstool.WSWriteJson(session, &WSConnectResp{
		Action: WSConnect,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
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
	Timing        int64              `json:"timing"`
	ID            uint64             `json:"id"`
	IsUpdate      bool               `json:"is_update"`
	AffectedRows  int                `json:"affected_rows"`
	FieldsCount   int                `json:"fields_count"`
	FieldsNames   []string           `json:"fields_names"`
	FieldsTypes   jsontype.JsonUint8 `json:"fields_types"`
	FieldsLengths []int64            `json:"fields_lengths"`
	Precision     int                `json:"precision"`
}

func (t *Taos) query(ctx context.Context, session *melody.Session, req *WSQueryReq) {
	if t.conn == nil {
		wsErrorMsg(ctx, session, 0xffff, "server not connected", WSQuery, req.ReqID)
		return
	}
	sqlType := monitor.WSRecordRequest(req.SQL)
	logger := wstool.GetLogger(session).WithField("action", WSQuery)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	handler := async.GlobalAsync.HandlerPool.Get()
	logger.Debugln("get handler cost:", log.GetLogDuration(isDebug, s))
	defer async.GlobalAsync.HandlerPool.Put(handler)
	s = log.GetLogNow(isDebug)
	result, _ := async.GlobalAsync.TaosQuery(t.conn, req.SQL, handler, int64(req.ReqID))
	logger.Debugln("query cost ", log.GetLogDuration(isDebug, s))
	code := wrapper.TaosError(result.Res)
	if code != httperror.SUCCESS {
		monitor.WSRecordResult(sqlType, false)
		errStr := wrapper.TaosErrorStr(result.Res)
		s = log.GetLogNow(isDebug)
		thread.Lock()
		logger.Debugln("free result get lock cost:", log.GetLogDuration(isDebug, s))
		s = log.GetLogNow(isDebug)
		wrapper.TaosFreeResult(result.Res)
		logger.Debugln("free result cost:", log.GetLogDuration(isDebug, s))
		thread.Unlock()
		wsErrorMsg(ctx, session, code, errStr, WSQuery, req.ReqID)
		return
	}
	monitor.WSRecordResult(sqlType, true)
	s = log.GetLogNow(isDebug)
	isUpdate := wrapper.TaosIsUpdateQuery(result.Res)
	logger.Debugln("is_update_query cost:", log.GetLogDuration(isDebug, s))
	queryResult := &WSQueryResult{Action: WSQuery, ReqID: req.ReqID}
	if isUpdate {
		var affectRows int
		s = log.GetLogNow(isDebug)
		affectRows = wrapper.TaosAffectedRows(result.Res)
		logger.Debugln("affected_rows cost:", log.GetLogDuration(isDebug, s))
		queryResult.IsUpdate = true
		queryResult.AffectedRows = affectRows
		s = log.GetLogNow(isDebug)
		thread.Lock()
		logger.Debugln("free_result get lock cost:", log.GetLogDuration(isDebug, s))
		s = log.GetLogNow(isDebug)
		wrapper.TaosFreeResult(result.Res)
		logger.Debugln("free_result cost:", log.GetLogDuration(isDebug, s))
		thread.Unlock()
		queryResult.Timing = wstool.GetDuration(ctx)
		wstool.WSWriteJson(session, queryResult)
		return
	} else {
		s = log.GetLogNow(isDebug)
		fieldsCount := wrapper.TaosNumFields(result.Res)
		logger.Debugln("num_fields cost:", log.GetLogDuration(isDebug, s))
		queryResult.FieldsCount = fieldsCount
		s = log.GetLogNow(isDebug)
		rowsHeader, _ := wrapper.ReadColumn(result.Res, fieldsCount)
		logger.Debugln("read column cost:", log.GetLogDuration(isDebug, s))
		queryResult.FieldsNames = rowsHeader.ColNames
		queryResult.FieldsLengths = rowsHeader.ColLength
		queryResult.FieldsTypes = rowsHeader.ColTypes
		s = log.GetLogNow(isDebug)
		precision := wrapper.TaosResultPrecision(result.Res)
		logger.Debugln("result_precision cost:", log.GetLogDuration(isDebug, s))
		queryResult.Precision = precision
		result := &Result{
			TaosResult:  result.Res,
			FieldsCount: fieldsCount,
			Header:      rowsHeader,
			precision:   precision,
		}
		t.addResult(result)
		queryResult.ID = result.index
		queryResult.Timing = wstool.GetDuration(ctx)
		wstool.WSWriteJson(session, queryResult)
	}
}

type WSWriteMetaResp struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	Action    string `json:"action"`
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
	Timing    int64  `json:"timing"`
}

func (t *Taos) writeRaw(ctx context.Context, session *melody.Session, reqID, messageID uint64, length uint32, metaType uint16, data unsafe.Pointer) {
	logger := wstool.GetLogger(session).WithField("action", WSWriteRaw)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.Lock()
	logger.Debugln("get global lock cost:", log.GetLogDuration(isDebug, s))
	defer t.Unlock()
	if t.conn == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "server not connected", WSWriteRaw, reqID, &messageID)
		return
	}
	meta := wrapper.BuildRawMeta(length, metaType, data)
	s = log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	errCode := wrapper.TMQWriteRaw(t.conn, meta)
	thread.Unlock()
	logger.Debugln("write_raw_meta cost:", log.GetLogDuration(isDebug, s))
	if errCode != 0 {
		errStr := wrapper.TaosErrorStr(nil)
		wsErrorMsg(ctx, session, int(errCode)&0xffff, errStr, WSWriteRaw, reqID)
		return
	}
	resp := &WSWriteMetaResp{Action: WSWriteRaw, ReqID: reqID, MessageID: messageID, Timing: wstool.GetDuration(ctx)}
	wstool.WSWriteJson(session, resp)
}

type WSWriteRawBlockResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func (t *Taos) writeRawBlock(ctx context.Context, session *melody.Session, reqID uint64, numOfRows int, tableName string, rawBlock unsafe.Pointer) {
	logger := wstool.GetLogger(session).WithField("action", WSWriteRawBlock)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.Lock()
	logger.Debugln("get global lock cost:", log.GetLogDuration(isDebug, s))
	defer t.Unlock()
	if t.conn == nil {
		wsErrorMsg(ctx, session, 0xffff, "server not connected", WSWriteRawBlock, reqID)
		return
	}
	s = log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	errCode := wrapper.TaosWriteRawBlock(t.conn, numOfRows, rawBlock, tableName)
	thread.Unlock()
	logger.Debugln("write_raw_meta cost:", log.GetLogDuration(isDebug, s))
	if errCode != 0 {
		errStr := wrapper.TaosErrorStr(nil)
		wsErrorMsg(ctx, session, errCode&0xffff, errStr, WSWriteRawBlock, reqID)
		return
	}
	resp := &WSWriteRawBlockResp{Action: WSWriteRawBlock, ReqID: reqID, Timing: wstool.GetDuration(ctx)}
	wstool.WSWriteJson(session, resp)
}

type WSWriteRawBlockWithFieldsResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func (t *Taos) writeRawBlockWithFields(ctx context.Context, session *melody.Session, reqID uint64, numOfRows int, tableName string, rawBlock unsafe.Pointer, fields unsafe.Pointer, numFields int) {
	logger := wstool.GetLogger(session).WithField("action", WSWriteRawBlockWithFields)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.Lock()
	logger.Debugln("get global lock cost:", log.GetLogDuration(isDebug, s))
	defer t.Unlock()
	if t.conn == nil {
		wsErrorMsg(ctx, session, 0xffff, "server not connected", WSWriteRawBlockWithFields, reqID)
		return
	}
	s = log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	errCode := wrapper.TaosWriteRawBlockWithFields(t.conn, numOfRows, rawBlock, tableName, fields, numFields)
	thread.Unlock()
	logger.Debugln("write_raw_meta cost:", log.GetLogDuration(isDebug, s))
	if errCode != 0 {
		errStr := wrapper.TaosErrorStr(nil)
		wsErrorMsg(ctx, session, errCode&0xffff, errStr, WSWriteRawBlockWithFields, reqID)
		return
	}
	resp := &WSWriteRawBlockWithFieldsResp{Action: WSWriteRawBlockWithFields, ReqID: reqID, Timing: wstool.GetDuration(ctx)}
	wstool.WSWriteJson(session, resp)
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
	Timing    int64  `json:"timing"`
	ID        uint64 `json:"id"`
	Completed bool   `json:"completed"`
	Lengths   []int  `json:"lengths"`
	Rows      int    `json:"rows"`
}

func (t *Taos) fetch(ctx context.Context, session *melody.Session, req *WSFetchReq) {
	if t.conn == nil {
		wsErrorMsg(ctx, session, 0xffff, "server not connected", WSFetch, req.ReqID)
		return
	}
	logger := wstool.GetLogger(session).WithField("action", WSFetch)
	isDebug := log.IsDebug()
	resultItem := t.getResult(req.ID)
	if resultItem == nil {
		wsErrorMsg(ctx, session, 0xffff, "result is nil", WSFetch, req.ReqID)
		return
	}
	resultS := resultItem.Value.(*Result)
	s := log.GetLogNow(isDebug)
	handler := async.GlobalAsync.HandlerPool.Get()
	logger.Debugln("get handler cost:", log.GetLogDuration(isDebug, s))
	defer async.GlobalAsync.HandlerPool.Put(handler)
	s = log.GetLogNow(isDebug)
	result, _ := async.GlobalAsync.TaosFetchRawBlockA(resultS.TaosResult, handler)
	logger.Debugln("fetch_raw_block_a cost:", log.GetLogDuration(isDebug, s))
	if result.N == 0 {
		t.FreeResult(resultItem)
		wstool.WSWriteJson(session, &WSFetchResp{
			Action:    WSFetch,
			ReqID:     req.ReqID,
			Timing:    wstool.GetDuration(ctx),
			ID:        req.ID,
			Completed: true,
		})
		return
	}
	if result.N < 0 {
		errStr := wrapper.TaosErrorStr(result.Res)
		t.FreeResult(resultItem)
		wsErrorMsg(ctx, session, result.N&0xffff, errStr, WSFetch, req.ReqID)
		return
	}
	s = log.GetLogNow(isDebug)
	resultS.Lengths = wrapper.FetchLengths(resultS.TaosResult, resultS.FieldsCount)
	logger.Debugln("fetch_lengths cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	block := wrapper.TaosGetRawBlock(resultS.TaosResult)
	logger.Debugln("get_raw_block cost:", log.GetLogDuration(isDebug, s))
	resultS.Block = block
	resultS.Size = result.N

	wstool.WSWriteJson(session, &WSFetchResp{
		Action:  WSFetch,
		ReqID:   req.ReqID,
		Timing:  wstool.GetDuration(ctx),
		ID:      req.ID,
		Lengths: resultS.Lengths,
		Rows:    result.N,
	})
}

type WSFetchBlockReq struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

func (t *Taos) fetchBlock(ctx context.Context, session *melody.Session, req *WSFetchBlockReq) {
	if t.conn == nil {
		wsErrorMsg(ctx, session, 0xffff, "server not connected", WSFetchBlock, req.ReqID)
		return
	}
	logger := wstool.GetLogger(session).WithField("action", WSFetchBlock)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	resultItem := t.getResult(req.ID)
	if resultItem == nil {
		wsErrorMsg(ctx, session, 0xffff, "result is nil", WSFetchBlock, req.ReqID)
		return
	}
	resultS := resultItem.Value.(*Result)
	if resultS.Block == nil {
		wsErrorMsg(ctx, session, 0xffff, "block is nil", WSFetchBlock, req.ReqID)
		return
	}
	resultS.Lock()
	blockLength := int(parser.RawBlockGetLength(resultS.Block))
	if resultS.buffer == nil {
		resultS.buffer = new(bytes.Buffer)
	} else {
		resultS.buffer.Reset()
	}
	resultS.buffer.Grow(blockLength + 16)
	wstool.WriteUint64(resultS.buffer, uint64(wstool.GetDuration(ctx)))
	wstool.WriteUint64(resultS.buffer, req.ID)
	for offset := 0; offset < blockLength; offset++ {
		resultS.buffer.WriteByte(*((*byte)(unsafe.Pointer(uintptr(resultS.Block) + uintptr(offset)))))
	}
	b := resultS.buffer.Bytes()
	resultS.Unlock()
	logger.Debugln("handle binary content cost:", log.GetLogDuration(isDebug, s))
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

type WSErrorResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func wsErrorMsg(ctx context.Context, session *melody.Session, code int, message string, action string, reqID uint64) {
	b, _ := json.Marshal(&WSErrorResp{
		Code:    code & 0xffff,
		Message: message,
		Action:  action,
		ReqID:   reqID,
		Timing:  wstool.GetDuration(ctx),
	})
	session.Write(b)
}

type WSTMQErrorResp struct {
	Code      int     `json:"code"`
	Message   string  `json:"message"`
	Action    string  `json:"action"`
	ReqID     uint64  `json:"req_id"`
	Timing    int64   `json:"timing"`
	MessageID *uint64 `json:"message_id,omitempty"`
}

func wsTMQErrorMsg(ctx context.Context, session *melody.Session, code int, message string, action string, reqID uint64, messageID *uint64) {
	b, _ := json.Marshal(&WSTMQErrorResp{
		Code:      code & 0xffff,
		Message:   message,
		Action:    action,
		ReqID:     reqID,
		Timing:    wstool.GetDuration(ctx),
		MessageID: messageID,
	})
	session.Write(b)
}

func init() {
	c := NewQueryController()
	controller.AddController(c)
}

package ws

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
	"unsafe"

	"github.com/huskar-t/melody"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/parser"
	stmtCommon "github.com/taosdata/driver-go/v3/common/stmt"
	errors2 "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/types"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/controller/ws/stmt"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/thread"
	"github.com/taosdata/taosadapter/v3/tools"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
	"github.com/taosdata/taosadapter/v3/tools/jsontype"
	"github.com/taosdata/taosadapter/v3/version"
)

type messageHandler struct {
	conn           unsafe.Pointer
	closed         bool
	once           sync.Once
	wait           sync.WaitGroup
	dropUserNotify chan struct{}
	sync.RWMutex

	queryResults *QueryResultHolder // ws query
	stmts        *StmtHolder        // stmt bind message

	exit                chan struct{}
	whitelistChangeChan chan int64
	session             *melody.Session
	ip                  net.IP
	ipStr               string
}

func newHandler(session *melody.Session) *messageHandler {
	ipAddr := iptool.GetRealIP(session.Request)
	return &messageHandler{
		queryResults:        NewQueryResultHolder(),
		stmts:               NewStmtHolder(),
		exit:                make(chan struct{}),
		whitelistChangeChan: make(chan int64, 1),
		dropUserNotify:      make(chan struct{}, 1),
		session:             session,
		ip:                  ipAddr,
		ipStr:               ipAddr.String(),
	}
}

func (h *messageHandler) waitSignal() {
	for {
		if h.closed {
			return
		}
		select {
		case <-h.dropUserNotify:
			h.Lock()
			if h.closed {
				h.Unlock()
				return
			}
			wstool.GetLogger(h.session).WithField("clientIP", h.ipStr).Info("user dropped! close connection!")
			h.session.Close()
			h.Unlock()
			h.Close()
			return
		case <-h.whitelistChangeChan:
			h.Lock()
			if h.closed {
				h.Unlock()
				return
			}
			whitelist, err := tool.GetWhitelist(h.conn)
			if err != nil {
				wstool.GetLogger(h.session).WithField("clientIP", h.ipStr).WithError(err).Errorln("get whitelist error! close connection!")
				h.session.Close()
				h.Unlock()
				h.Close()
				return
			}
			valid := tool.CheckWhitelist(whitelist, h.ip)
			if !valid {
				wstool.GetLogger(h.session).WithField("clientIP", h.ipStr).Errorln("ip not in whitelist! close connection!")
				h.session.Close()
				h.Unlock()
				h.Close()
				return
			}
			h.Unlock()
		case <-h.exit:
			return
		}
	}
}

func (h *messageHandler) Close() {
	h.Lock()
	defer h.Unlock()

	if h.closed {
		return
	}
	h.closed = true
	close(h.exit)
	close(h.whitelistChangeChan)
	close(h.dropUserNotify)
	h.stop()
}

type Request struct {
	ReqID  uint64          `json:"req_id"`
	Action string          `json:"action"`
	Args   json.RawMessage `json:"args"`
}

func (h *messageHandler) handleMessage(session *melody.Session, data []byte) {
	ctx := context.WithValue(context.Background(), wstool.StartTimeKey, time.Now().UnixNano())
	logger.Debugln("get ws message data:", string(data))

	var request Request
	if err := json.Unmarshal(data, &request); err != nil {
		logger.WithError(err).Errorln("unmarshal ws request")
		return
	}

	var f dealFunc
	switch request.Action {
	case wstool.ClientVersion:
		f = h.handleVersion
	case Connect:
		f = h.handleConnect
	case WSQuery:
		f = h.handleQuery
	case WSFetch:
		f = h.handleFetch
	case WSFetchBlock:
		f = h.handleFetchBlock
	case WSFreeResult:
		f = h.handleFreeResult
	case SchemalessWrite:
		f = h.handleSchemalessWrite
	case STMTInit:
		f = h.handleStmtInit
	case STMTPrepare:
		f = h.handleStmtPrepare
	case STMTSetTableName:
		f = h.handleStmtSetTableName
	case STMTSetTags:
		f = h.handleStmtSetTags
	case STMTBind:
		f = h.handleStmtBind
	case STMTAddBatch:
		f = h.handleStmtAddBatch
	case STMTExec:
		f = h.handleStmtExec
	case STMTClose:
		f = h.handleStmtClose
	case STMTGetColFields:
		f = h.handleStmtGetColFields
	case STMTGetTagFields:
		f = h.handleStmtGetTagFields
	case STMTUseResult:
		f = h.handleStmtUseResult
	case STMTNumParams:
		f = h.handleStmtNumParams
	case STMTGetParam:
		f = h.handleStmtGetParam
	case WSNumFields:
		f = h.handleNumFields
	case WSGetCurrentDB:
		f = h.handleGetCurrentDB
	case WSGetServerInfo:
		f = h.handleGetServerInfo
	default:
		f = h.handleDefault
	}
	h.deal(ctx, session, request, f)
}

func (h *messageHandler) handleMessageBinary(session *melody.Session, bytes []byte) {
	//p0 uin64  req_id
	//p0+8 uint64  message_id
	//p0+16 uint64 (1 (set tag) 2 (bind))
	p0 := unsafe.Pointer(&bytes[0])
	reqID := *(*uint64)(p0)
	messageID := *(*uint64)(tools.AddPointer(p0, uintptr(8)))
	action := *(*uint64)(tools.AddPointer(p0, uintptr(16)))
	logger.Debugln("get ws message binary reqID:", reqID, "messageID:", messageID, "action:", action)

	ctx := context.WithValue(context.Background(), wstool.StartTimeKey, time.Now().UnixNano())
	mt := messageType(action)

	var f dealBinaryFunc
	switch mt {
	case SetTagsMessage:
		f = h.handleSetTagsMessage
	case BindMessage:
		f = h.handleBindMessage
	case TMQRawMessage:
		f = h.handleTMQRawMessage
	case RawBlockMessage:
		f = h.handleRawBlockMessage
	case RawBlockMessageWithFields:
		f = h.handleRawBlockMessageWithFields
	default:
		f = h.handleDefaultBinary
	}
	h.dealBinary(ctx, session, mt, reqID, messageID, p0, f)
}

type RequestID struct {
	ReqID uint64 `json:"req_id"`
}

type dealFunc func(context.Context, Request, *logrus.Entry, bool, time.Time) Response

type dealBinaryRequest struct {
	action messageType
	reqID  uint64
	id     uint64 // messageID or stmtID
	p0     unsafe.Pointer
}
type dealBinaryFunc func(context.Context, dealBinaryRequest, *logrus.Entry, bool, time.Time) Response

func (h *messageHandler) deal(ctx context.Context, session *melody.Session, request Request, f dealFunc) {
	h.wait.Add(1)
	go func() {
		defer h.wait.Done()

		if h.conn == nil && request.Action != Connect && request.Action != wstool.ClientVersion {
			resp := wsCommonErrorMsg(0xffff, "server not connected")
			h.writeResponse(ctx, session, resp, request.Action, request.ReqID)
			return
		}

		reqID := request.ReqID
		if reqID == 0 {
			var req RequestID
			_ = json.Unmarshal(request.Args, &req)
			reqID = req.ReqID
		}
		request.ReqID = reqID

		logger := logger.WithField(actionKey, request.Action).WithField("req_id", reqID)
		isDebug := log.IsDebug()
		s := log.GetLogNow(isDebug)

		resp := f(ctx, request, logger, isDebug, s)
		h.writeResponse(ctx, session, resp, request.Action, reqID)
	}()
}

func (h *messageHandler) dealBinary(ctx context.Context, session *melody.Session, action messageType, reqID uint64, messageID uint64, p0 unsafe.Pointer, f dealBinaryFunc) {
	h.wait.Add(1)
	go func() {
		defer h.wait.Done()

		if h.conn == nil {
			resp := wsCommonErrorMsg(0xffff, "server not connected")
			h.writeResponse(ctx, session, resp, action.String(), reqID)
			return
		}

		logger := logger.WithField(actionKey, action.String()).WithField("req_id", reqID)
		isDebug := log.IsDebug()
		s := log.GetLogNow(isDebug)

		req := dealBinaryRequest{
			action: action,
			reqID:  reqID,
			id:     messageID,
			p0:     p0,
		}
		resp := f(ctx, req, logger, isDebug, s)
		h.writeResponse(ctx, session, resp, action.String(), reqID)
	}()
}

type BaseResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	binary  bool
	null    bool
}

func (h *messageHandler) writeResponse(ctx context.Context, session *melody.Session, response Response, action string, reqID uint64) {
	if response == nil {
		// session closed handle return nil
		return
	}
	if response.IsNull() {
		return
	}
	if response.IsBinary() {
		_ = session.WriteBinary(response.(*BinaryResponse).Data)
		return
	}
	response.SetAction(action)
	response.SetReqID(reqID)
	response.SetTiming(wstool.GetDuration(ctx))

	respByte, _ := json.Marshal(response)
	_ = session.Write(respByte)
}

func (h *messageHandler) stop() {
	h.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		waitCh := make(chan struct{}, 1)
		go func() {
			h.wait.Wait()
			close(waitCh)
		}()

		select {
		case <-ctx.Done():
		case <-waitCh:
		}
		// clean query result and stmt
		h.queryResults.FreeAll()
		h.stmts.FreeAll()
		// clean connection
		if h.conn != nil {
			thread.Lock()
			wrapper.TaosClose(h.conn)
			thread.Unlock()
		}
	})
}

func (h *messageHandler) handleDefault(_ context.Context, request Request, _ *logrus.Entry, _ bool, _ time.Time) (resp Response) {
	return wsCommonErrorMsg(0xffff, fmt.Sprintf("unknown action %s", request.Action))
}

func (h *messageHandler) handleDefaultBinary(_ context.Context, req dealBinaryRequest, _ *logrus.Entry, _ bool, _ time.Time) (resp Response) {
	return wsCommonErrorMsg(0xffff, fmt.Sprintf("unknown action %v", req.action))
}

func (h *messageHandler) handleVersion(_ context.Context, _ Request, _ *logrus.Entry, _ bool, _ time.Time) (resp Response) {
	return &VersionResponse{Version: version.TaosClientVersion}
}

type ConnRequest struct {
	ReqID    uint64 `json:"req_id"`
	User     string `json:"user"`
	Password string `json:"password"`
	DB       string `json:"db"`
	Mode     *int   `json:"mode"`
}

func (h *messageHandler) handleConnect(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	var req ConnRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("## unmarshal connect request %s error: %s", string(request.Args), err)
		return wsCommonErrorMsg(0xffff, "unmarshal connect request error")
	}

	h.Lock()
	defer h.Unlock()
	if h.closed {
		return
	}
	if h.conn != nil {
		return wsCommonErrorMsg(0xffff, "duplicate connections")
	}

	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	conn, err := wrapper.TaosConnect("", req.User, req.Password, req.DB, 0)
	logger.Debugln("connect cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()

	if err != nil {
		var taosErr *errors2.TaosError
		errors.As(err, &taosErr)
		return wsCommonErrorMsg(int(taosErr.Code), taosErr.ErrStr)
	}
	whitelist, err := tool.GetWhitelist(conn)
	if err != nil {
		thread.Lock()
		wrapper.TaosClose(conn)
		thread.Unlock()
		var taosErr *errors2.TaosError
		errors.As(err, &taosErr)
		return wsCommonErrorMsg(int(taosErr.Code), taosErr.ErrStr)
	}
	valid := tool.CheckWhitelist(whitelist, h.ip)
	if !valid {
		thread.Lock()
		wrapper.TaosClose(conn)
		thread.Unlock()
		return wsCommonErrorMsg(0xffff, "whitelist prohibits current IP access")
	}
	err = tool.RegisterChangeWhitelist(conn, h.whitelistChangeChan)
	if err != nil {
		thread.Lock()
		wrapper.TaosClose(conn)
		thread.Unlock()
		var taosErr *errors2.TaosError
		errors.As(err, &taosErr)
		return wsCommonErrorMsg(int(taosErr.Code), taosErr.ErrStr)
	}
	err = tool.RegisterDropUser(conn, h.dropUserNotify)
	if err != nil {
		thread.Lock()
		wrapper.TaosClose(conn)
		thread.Unlock()
		var taosErr *errors2.TaosError
		errors.As(err, &taosErr)
		return wsCommonErrorMsg(int(taosErr.Code), taosErr.ErrStr)
	}
	if req.Mode != nil {
		switch *req.Mode {
		case common.TAOS_CONN_MODE_BI:
			// BI mode
			code := wrapper.TaosSetConnMode(conn, common.TAOS_CONN_MODE_BI, 1)
			if code != 0 {
				thread.Lock()
				wrapper.TaosClose(conn)
				thread.Unlock()
				return wsCommonErrorMsg(code, wrapper.TaosErrorStr(nil))
			}
		default:
			thread.Lock()
			wrapper.TaosClose(conn)
			thread.Unlock()
			return wsCommonErrorMsg(0xffff, fmt.Sprintf("unexpected mode: %d", req.Mode))
		}
	}
	h.conn = conn
	go h.waitSignal()
	return &BaseResponse{}
}

type QueryRequest struct {
	ReqID uint64 `json:"req_id"`
	Sql   string `json:"sql"`
}

type QueryResponse struct {
	BaseResponse
	ID            uint64             `json:"id"`
	IsUpdate      bool               `json:"is_update"`
	AffectedRows  int                `json:"affected_rows"`
	FieldsCount   int                `json:"fields_count"`
	FieldsNames   []string           `json:"fields_names"`
	FieldsTypes   jsontype.JsonUint8 `json:"fields_types"`
	FieldsLengths []int64            `json:"fields_lengths"`
	Precision     int                `json:"precision"`
}

func (h *messageHandler) handleQuery(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	var req QueryRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("## unmarshal ws query request %s error: %s", request.Args, err)
		return wsCommonErrorMsg(0xffff, "unmarshal ws query request error")
	}
	sqlType := monitor.WSRecordRequest(req.Sql)
	handler := async.GlobalAsync.HandlerPool.Get()
	defer async.GlobalAsync.HandlerPool.Put(handler)
	logger.Debugln("get handler cost:", log.GetLogDuration(isDebug, s))
	result, _ := async.GlobalAsync.TaosQuery(h.conn, req.Sql, handler, int64(request.ReqID))
	logger.Debugln("query cost ", log.GetLogDuration(isDebug, s))

	code := wrapper.TaosError(result.Res)
	if code != httperror.SUCCESS {
		monitor.WSRecordResult(sqlType, false)
		errStr := wrapper.TaosErrorStr(result.Res)
		freeCPointer(result.Res)
		return wsCommonErrorMsg(code, errStr)
	}
	monitor.WSRecordResult(sqlType, true)
	isUpdate := wrapper.TaosIsUpdateQuery(result.Res)
	logger.Debugln("is_update_query cost:", log.GetLogDuration(isDebug, s))
	if isUpdate {
		affectRows := wrapper.TaosAffectedRows(result.Res)
		logger.Debugln("affected_rows cost:", log.GetLogDuration(isDebug, s))
		freeCPointer(result.Res)
		return &QueryResponse{IsUpdate: true, AffectedRows: affectRows}
	}
	fieldsCount := wrapper.TaosNumFields(result.Res)
	logger.Debugln("num_fields cost:", log.GetLogDuration(isDebug, s))
	rowsHeader, _ := wrapper.ReadColumn(result.Res, fieldsCount)
	logger.Debugln("read column cost:", log.GetLogDuration(isDebug, s))
	precision := wrapper.TaosResultPrecision(result.Res)
	logger.Debugln("result_precision cost:", log.GetLogDuration(isDebug, s))
	queryResult := QueryResult{TaosResult: result.Res, FieldsCount: fieldsCount, Header: rowsHeader, precision: precision}
	idx := h.queryResults.Add(&queryResult)

	return &QueryResponse{
		ID:            idx,
		FieldsCount:   fieldsCount,
		FieldsNames:   rowsHeader.ColNames,
		FieldsLengths: rowsHeader.ColLength,
		FieldsTypes:   rowsHeader.ColTypes,
		Precision:     precision,
	}
}

type FetchRequest struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

type FetchResponse struct {
	BaseResponse
	ID        uint64 `json:"id"`
	Completed bool   `json:"completed"`
	Lengths   []int  `json:"lengths"`
	Rows      int    `json:"rows"`
}

func (h *messageHandler) handleFetch(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	var req FetchRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("## unmarshal ws fetch request %s error: %s", request.Args, err)
		return wsCommonErrorMsg(0xffff, "unmarshal ws fetch request error")
	}

	item := h.queryResults.Get(req.ID)
	if item == nil {
		return wsCommonErrorMsg(0xffff, "result is nil")
	}
	item.Lock()
	if item.TaosResult == nil {
		item.Unlock()
		return wsCommonErrorMsg(0xffff, "result has been freed")
	}
	handler := async.GlobalAsync.HandlerPool.Get()
	defer async.GlobalAsync.HandlerPool.Put(handler)
	logger.Debugln("get handler cost:", log.GetLogDuration(isDebug, s))
	result, _ := async.GlobalAsync.TaosFetchRawBlockA(item.TaosResult, handler)
	logger.Debugln("fetch_raw_block_a cost:", log.GetLogDuration(isDebug, s))
	if result.N == 0 {
		item.Unlock()
		h.queryResults.FreeResultByID(req.ID)
		return &FetchResponse{ID: req.ID, Completed: true}
	}
	if result.N < 0 {
		item.Unlock()
		errStr := wrapper.TaosErrorStr(result.Res)
		h.queryResults.FreeResultByID(req.ID)
		return wsCommonErrorMsg(0xffff, errStr)
	}
	length := wrapper.FetchLengths(item.TaosResult, item.FieldsCount)
	logger.Debugln("fetch_lengths cost:", log.GetLogDuration(isDebug, s))
	item.Block = wrapper.TaosGetRawBlock(item.TaosResult)
	logger.Debugln("get_raw_block cost:", log.GetLogDuration(isDebug, s))
	item.Size = result.N
	item.Unlock()
	return &FetchResponse{ID: req.ID, Lengths: length, Rows: result.N}
}

type FetchBlockRequest struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

func (h *messageHandler) handleFetchBlock(ctx context.Context, request Request, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	var req FetchBlockRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("## unmarshal ws fetch block request %s error: %s", request.Args, err)
		return wsCommonErrorMsg(0xffff, "unmarshal ws fetch block request error")
	}

	item := h.queryResults.Get(req.ID)
	if item == nil {
		return wsCommonErrorMsg(0xffff, "result is nil")
	}
	item.Lock()
	defer item.Unlock()
	if item.TaosResult == nil {
		return wsCommonErrorMsg(0xffff, "result has been freed")
	}
	if item.Block == nil {
		return wsCommonErrorMsg(0xffff, "block is nil")
	}

	blockLength := int(parser.RawBlockGetLength(item.Block))
	if item.buffer == nil {
		item.buffer = new(bytes.Buffer)
	} else {
		item.buffer.Reset()
	}
	item.buffer.Grow(blockLength + 16)
	wstool.WriteUint64(item.buffer, uint64(wstool.GetDuration(ctx)))
	wstool.WriteUint64(item.buffer, req.ID)
	for offset := 0; offset < blockLength; offset++ {
		item.buffer.WriteByte(*((*byte)(tools.AddPointer(item.Block, uintptr(offset)))))
	}
	b := item.buffer.Bytes()
	logger.Debugln("handle binary content cost:", log.GetLogDuration(isDebug, s))
	resp = &BinaryResponse{Data: b}
	resp.SetBinary(true)
	return resp
}

type FreeResultRequest struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

func (h *messageHandler) handleFreeResult(_ context.Context, request Request, logger *logrus.Entry, _ bool, _ time.Time) (resp Response) {
	var req FreeResultRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("## unmarshal ws fetch request %s error: %s", request.Args, err)
		return wsCommonErrorMsg(0xffff, "unmarshal connect request error")
	}

	h.queryResults.FreeResultByID(req.ID)
	resp = &BaseResponse{}
	resp.SetNull(true)
	return resp
}

type SchemalessWriteRequest struct {
	ReqID     uint64 `json:"req_id"`
	Protocol  int    `json:"protocol"`
	Precision string `json:"precision"`
	TTL       int    `json:"ttl"`
	Data      string `json:"data"`
}

func (h *messageHandler) handleSchemalessWrite(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	var req SchemalessWriteRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("## unmarshal schemaless write request %s error: %s", request.Args, err)
		return wsCommonErrorMsg(0xffff, "unmarshal schemaless write request error")
	}

	if req.Protocol == 0 {
		logger.Errorf("## schemaless write request %s args error. protocol is null", request.Args)
		return wsCommonErrorMsg(0xffff, "args error")
	}

	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	_, result := wrapper.TaosSchemalessInsertRawTTLWithReqID(h.conn, req.Data, req.Protocol, req.Precision, req.TTL, int64(request.ReqID))
	logger.Debugln("taos_schemaless_insert_raw_ttl_with_reqid cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	defer freeCPointer(result)

	if code := wrapper.TaosError(result); code != 0 {
		return wsCommonErrorMsg(code, wrapper.TaosErrorStr(result))
	}
	return &BaseResponse{}
}

type StmtInitResponse struct {
	BaseResponse
	StmtID uint64 `json:"stmt_id"`
}

func (h *messageHandler) handleStmtInit(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	stmtInit := wrapper.TaosStmtInitWithReqID(h.conn, int64(request.ReqID))
	logger.Debugln("stmt_init cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if stmtInit == nil {
		errStr := wrapper.TaosStmtErrStr(stmtInit)
		logger.Errorf("## stmt init error: %s", errStr)
		return wsCommonErrorMsg(0xffff, errStr)
	}
	stmtItem := &StmtItem{stmt: stmtInit}
	h.stmts.Add(stmtItem)
	return &StmtInitResponse{StmtID: stmtItem.index}
}

type StmtPrepareRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
	SQL    string `json:"sql"`
}

type StmtPrepareResponse struct {
	BaseResponse
	StmtID   uint64 `json:"stmt_id"`
	IsInsert bool   `json:"is_insert"`
}

func (h *messageHandler) handleStmtPrepare(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	var req StmtPrepareRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("## unmarshal stmt prepare request %s error: %s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal connect request error", req.StmtID)
	}

	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	code := wrapper.TaosStmtPrepare(stmtItem.stmt, req.SQL)
	logger.Debugln("stmt_prepare cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("## stmt prepare error: %s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	thread.Lock()
	isInsert, code := wrapper.TaosStmtIsInsert(stmtItem.stmt)
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("## check stmt is insert error: %s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	stmtItem.isInsert = isInsert
	return &StmtPrepareResponse{StmtID: req.StmtID, IsInsert: isInsert}
}

type StmtSetTableNameRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
	Name   string `json:"name"`
}

type StmtSetTableNameResponse struct {
	BaseResponse
	StmtID uint64 `json:"stmt_id"`
}

func (h *messageHandler) handleStmtSetTableName(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	var req StmtSetTableNameRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("## unmarshal stmt set table name request %s error: %s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt set table name request error", req.StmtID)
	}

	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	code := wrapper.TaosStmtSetTBName(stmtItem.stmt, req.Name)
	logger.Debugln("stmt_set_tbname cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("## stmt set table name error: %s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	return &StmtSetTableNameResponse{StmtID: req.StmtID}
}

type StmtSetTagsRequest struct {
	ReqID  uint64          `json:"req_id"`
	StmtID uint64          `json:"stmt_id"`
	Tags   json.RawMessage `json:"tags"`
}

type StmtSetTagsResponse struct {
	BaseResponse
	StmtID uint64 `json:"stmt_id"`
}

func (h *messageHandler) handleStmtSetTags(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	var req StmtSetTagsRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("## unmarshal stmt set tags request %s error: %s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt set tags request error", req.StmtID)
	}

	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	thread.Lock()
	logger.Debugln("stmt_get_tag_fields get thread lock cost:", log.GetLogDuration(isDebug, s))
	code, tagNums, tagFields := wrapper.TaosStmtGetTagFields(stmtItem.stmt)
	logger.Debugln("stmt_get_tag_fields cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("## stmt get tag fields error: %s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	defer func() {
		wrapper.TaosStmtReclaimFields(stmtItem.stmt, tagFields)
	}()
	if tagNums == 0 {
		return &StmtSetTagsResponse{StmtID: req.StmtID}
	}
	fields := wrapper.StmtParseFields(tagNums, tagFields)
	logger.Debugln("stmt parse fields cost:", log.GetLogDuration(isDebug, s))
	tags := make([][]driver.Value, tagNums)
	for i := 0; i < tagNums; i++ {
		tags[i] = []driver.Value{req.Tags[i]}
	}
	data, err := stmt.StmtParseTag(req.Tags, fields)
	logger.Debugln("stmt parse tag json cost:", log.GetLogDuration(isDebug, s))
	if err != nil {
		return wsStmtErrorMsg(0xffff, fmt.Sprintf("stmt parse tag json:%s", err.Error()), req.StmtID)
	}
	thread.Lock()
	logger.Debugln("stmt_set_tags get thread lock cost:", log.GetLogDuration(isDebug, s))
	code = wrapper.TaosStmtSetTags(stmtItem.stmt, data)
	logger.Debugln("stmt_set_tags cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	return &StmtSetTagsResponse{StmtID: req.StmtID}
}

type StmtBindRequest struct {
	ReqID   uint64          `json:"req_id"`
	StmtID  uint64          `json:"stmt_id"`
	Columns json.RawMessage `json:"columns"`
}

type StmtBindResponse struct {
	BaseResponse
	StmtID uint64 `json:"stmt_id"`
}

func (h *messageHandler) handleStmtBind(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	var req StmtBindRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("## unmarshal stmt bind tag request %s error: %s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt bind request error", req.StmtID)
	}

	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	thread.Lock()
	logger.Debugln("stmt_get_col_fields get thread lock cost:", log.GetLogDuration(isDebug, s))
	code, colNums, colFields := wrapper.TaosStmtGetColFields(stmtItem.stmt)
	logger.Debugln("stmt_get_col_fields cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("## stmt get col fields error: %s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	defer func() {
		wrapper.TaosStmtReclaimFields(stmtItem.stmt, colFields)
	}()
	if colNums == 0 {
		return &StmtBindResponse{StmtID: req.StmtID}
	}
	fields := wrapper.StmtParseFields(colNums, colFields)
	logger.Debugln("stmt parse fields cost:", log.GetLogDuration(isDebug, s))
	fieldTypes := make([]*types.ColumnType, colNums)

	var err error
	for i := 0; i < colNums; i++ {
		if fieldTypes[i], err = fields[i].GetType(); err != nil {
			return wsStmtErrorMsg(0xffff, fmt.Sprintf("stmt get column type error:%s", err.Error()), req.StmtID)
		}
	}
	data, err := stmt.StmtParseColumn(req.Columns, fields, fieldTypes)
	logger.Debugln("stmt parse column json cost:", log.GetLogDuration(isDebug, s))
	if err != nil {
		return wsStmtErrorMsg(0xffff, fmt.Sprintf("stmt parse column json:%s", err.Error()), req.StmtID)
	}
	thread.Lock()
	logger.Debugln("stmt_bind_param_batch get thread lock cost:", log.GetLogDuration(isDebug, s))
	wrapper.TaosStmtBindParamBatch(stmtItem.stmt, data, fieldTypes)
	logger.Debugln("stmt_bind_param_batch cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	return &StmtBindResponse{StmtID: req.StmtID}
}

func (h *messageHandler) handleBindMessage(_ context.Context, req dealBinaryRequest, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	block := tools.AddPointer(req.p0, uintptr(24))
	columns := parser.RawBlockGetNumOfCols(block)
	rows := parser.RawBlockGetNumOfRows(block)

	stmtItem := h.stmts.Get(req.id)
	if stmtItem == nil {
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.id)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.id)
	}
	var data [][]driver.Value
	var fieldTypes []*types.ColumnType
	if stmtItem.isInsert {
		thread.Lock()
		logger.Debugln("stmt_get_col_fields get thread lock cost:", log.GetLogDuration(isDebug, s))
		code, colNums, colFields := wrapper.TaosStmtGetColFields(stmtItem.stmt)
		logger.Debugln("stmt_get_col_fields cost:", log.GetLogDuration(isDebug, s))
		thread.Unlock()
		if code != httperror.SUCCESS {
			errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
			return wsStmtErrorMsg(code, errStr, req.id)
		}
		defer func() {
			wrapper.TaosStmtReclaimFields(stmtItem.stmt, colFields)
		}()
		if colNums == 0 {
			return &StmtBindResponse{StmtID: req.id}
		}
		fields := wrapper.StmtParseFields(colNums, colFields)
		logger.Debugln("stmt parse fields cost:", log.GetLogDuration(isDebug, s))
		fieldTypes = make([]*types.ColumnType, colNums)
		var err error
		for i := 0; i < colNums; i++ {
			fieldTypes[i], err = fields[i].GetType()
			if err != nil {
				return wsStmtErrorMsg(0xffff, fmt.Sprintf("stmt get column type error:%s", err.Error()), req.id)
			}
		}
		if int(columns) != colNums {
			return wsStmtErrorMsg(0xffff, "stmt column count not match", req.id)
		}
		data = stmt.BlockConvert(block, int(rows), fields, fieldTypes)
		logger.Debugln("block convert cost:", log.GetLogDuration(isDebug, s))
	} else {
		var fields []*stmtCommon.StmtField
		var err error
		fields, fieldTypes, err = parseRowBlockInfo(block, int(columns))
		if err != nil {
			return wsStmtErrorMsg(0xffff, fmt.Sprintf("parse row block info error:%s", err.Error()), req.id)
		}
		data = stmt.BlockConvert(block, int(rows), fields, fieldTypes)
	}

	thread.Lock()
	logger.Debugln("stmt_bind_param_batch get thread lock cost:", log.GetLogDuration(isDebug, s))
	code := wrapper.TaosStmtBindParamBatch(stmtItem.stmt, data, fieldTypes)
	logger.Debugln("stmt_bind_param_batch cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("## stmt bind param error: %s", errStr)
		return wsStmtErrorMsg(code, errStr, req.id)
	}

	return &StmtBindResponse{StmtID: req.id}
}

func parseRowBlockInfo(block unsafe.Pointer, columns int) (fields []*stmtCommon.StmtField, fieldTypes []*types.ColumnType, err error) {
	infos := make([]parser.RawBlockColInfo, columns)
	parser.RawBlockGetColInfo(block, infos)

	fields = make([]*stmtCommon.StmtField, len(infos))
	fieldTypes = make([]*types.ColumnType, len(infos))

	for i, info := range infos {
		switch info.ColType {
		case common.TSDB_DATA_TYPE_BOOL:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_BOOL}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosBoolType}
		case common.TSDB_DATA_TYPE_TINYINT:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_TINYINT}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosTinyintType}
		case common.TSDB_DATA_TYPE_SMALLINT:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_SMALLINT}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosSmallintType}
		case common.TSDB_DATA_TYPE_INT:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_INT}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosIntType}
		case common.TSDB_DATA_TYPE_BIGINT:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_BIGINT}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosBigintType}
		case common.TSDB_DATA_TYPE_FLOAT:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_FLOAT}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosFloatType}
		case common.TSDB_DATA_TYPE_DOUBLE:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_DOUBLE}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosDoubleType}
		case common.TSDB_DATA_TYPE_BINARY:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_BINARY}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosBinaryType}
		//case common.TSDB_DATA_TYPE_TIMESTAMP: // todo precision
		//	fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_TIMESTAMP}
		//	fieldTypes[i] = &types.ColumnType{Type: types.TaosTimestampType}
		case common.TSDB_DATA_TYPE_NCHAR:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_NCHAR}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosNcharType}
		case common.TSDB_DATA_TYPE_UTINYINT:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_UTINYINT}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosUTinyintType}
		case common.TSDB_DATA_TYPE_USMALLINT:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_USMALLINT}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosUSmallintType}
		case common.TSDB_DATA_TYPE_UINT:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_UINT}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosUIntType}
		case common.TSDB_DATA_TYPE_UBIGINT:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_UBIGINT}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosUBigintType}
		case common.TSDB_DATA_TYPE_JSON:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_JSON}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosJsonType}
		case common.TSDB_DATA_TYPE_VARBINARY:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_VARBINARY}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosBinaryType}
		default:
			err = fmt.Errorf("unsupported data type %d", info.ColType)
		}
	}

	return
}

type StmtAddBatchRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type StmtAddBatchResponse struct {
	BaseResponse
	StmtID uint64 `json:"stmt_id"`
}

func (h *messageHandler) handleStmtAddBatch(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	var req StmtAddBatchRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("## unmarshal stmt add batch request %s error: %s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt add batch request error", req.StmtID)
	}

	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	code := wrapper.TaosStmtAddBatch(stmtItem.stmt)
	logger.Debugln("stmt_add_batch cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()

	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("## stmt add batch error: %s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	return &StmtAddBatchResponse{StmtID: req.StmtID}
}

type StmtExecRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type StmtExecResponse struct {
	BaseResponse
	StmtID   uint64 `json:"stmt_id"`
	Affected int    `json:"affected"`
}

func (h *messageHandler) handleStmtExec(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	var req StmtExecRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("## unmarshal stmt add batch request %s error: %s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt add batch request error", req.StmtID)
	}

	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	thread.Lock()
	logger.Debugln("stmt_execute get thread lock cost:", log.GetLogDuration(isDebug, s))
	code := wrapper.TaosStmtExecute(stmtItem.stmt)
	logger.Debugln("stmt_execute cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("## stmt execute error: %s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	affected := wrapper.TaosStmtAffectedRowsOnce(stmtItem.stmt)
	logger.Debugln("stmt_affected_rows_once cost:", log.GetLogDuration(isDebug, s))
	return &StmtExecResponse{StmtID: req.StmtID, Affected: affected}
}

type StmtCloseRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type StmtCloseResponse struct {
	BaseResponse
	StmtID uint64 `json:"stmt_id,omitempty"`
}

func (h *messageHandler) handleStmtClose(_ context.Context, request Request, logger *logrus.Entry, _ bool, _ time.Time) (resp Response) {
	var req StmtCloseRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("## unmarshal stmt close request %s error: %s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt add batch request error", req.StmtID)
	}

	h.stmts.FreeStmtByID(req.StmtID)
	resp = &BaseResponse{}
	resp.SetNull(true)
	return resp
}

type StmtGetColFieldsRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type StmtGetColFieldsResponse struct {
	BaseResponse
	StmtID uint64                  `json:"stmt_id"`
	Fields []*stmtCommon.StmtField `json:"fields"`
}

func (h *messageHandler) handleStmtGetColFields(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	var req StmtGetColFieldsRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("## unmarshal stmt get tags request %s error: %s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt get tags request error", req.StmtID)
	}

	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	thread.Lock()
	logger.Debugln("stmt_get_col_fields get thread lock cost:", log.GetLogDuration(isDebug, s))
	code, colNums, colFields := wrapper.TaosStmtGetColFields(stmtItem.stmt)
	logger.Debugln("stmt_get_col_fields cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("## stmt get col fields error: %s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	defer func() {
		wrapper.TaosStmtReclaimFields(stmtItem.stmt, colFields)
	}()
	if colNums == 0 {
		return &StmtGetColFieldsResponse{StmtID: req.StmtID}
	}
	fields := wrapper.StmtParseFields(colNums, colFields)
	logger.Debugln("stmt parse fields cost:", log.GetLogDuration(isDebug, s))
	return &StmtGetColFieldsResponse{StmtID: req.StmtID, Fields: fields}
}

type StmtGetTagFieldsRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type StmtGetTagFieldsResponse struct {
	BaseResponse
	StmtID uint64                  `json:"stmt_id"`
	Fields []*stmtCommon.StmtField `json:"fields,omitempty"`
}

func (h *messageHandler) handleStmtGetTagFields(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	var req StmtGetTagFieldsRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("## unmarshal stmt get tags request %s error: %s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt get tags request error", req.StmtID)
	}

	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	thread.Lock()
	logger.Debugln("stmt_get_tag_fields get thread lock cost:", log.GetLogDuration(isDebug, s))
	code, tagNums, tagFields := wrapper.TaosStmtGetTagFields(stmtItem.stmt)
	logger.Debugln("stmt_get_tag_fields cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("## stmt get tag fields error: %s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	defer func() {
		wrapper.TaosStmtReclaimFields(stmtItem.stmt, tagFields)
	}()
	if tagNums == 0 {
		return &StmtGetTagFieldsResponse{StmtID: req.StmtID}
	}
	logger.Debugln("stmt parse fields cost:", log.GetLogDuration(isDebug, s))
	return &StmtGetTagFieldsResponse{StmtID: req.StmtID, Fields: wrapper.StmtParseFields(tagNums, tagFields)}
}

type StmtUseResultRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type StmtUseResultResponse struct {
	BaseResponse
	StmtID        uint64             `json:"stmt_id"`
	ResultID      uint64             `json:"result_id"`
	FieldsCount   int                `json:"fields_count"`
	FieldsNames   []string           `json:"fields_names"`
	FieldsTypes   jsontype.JsonUint8 `json:"fields_types"`
	FieldsLengths []int64            `json:"fields_lengths"`
	Precision     int                `json:"precision"`
}

func (h *messageHandler) handleStmtUseResult(_ context.Context, request Request, logger *logrus.Entry, _ bool, _ time.Time) (resp Response) {
	var req StmtUseResultRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("## unmarshal stmt get tags request %s error: %s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt get tags request error", req.StmtID)
	}

	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	result := wrapper.TaosStmtUseResult(stmtItem.stmt)
	if result == nil {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("## stmt use result error: %s", errStr)
		return wsStmtErrorMsg(0xffff, errStr, req.StmtID)
	}

	fieldsCount := wrapper.TaosNumFields(result)
	rowsHeader, _ := wrapper.ReadColumn(result, fieldsCount)
	precision := wrapper.TaosResultPrecision(result)
	queryResult := QueryResult{TaosResult: result, FieldsCount: fieldsCount, Header: rowsHeader, precision: precision, inStmt: true}
	idx := h.queryResults.Add(&queryResult)

	return &StmtUseResultResponse{
		StmtID:        req.StmtID,
		ResultID:      idx,
		FieldsCount:   fieldsCount,
		FieldsNames:   rowsHeader.ColNames,
		FieldsTypes:   rowsHeader.ColTypes,
		FieldsLengths: rowsHeader.ColLength,
		Precision:     precision,
	}
}

func (h *messageHandler) handleSetTagsMessage(_ context.Context, req dealBinaryRequest, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	block := tools.AddPointer(req.p0, uintptr(24))
	columns := parser.RawBlockGetNumOfCols(block)
	rows := parser.RawBlockGetNumOfRows(block)

	if rows != 1 {
		return wsStmtErrorMsg(0xffff, "rows not equal 1", req.id)
	}

	stmtItem := h.stmts.Get(req.id)
	if stmtItem == nil {
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.id)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.id)
	}
	thread.Lock()
	logger.Debugln("stmt_get_tag_fields get thread lock cost:", log.GetLogDuration(isDebug, s))
	code, tagNums, tagFields := wrapper.TaosStmtGetTagFields(stmtItem.stmt)
	logger.Debugln("stmt_get_tag_fields cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		return wsStmtErrorMsg(code, wrapper.TaosStmtErrStr(stmtItem.stmt), req.id)
	}
	defer func() {
		wrapper.TaosStmtReclaimFields(stmtItem.stmt, tagFields)
	}()
	if tagNums == 0 {
		return &StmtSetTagsResponse{StmtID: req.id}
	}
	if int(columns) != tagNums {
		return wsStmtErrorMsg(0xffff, "stmt tags count not match", req.id)
	}
	fields := wrapper.StmtParseFields(tagNums, tagFields)
	logger.Debugln("stmt parse fields cost:", log.GetLogDuration(isDebug, s))
	tags := stmt.BlockConvert(block, int(rows), fields, nil)
	logger.Debugln("block concert cost:", log.GetLogDuration(isDebug, s))
	reTags := make([]driver.Value, tagNums)
	for i := 0; i < tagNums; i++ {
		reTags[i] = tags[i][0]
	}
	thread.Lock()
	logger.Debugln("stmt_set_tags get thread lock cost:", log.GetLogDuration(isDebug, s))
	code = wrapper.TaosStmtSetTags(stmtItem.stmt, reTags)
	logger.Debugln("stmt_set_tags cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		return wsStmtErrorMsg(code, wrapper.TaosStmtErrStr(stmtItem.stmt), req.id)
	}

	return &StmtSetTagsResponse{StmtID: req.id}
}

func (h *messageHandler) handleTMQRawMessage(_ context.Context, req dealBinaryRequest, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	length := *(*uint32)(tools.AddPointer(req.p0, uintptr(24)))
	metaType := *(*uint16)(tools.AddPointer(req.p0, uintptr(28)))
	data := tools.AddPointer(req.p0, uintptr(30))

	h.Lock()
	logger.Debugln("get global lock cost:", log.GetLogDuration(isDebug, s))
	defer h.Unlock()
	if h.closed {
		return
	}
	meta := wrapper.BuildRawMeta(length, metaType, data)

	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	code := wrapper.TMQWriteRaw(h.conn, meta)
	thread.Unlock()
	logger.Debugln("write_raw_meta cost:", log.GetLogDuration(isDebug, s))

	if code != 0 {
		errStr := wrapper.TMQErr2Str(code)
		logger.Errorf("## write raw meta error: %s", errStr)
		return wsCommonErrorMsg(int(code)&0xffff, errStr)
	}

	return &BaseResponse{}
}

func (h *messageHandler) handleRawBlockMessage(_ context.Context, req dealBinaryRequest, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	numOfRows := *(*int32)(tools.AddPointer(req.p0, uintptr(24)))
	tableNameLength := *(*uint16)(tools.AddPointer(req.p0, uintptr(28)))
	tableName := make([]byte, tableNameLength)
	for i := 0; i < int(tableNameLength); i++ {
		tableName[i] = *(*byte)(tools.AddPointer(req.p0, uintptr(30+i)))
	}
	rawBlock := tools.AddPointer(req.p0, uintptr(30+tableNameLength))

	h.Lock()
	logger.Debugln("get global lock cost:", log.GetLogDuration(isDebug, s))
	defer h.Unlock()
	if h.closed {
		return
	}

	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	code := wrapper.TaosWriteRawBlockWithReqID(h.conn, int(numOfRows), rawBlock, string(tableName), int64(req.reqID))
	thread.Unlock()
	logger.Debugln("write_raw_meta cost:", log.GetLogDuration(isDebug, s))
	if code != 0 {
		errStr := wrapper.TMQErr2Str(int32(code))
		logger.Errorf("## write raw meta error: %s", errStr)
		return wsCommonErrorMsg(int(code)&0xffff, errStr)
	}

	return &BaseResponse{}
}

func (h *messageHandler) handleRawBlockMessageWithFields(_ context.Context, req dealBinaryRequest, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	numOfRows := *(*int32)(tools.AddPointer(req.p0, uintptr(24)))
	tableNameLength := int(*(*uint16)(tools.AddPointer(req.p0, uintptr(28))))
	tableName := make([]byte, tableNameLength)
	for i := 0; i < tableNameLength; i++ {
		tableName[i] = *(*byte)(tools.AddPointer(req.p0, uintptr(30+i)))
	}
	rawBlock := tools.AddPointer(req.p0, uintptr(30+tableNameLength))
	blockLength := int(parser.RawBlockGetLength(rawBlock))
	numOfColumn := int(parser.RawBlockGetNumOfCols(rawBlock))
	fieldsBlock := tools.AddPointer(req.p0, uintptr(30+tableNameLength+blockLength))

	h.Lock()
	defer h.Unlock()
	if h.closed {
		return
	}

	logger.Debugln("get global lock cost:", log.GetLogDuration(isDebug, s))
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code := wrapper.TaosWriteRawBlockWithFieldsWithReqID(h.conn, int(numOfRows), rawBlock, string(tableName), fieldsBlock, numOfColumn, int64(req.reqID))
	thread.Unlock()
	logger.Debugln("write_raw_meta cost:", log.GetLogDuration(isDebug, s))
	if code != 0 {
		errStr := wrapper.TMQErr2Str(int32(code))
		logger.Errorf("## write raw meta error: %s", errStr)
		return wsCommonErrorMsg(int(code)&0xffff, errStr)
	}
	return &BaseResponse{}
}

type GetCurrentDBResponse struct {
	BaseResponse
	DB string `json:"db"`
}

func (h *messageHandler) handleGetCurrentDB(_ context.Context, _ Request, logger *logrus.Entry, _ bool, _ time.Time) (resp Response) {
	thread.Lock()
	db, err := wrapper.TaosGetCurrentDB(h.conn)
	thread.Unlock()
	if err != nil {
		var taosErr *errors2.TaosError
		errors.As(err, &taosErr)
		logger.Errorf("## get current db error: %s", taosErr.Error())
		return wsCommonErrorMsg(int(taosErr.Code), taosErr.Error())
	}
	return &GetCurrentDBResponse{DB: db}
}

type GetServerInfoResponse struct {
	BaseResponse
	Info string `json:"info"`
}

func (h *messageHandler) handleGetServerInfo(_ context.Context, _ Request, _ *logrus.Entry, _ bool, _ time.Time) (resp Response) {
	thread.Lock()
	serverInfo := wrapper.TaosGetServerInfo(h.conn)
	thread.Unlock()
	return &GetServerInfoResponse{Info: serverInfo}
}

type NumFieldsRequest struct {
	ReqID    uint64 `json:"req_id"`
	ResultID uint64 `json:"result_id"`
}

type NumFieldsResponse struct {
	BaseResponse
	NumFields int `json:"num_fields"`
}

func (h *messageHandler) handleNumFields(_ context.Context, request Request, logger *logrus.Entry, _ bool, _ time.Time) (resp Response) {
	var req NumFieldsRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("## unmarshal stmt num params request %s error: %s", request.Args, err)
		return wsCommonErrorMsg(0xffff, "unmarshal stmt num params request error")
	}

	item := h.queryResults.Get(req.ResultID)
	if item == nil {
		return wsCommonErrorMsg(0xffff, "result is nil")
	}
	item.Lock()
	defer item.Unlock()
	if item.TaosResult == nil {
		return wsCommonErrorMsg(0xffff, "result has been freed")
	}
	thread.Lock()
	num := wrapper.TaosNumFields(item.TaosResult)
	thread.Unlock()
	return &NumFieldsResponse{NumFields: num}
}

type StmtNumParamsRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type StmtNumParamsResponse struct {
	BaseResponse
	StmtID    uint64 `json:"stmt_id"`
	NumParams int    `json:"num_params"`
}

func (h *messageHandler) handleStmtNumParams(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	var req StmtNumParamsRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("## unmarshal stmt num params request %s error: %s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt num params request error", req.StmtID)
	}

	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	thread.Lock()
	logger.Debugln("stmt_num_params get thread lock cost:", log.GetLogDuration(isDebug, s))
	count, code := wrapper.TaosStmtNumParams(stmtItem.stmt)
	logger.Debugln("stmt_num_params cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("## stmt get col fields error: %s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	return &StmtNumParamsResponse{StmtID: req.StmtID, NumParams: count}
}

type StmtGetParamRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
	Index  int    `json:"index"`
}

type StmtGetParamResponse struct {
	BaseResponse
	StmtID   uint64 `json:"stmt_id"`
	Index    int    `json:"index"`
	DataType int    `json:"data_type"`
	Length   int    `json:"length"`
}

func (h *messageHandler) handleStmtGetParam(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	var req StmtGetParamRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("## unmarshal stmt get param request %s error: %s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt get param request error", req.StmtID)
	}

	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	thread.Lock()
	logger.Debugln("stmt_get_param get thread lock cost:", log.GetLogDuration(isDebug, s))
	dataType, length, err := wrapper.TaosStmtGetParam(stmtItem.stmt, req.Index)
	logger.Debugln("stmt_get_param cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if err != nil {
		var taosErr *errors2.TaosError
		errors.As(err, &taosErr)
		logger.Errorf("## stmt get param error: %s", taosErr.Error())
		return wsStmtErrorMsg(int(taosErr.Code), taosErr.Error(), req.StmtID)
	}
	return &StmtGetParamResponse{StmtID: req.StmtID, Index: req.Index, DataType: dataType, Length: length}
}

func freeCPointer(pointer unsafe.Pointer) {
	if pointer == nil {
		return
	}
	isDebug := log.IsDebug()
	s := log.GetLogNow(log.IsDebug())
	thread.Lock()
	logger.Debugln("free result get lock cost:", log.GetLogDuration(isDebug, s))
	wrapper.TaosFreeResult(pointer)
	logger.Debugln("free result cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
}

type Response interface {
	SetCode(code int)
	SetMessage(message string)
	SetAction(action string)
	SetReqID(reqID uint64)
	SetTiming(timing int64)
	SetBinary(b bool)
	IsBinary() bool
	SetNull(b bool)
	IsNull() bool
}

func (b *BaseResponse) SetCode(code int) {
	b.Code = code
}

func (b *BaseResponse) SetMessage(message string) {
	b.Message = message
}

func (b *BaseResponse) SetAction(action string) {
	b.Action = action
}

func (b *BaseResponse) SetReqID(reqID uint64) {
	b.ReqID = reqID
}

func (b *BaseResponse) SetTiming(timing int64) {
	b.Timing = timing
}

func (b *BaseResponse) SetBinary(binary bool) {
	b.binary = binary
}

func (b *BaseResponse) IsBinary() bool {
	return b.binary
}

func (b *BaseResponse) SetNull(null bool) {
	b.null = null
}

func (b *BaseResponse) IsNull() bool {
	return b.null
}

type VersionResponse struct {
	BaseResponse
	Version string `json:"version"`
}

type BinaryResponse struct {
	BaseResponse
	Data []byte
}

type WSStmtErrorResp struct {
	BaseResponse
	StmtID uint64 `json:"stmt_id"`
}

func wsStmtErrorMsg(code int, message string, stmtID uint64) *WSStmtErrorResp {
	return &WSStmtErrorResp{
		BaseResponse: BaseResponse{
			Code:    code & 0xffff,
			Message: message,
		},
		StmtID: stmtID,
	}
}

func wsCommonErrorMsg(code int, message string) *BaseResponse {
	return &BaseResponse{
		Code:    code & 0xffff,
		Message: message,
	}
}

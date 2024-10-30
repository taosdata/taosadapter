package ws

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/huskar-t/melody"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/parser"
	stmtCommon "github.com/taosdata/driver-go/v3/common/stmt"
	errors2 "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/types"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller/ws/stmt"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/tools"
	"github.com/taosdata/taosadapter/v3/tools/bytesutil"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
	"github.com/taosdata/taosadapter/v3/tools/jsontype"
	"github.com/taosdata/taosadapter/v3/version"
)

type messageHandler struct {
	conn         unsafe.Pointer
	logger       *logrus.Entry
	closed       bool
	once         sync.Once
	wait         sync.WaitGroup
	dropUserChan chan struct{}
	sync.RWMutex

	queryResults *QueryResultHolder // ws query
	stmts        *StmtHolder        // stmt bind message

	exit                  chan struct{}
	whitelistChangeChan   chan int64
	session               *melody.Session
	ip                    net.IP
	ipStr                 string
	whitelistChangeHandle cgo.Handle
	dropUserHandle        cgo.Handle
}

func newHandler(session *melody.Session) *messageHandler {
	logger := wstool.GetLogger(session)
	ipAddr := iptool.GetRealIP(session.Request)
	whitelistChangeChan, whitelistChangeHandle := tool.GetRegisterChangeWhiteListHandle()
	dropUserChan, dropUserHandle := tool.GetRegisterDropUserHandle()
	return &messageHandler{
		queryResults:          NewQueryResultHolder(),
		stmts:                 NewStmtHolder(),
		exit:                  make(chan struct{}),
		whitelistChangeChan:   whitelistChangeChan,
		whitelistChangeHandle: whitelistChangeHandle,
		dropUserChan:          dropUserChan,
		dropUserHandle:        dropUserHandle,
		session:               session,
		ip:                    ipAddr,
		ipStr:                 ipAddr.String(),
		logger:                logger,
	}
}

func (h *messageHandler) waitSignal(logger *logrus.Entry) {
	defer func() {
		logger.Trace("exit wait signal")
		tool.PutRegisterChangeWhiteListHandle(h.whitelistChangeHandle)
		tool.PutRegisterDropUserHandle(h.dropUserHandle)
	}()
	for {
		select {
		case <-h.dropUserChan:
			logger.Info("get drop user signal")
			isDebug := log.IsDebug()
			h.lock(logger, isDebug)
			if h.closed {
				logger.Trace("server closed")
				h.Unlock()
				return
			}
			logger.Info("user dropped, close connection")
			s := log.GetLogNow(isDebug)
			h.session.Close()
			h.Unlock()
			logger.Debugf("close session cost:%s", log.GetLogDuration(isDebug, s))
			s = log.GetLogNow(isDebug)
			h.Close()
			logger.Debugf("close handler cost:%s", log.GetLogDuration(isDebug, s))
			return
		case <-h.whitelistChangeChan:
			logger.Info("get whitelist change signal")
			isDebug := log.IsDebug()
			h.lock(logger, isDebug)
			if h.closed {
				logger.Trace("server closed")
				h.Unlock()
				return
			}
			logger.Trace("get whitelist")
			s := log.GetLogNow(isDebug)
			whitelist, err := tool.GetWhitelist(h.conn)
			if err != nil {
				logger.Errorf("get whitelist error, close connection, err:%s", err)
				s = log.GetLogNow(isDebug)
				h.session.Close()
				logger.Debugf("close session cost:%s", log.GetLogDuration(isDebug, s))
				h.Unlock()
				s = log.GetLogNow(isDebug)
				h.Close()
				logger.Debugf("close handler cost:%s", log.GetLogDuration(isDebug, s))
				return
			}
			logger.Tracef("check whitelist, ip:%s, whitelist:%s", h.ipStr, tool.IpNetSliceToString(whitelist))
			valid := tool.CheckWhitelist(whitelist, h.ip)
			if !valid {
				logger.Errorf("ip not in whitelist! close connection, ip:%s, whitelist:%s", h.ipStr, tool.IpNetSliceToString(whitelist))
				logger.Trace("close session")
				s = log.GetLogNow(isDebug)
				h.session.Close()
				logger.Debugf("close session cost:%s", log.GetLogDuration(isDebug, s))
				h.Unlock()
				logger.Trace("close handler")
				s = log.GetLogNow(isDebug)
				h.Close()
				logger.Debugf("close handler cost:%s", log.GetLogDuration(isDebug, s))
				return
			}
			h.Unlock()
		case <-h.exit:
			return
		}
	}
}

func (h *messageHandler) lock(logger *logrus.Entry, isDebug bool) {
	logger.Trace("get handler lock")
	s := log.GetLogNow(isDebug)
	h.Lock()
	logger.Debugf("get handler lock cost:%s", log.GetLogDuration(isDebug, s))
}

func (h *messageHandler) Close() {
	h.Lock()
	defer h.Unlock()

	if h.closed {
		h.logger.Trace("server closed")
		return
	}
	h.closed = true
	h.stop()
	close(h.exit)
}

type Request struct {
	ReqID  uint64          `json:"req_id"`
	Action string          `json:"action"`
	Args   json.RawMessage `json:"args"`
}

var jsonI = jsoniter.ConfigCompatibleWithStandardLibrary

func (h *messageHandler) handleMessage(session *melody.Session, data []byte) {
	ctx := context.WithValue(context.Background(), wstool.StartTimeKey, time.Now().UnixNano())
	h.logger.Debugf("get ws message data:%s", data)

	var request Request
	if err := json.Unmarshal(data, &request); err != nil {
		h.logger.WithError(err).Errorln("unmarshal ws request")
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
	case STMT2Init:
		f = h.handleStmt2Init
	case STMT2Prepare:
		f = h.handleStmt2Prepare
	case STMT2GetFields:
		f = h.handleStmt2GetFields
	case STMT2Exec:
		f = h.handleStmt2Exec
	case STMT2Result:
		f = h.handleStmt2UseResult
	case STMT2Close:
		f = h.handleStmt2Close
	default:
		f = h.handleDefault
	}
	h.deal(ctx, session, request, f)
}

func (h *messageHandler) handleMessageBinary(session *melody.Session, bytes []byte) {
	//p0 uin64  req_id
	//p0+8 uint64  message_id
	//p0+16 uint64 (1 (set tag) 2 (bind))
	h.logger.Tracef("get ws block message data:%+v", bytes)
	p0 := unsafe.Pointer(&bytes[0])
	reqID := *(*uint64)(p0)
	messageID := *(*uint64)(tools.AddPointer(p0, uintptr(8)))
	action := *(*uint64)(tools.AddPointer(p0, uintptr(16)))
	h.logger.Debugf("get ws message binary QID:0x%x, messageID:%d, action:%d", reqID, messageID, action)

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
	case BinaryQueryMessage:
		f = h.handleBinaryQuery
	case FetchRawBlockMessage:
		f = h.handleFetchRawBlock
	case Stmt2BindMessage:
		f = h.handleStmt2Bind
	default:
		f = h.handleDefaultBinary
	}
	h.dealBinary(ctx, session, mt, reqID, messageID, p0, bytes, f)
}

type RequestID struct {
	ReqID uint64 `json:"req_id"`
}

type dealFunc func(context.Context, Request, *logrus.Entry, bool, time.Time) Response

type dealBinaryRequest struct {
	action  messageType
	reqID   uint64
	id      uint64 // messageID or stmtID
	p0      unsafe.Pointer
	message []byte
}
type dealBinaryFunc func(context.Context, dealBinaryRequest, *logrus.Entry, bool, time.Time) Response

func (h *messageHandler) deal(ctx context.Context, session *melody.Session, request Request, f dealFunc) {
	h.wait.Add(1)
	go func() {
		defer h.wait.Done()
		isDebug := log.IsDebug()
		reqID := request.ReqID
		if reqID == 0 {
			var req RequestID
			_ = json.Unmarshal(request.Args, &req)
			reqID = req.ReqID
		}
		request.ReqID = reqID

		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       request.Action,
			config.ReqIDKey: reqID,
		})

		if h.conn == nil && request.Action != Connect && request.Action != wstool.ClientVersion {
			logger.Errorf("server not connected")
			resp := wsCommonErrorMsg(0xffff, "server not connected")
			h.writeResponse(ctx, session, resp, request.Action, request.ReqID, logger)
			return
		}

		s := log.GetLogNow(isDebug)

		resp := f(ctx, request, logger, isDebug, s)
		h.writeResponse(ctx, session, resp, request.Action, reqID, logger)
	}()
}

func (h *messageHandler) dealBinary(ctx context.Context, session *melody.Session, action messageType, reqID uint64, messageID uint64, p0 unsafe.Pointer, message []byte, f dealBinaryFunc) {
	h.wait.Add(1)
	go func() {
		defer h.wait.Done()

		logger := h.logger.WithField(actionKey, action.String()).WithField(config.ReqIDKey, reqID)
		isDebug := log.IsDebug()
		if h.conn == nil {
			resp := wsCommonErrorMsg(0xffff, "server not connected")
			h.writeResponse(ctx, session, resp, action.String(), reqID, logger)
			return
		}

		s := log.GetLogNow(isDebug)

		req := dealBinaryRequest{
			action:  action,
			reqID:   reqID,
			id:      messageID,
			p0:      p0,
			message: message,
		}
		resp := f(ctx, req, logger, isDebug, s)
		h.writeResponse(ctx, session, resp, action.String(), reqID, logger)
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

func (h *messageHandler) writeResponse(ctx context.Context, session *melody.Session, response Response, action string, reqID uint64, logger *logrus.Entry) {
	if response == nil {
		logger.Trace("response is nil")
		// session closed handle return nil
		return
	}
	if response.IsNull() {
		logger.Trace("no need to response")
		return
	}
	if response.IsBinary() {
		logger.Tracef("write binary response:%v", response)
		_ = session.WriteBinary(response.(*BinaryResponse).Data)
		return
	}
	response.SetAction(action)
	response.SetReqID(reqID)
	response.SetTiming(wstool.GetDuration(ctx))

	respByte, _ := json.Marshal(response)
	logger.Tracef("write json response:%s", respByte)
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
		h.queryResults.FreeAll(h.logger)
		h.stmts.FreeAll(h.logger)
		// clean connection
		if h.conn != nil {
			syncinterface.TaosClose(h.conn, h.logger, log.IsDebug())
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
		logger.Errorf("unmarshal connect request:%s, error, err:%s", string(request.Args), err)
		return wsCommonErrorMsg(0xffff, "unmarshal connect request error")
	}

	h.lock(logger, isDebug)
	defer h.Unlock()
	if h.closed {
		logger.Trace("server closed")
		return
	}
	if h.conn != nil {
		logger.Trace("duplicate connections")
		return wsCommonErrorMsg(0xffff, "duplicate connections")
	}

	conn, err := syncinterface.TaosConnect("", req.User, req.Password, req.DB, 0, logger, isDebug)

	if err != nil {
		logger.WithError(err).Errorln("connect to TDengine error")
		var taosErr *errors2.TaosError
		errors.As(err, &taosErr)
		return wsCommonErrorMsg(int(taosErr.Code), taosErr.ErrStr)
	}
	logger.Trace("get whitelist")
	s = log.GetLogNow(isDebug)
	whitelist, err := tool.GetWhitelist(conn)
	logger.Debugf("get whitelist cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.WithError(err).Errorln("get whitelist error")
		syncinterface.TaosClose(conn, logger, isDebug)
		var taosErr *errors2.TaosError
		errors.As(err, &taosErr)
		return wsCommonErrorMsg(int(taosErr.Code), taosErr.ErrStr)
	}
	logger.Tracef("check whitelist, ip:%s, whitelist:%s", h.ipStr, tool.IpNetSliceToString(whitelist))
	valid := tool.CheckWhitelist(whitelist, h.ip)
	if !valid {
		logger.Errorf("ip not in whitelist, ip:%s, whitelist:%s", h.ipStr, tool.IpNetSliceToString(whitelist))
		syncinterface.TaosClose(conn, logger, isDebug)
		return wsCommonErrorMsg(0xffff, "whitelist prohibits current IP access")
	}
	s = log.GetLogNow(isDebug)
	logger.Trace("register whitelist change")
	err = tool.RegisterChangeWhitelist(conn, h.whitelistChangeHandle)
	logger.Debugf("register whitelist change cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.WithError(err).Errorln("register whitelist change error")
		syncinterface.TaosClose(conn, logger, isDebug)
		var taosErr *errors2.TaosError
		errors.As(err, &taosErr)
		return wsCommonErrorMsg(int(taosErr.Code), taosErr.ErrStr)
	}
	s = log.GetLogNow(isDebug)
	logger.Trace("register drop user")
	err = tool.RegisterDropUser(conn, h.dropUserHandle)
	logger.Debugf("register drop user cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.WithError(err).Errorln("register drop user error")
		syncinterface.TaosClose(conn, logger, isDebug)
		var taosErr *errors2.TaosError
		errors.As(err, &taosErr)
		return wsCommonErrorMsg(int(taosErr.Code), taosErr.ErrStr)
	}
	if req.Mode != nil {
		switch *req.Mode {
		case common.TAOS_CONN_MODE_BI:
			// BI mode
			logger.Trace("set connection mode to BI")
			code := wrapper.TaosSetConnMode(conn, common.TAOS_CONN_MODE_BI, 1)
			logger.Trace("set connection mode to BI done")
			if code != 0 {
				logger.Errorf("set connection mode to BI error, err:%s", wrapper.TaosErrorStr(nil))
				syncinterface.TaosClose(conn, logger, isDebug)
				return wsCommonErrorMsg(code, wrapper.TaosErrorStr(nil))
			}
		default:
			syncinterface.TaosClose(conn, logger, isDebug)
			logger.Tracef("unexpected mode:%d", *req.Mode)
			return wsCommonErrorMsg(0xffff, fmt.Sprintf("unexpected mode:%d", req.Mode))
		}
	}
	h.conn = conn
	logger.Trace("start wait signal goroutine")
	go h.waitSignal(h.logger)
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
		logger.Errorf("unmarshal ws query request %s error, err:%s", request.Args, err)
		return wsCommonErrorMsg(0xffff, "unmarshal ws query request error")
	}
	sqlType := monitor.WSRecordRequest(req.Sql)
	logger.Debugf("get query request, sql:%s", req.Sql)
	handler := async.GlobalAsync.HandlerPool.Get()
	defer async.GlobalAsync.HandlerPool.Put(handler)
	logger.Debugf("get handler cost:%s", log.GetLogDuration(isDebug, s))
	result := async.GlobalAsync.TaosQuery(h.conn, logger, isDebug, req.Sql, handler, int64(request.ReqID))
	code := wrapper.TaosError(result.Res)
	if code != httperror.SUCCESS {
		monitor.WSRecordResult(sqlType, false)
		errStr := wrapper.TaosErrorStr(result.Res)
		logger.Errorf("query error, code:%d, message:%s", code, errStr)
		syncinterface.FreeResult(result.Res, logger, isDebug)
		return wsCommonErrorMsg(code, errStr)
	}

	monitor.WSRecordResult(sqlType, true)
	logger.Trace("check is_update_query")
	s = log.GetLogNow(isDebug)
	isUpdate := wrapper.TaosIsUpdateQuery(result.Res)
	logger.Debugf("get is_update_query %t, cost:%s", isUpdate, log.GetLogDuration(isDebug, s))
	if isUpdate {
		s = log.GetLogNow(isDebug)
		affectRows := wrapper.TaosAffectedRows(result.Res)
		logger.Debugf("affected_rows %d cost:%s", affectRows, log.GetLogDuration(isDebug, s))
		syncinterface.FreeResult(result.Res, logger, isDebug)
		return &QueryResponse{IsUpdate: true, AffectedRows: affectRows}
	}
	s = log.GetLogNow(isDebug)
	fieldsCount := wrapper.TaosNumFields(result.Res)
	logger.Debugf("get num_fields:%d, cost:%s", fieldsCount, log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	rowsHeader, _ := wrapper.ReadColumn(result.Res, fieldsCount)
	logger.Debugf("read column cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	precision := wrapper.TaosResultPrecision(result.Res)
	logger.Debugf("get result_precision:%d, cost:%s", precision, log.GetLogDuration(isDebug, s))
	queryResult := QueryResult{TaosResult: result.Res, FieldsCount: fieldsCount, Header: rowsHeader, precision: precision}
	idx := h.queryResults.Add(&queryResult)
	logger.Trace("add result to list finished")

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
		logger.Errorf("unmarshal ws fetch request %s error, err:%s", request.Args, err)
		return wsCommonErrorMsg(0xffff, "unmarshal ws fetch request error")
	}

	logger.Tracef("get result by id, id:%d", req.ID)
	item := h.queryResults.Get(req.ID)
	if item == nil {
		logger.Errorf("result is nil")
		return wsCommonErrorMsg(0xffff, "result is nil")
	}
	item.Lock()
	if item.TaosResult == nil {
		item.Unlock()
		logger.Errorf("result has been freed")
		return wsCommonErrorMsg(0xffff, "result has been freed")
	}
	s = log.GetLogNow(isDebug)
	handler := async.GlobalAsync.HandlerPool.Get()
	defer async.GlobalAsync.HandlerPool.Put(handler)
	logger.Debugf("get handler, cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	result := async.GlobalAsync.TaosFetchRawBlockA(item.TaosResult, logger, isDebug, handler)
	logger.Debugf("fetch_raw_block_a, cost:%s", log.GetLogDuration(isDebug, s))
	if result.N == 0 {
		logger.Trace("fetch raw block completed")
		item.Unlock()
		h.queryResults.FreeResultByID(req.ID, logger)
		return &FetchResponse{ID: req.ID, Completed: true}
	}
	if result.N < 0 {
		item.Unlock()
		errStr := wrapper.TaosErrorStr(result.Res)
		logger.Errorf("fetch raw block error, code:%d, message:%s", result.N, errStr)
		h.queryResults.FreeResultByID(req.ID, logger)
		return wsCommonErrorMsg(0xffff, errStr)
	}
	s = log.GetLogNow(isDebug)
	length := wrapper.FetchLengths(item.TaosResult, item.FieldsCount)
	logger.Debugf("fetch_lengths result:%d, cost:%s", length, log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	logger.Trace("get raw block")
	item.Block = wrapper.TaosGetRawBlock(item.TaosResult)
	logger.Debugf("get_raw_block result:%p, cost:%s", item.Block, log.GetLogDuration(isDebug, s))
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
		logger.Errorf("unmarshal ws fetch block request, req:%s, error, err:%s", request.Args, err)
		return wsCommonErrorMsg(0xffff, "unmarshal ws fetch block request error")
	}

	item := h.queryResults.Get(req.ID)
	if item == nil {
		logger.Errorf("result is nil")
		return wsCommonErrorMsg(0xffff, "result is nil")
	}
	item.Lock()
	defer item.Unlock()
	if item.TaosResult == nil {
		logger.Trace("result has been freed")
		return wsCommonErrorMsg(0xffff, "result has been freed")
	}
	if item.Block == nil {
		logger.Trace("block is nil")
		return wsCommonErrorMsg(0xffff, "block is nil")
	}

	blockLength := int(parser.RawBlockGetLength(item.Block))
	if blockLength <= 0 {
		return wsCommonErrorMsg(0xffff, "block length illegal")
	}
	if cap(item.buf) < blockLength+16 {
		item.buf = make([]byte, 0, blockLength+16)
	}
	item.buf = item.buf[:blockLength+16]
	binary.LittleEndian.PutUint64(item.buf, uint64(wstool.GetDuration(ctx)))
	binary.LittleEndian.PutUint64(item.buf[8:], req.ID)
	bytesutil.Copy(item.Block, item.buf, 16, blockLength)
	logger.Debugf("handle binary content cost:%s", log.GetLogDuration(isDebug, s))
	resp = &BinaryResponse{Data: item.buf}
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
		logger.Errorf("unmarshal ws fetch request %s error, err:%s", request.Args, err)
		return wsCommonErrorMsg(0xffff, "unmarshal connect request error")
	}
	logger.Tracef("free result by id, id:%d", req.ID)
	h.queryResults.FreeResultByID(req.ID, logger)
	resp = &BaseResponse{}
	resp.SetNull(true)
	return resp
}

type SchemalessWriteRequest struct {
	ReqID        uint64 `json:"req_id"`
	Protocol     int    `json:"protocol"`
	Precision    string `json:"precision"`
	TTL          int    `json:"ttl"`
	Data         string `json:"data"`
	TableNameKey string `json:"table_name_key"`
}

type SchemalessWriteResponse struct {
	BaseResponse
	AffectedRows int   `json:"affected_rows"`
	TotalRows    int32 `json:"total_rows"`
}

func (h *messageHandler) handleSchemalessWrite(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, _ time.Time) (resp Response) {
	var req SchemalessWriteRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("unmarshal schemaless write request %s error, err:%s", request.Args, err)
		return wsCommonErrorMsg(0xffff, "unmarshal schemaless write request error")
	}

	if req.Protocol == 0 {
		logger.Errorf("schemaless write request %s args error. protocol is null", request.Args)
		return wsCommonErrorMsg(0xffff, "args error")
	}
	var totalRows int32
	var affectedRows int
	totalRows, result := syncinterface.TaosSchemalessInsertRawTTLWithReqIDTBNameKey(h.conn, req.Data, req.Protocol, req.Precision, req.TTL, int64(request.ReqID), req.TableNameKey, logger, isDebug)
	logger.Tracef("total_rows:%d, result:%p", totalRows, result)
	defer syncinterface.FreeResult(result, logger, isDebug)
	affectedRows = wrapper.TaosAffectedRows(result)
	if code := wrapper.TaosError(result); code != 0 {
		logger.Errorf("schemaless write error, err:%s", wrapper.TaosErrorStr(result))
		return wsCommonErrorMsg(code, wrapper.TaosErrorStr(result))
	}
	logger.Tracef("schemaless write total rows:%d, affected rows:%d", totalRows, affectedRows)
	return &SchemalessWriteResponse{
		TotalRows:    totalRows,
		AffectedRows: affectedRows,
	}
}

type StmtInitResponse struct {
	BaseResponse
	StmtID uint64 `json:"stmt_id"`
}

func (h *messageHandler) handleStmtInit(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, _ time.Time) (resp Response) {
	stmtInit := syncinterface.TaosStmtInitWithReqID(h.conn, int64(request.ReqID), logger, isDebug)
	if stmtInit == nil {
		errStr := wrapper.TaosStmtErrStr(stmtInit)
		logger.Errorf("stmt init error, err:%s", errStr)
		return wsCommonErrorMsg(0xffff, errStr)
	}
	stmtItem := &StmtItem{stmt: stmtInit}
	h.stmts.Add(stmtItem)
	logger.Tracef("stmt init sucess, stmt_id:%d, stmt pointer:%p", stmtItem.index, stmtInit)
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
		logger.Errorf("unmarshal stmt prepare request %s error, err:%s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal connect request error", req.StmtID)
	}
	logger.Debugf("stmt prepare, stmt_id:%d, sql:%s", req.StmtID, req.SQL)
	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	s = log.GetLogNow(isDebug)
	logger.Trace("get stmt lock")
	stmtItem.Lock()
	logger.Debugf("get stmt lock cost:%s", log.GetLogDuration(isDebug, s))
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		logger.Errorf("stmt has been freed, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	code := syncinterface.TaosStmtPrepare(stmtItem.stmt, req.SQL, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("stmt prepare error, err:%s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	logger.Tracef("stmt prepare success, stmt_id:%d", req.StmtID)
	isInsert, code := syncinterface.TaosStmtIsInsert(stmtItem.stmt, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("check stmt is insert error, err:%s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	logger.Tracef("stmt is insert:%t", isInsert)
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

func (h *messageHandler) handleStmtSetTableName(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, _ time.Time) (resp Response) {
	var req StmtSetTableNameRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("unmarshal stmt set table name request %s error, err:%s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt set table name request error", req.StmtID)
	}

	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		logger.Errorf("stmt has been freed, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	code := syncinterface.TaosStmtSetTBName(stmtItem.stmt, req.Name, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("stmt set table name error, err:%s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	logger.Tracef("stmt set table name success, stmt_id:%d", req.StmtID)
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
		logger.Errorf("unmarshal stmt set tags request %s error, err:%s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt set tags request error", req.StmtID)
	}
	logger.Tracef("stmt set tags, stmt_id:%d, tags:%s", req.StmtID, req.Tags)
	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		logger.Errorf("stmt has been freed, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	code, tagNums, tagFields := syncinterface.TaosStmtGetTagFields(stmtItem.stmt, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("stmt get tag fields error, err:%s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	defer func() {
		wrapper.TaosStmtReclaimFields(stmtItem.stmt, tagFields)
	}()
	logger.Tracef("stmt tag nums:%d", tagNums)
	if tagNums == 0 {
		logger.Trace("no tags")
		return &StmtSetTagsResponse{StmtID: req.StmtID}
	}
	s = log.GetLogNow(isDebug)
	fields := wrapper.StmtParseFields(tagNums, tagFields)
	logger.Debugf("stmt parse fields cost:%s", log.GetLogDuration(isDebug, s))
	tags := make([][]driver.Value, tagNums)
	for i := 0; i < tagNums; i++ {
		tags[i] = []driver.Value{req.Tags[i]}
	}
	data, err := stmt.StmtParseTag(req.Tags, fields)
	logger.Debugf("stmt parse tag json cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.Errorf("stmt parse tag json error, err:%s", err.Error())
		return wsStmtErrorMsg(0xffff, fmt.Sprintf("stmt parse tag json:%s", err.Error()), req.StmtID)
	}
	code = syncinterface.TaosStmtSetTags(stmtItem.stmt, data, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("stmt set tags error, err:%s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	logger.Trace("stmt set tags success")
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
		logger.Errorf("unmarshal stmt bind tag request %s error, err:%s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt bind request error", req.StmtID)
	}

	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		logger.Errorf("stmt has been freed, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	code, colNums, colFields := syncinterface.TaosStmtGetColFields(stmtItem.stmt, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("stmt get col fields error, err:%s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	defer func() {
		wrapper.TaosStmtReclaimFields(stmtItem.stmt, colFields)
	}()
	if colNums == 0 {
		logger.Trace("no columns")
		return &StmtBindResponse{StmtID: req.StmtID}
	}
	s = log.GetLogNow(isDebug)
	fields := wrapper.StmtParseFields(colNums, colFields)
	logger.Debugf("stmt parse fields cost:%s", log.GetLogDuration(isDebug, s))
	fieldTypes := make([]*types.ColumnType, colNums)

	var err error
	for i := 0; i < colNums; i++ {
		if fieldTypes[i], err = fields[i].GetType(); err != nil {
			logger.Errorf("stmt get column type error, err:%s", err.Error())
			return wsStmtErrorMsg(0xffff, fmt.Sprintf("stmt get column type error, err:%s", err.Error()), req.StmtID)
		}
	}
	s = log.GetLogNow(isDebug)
	data, err := stmt.StmtParseColumn(req.Columns, fields, fieldTypes)
	logger.Debugf("stmt parse column json cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.Errorf("stmt parse column json error, err:%s", err.Error())
		return wsStmtErrorMsg(0xffff, fmt.Sprintf("stmt parse column json:%s", err.Error()), req.StmtID)
	}
	code = syncinterface.TaosStmtBindParamBatch(stmtItem.stmt, data, fieldTypes, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("stmt bind param error, err:%s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	logger.Trace("stmt bind success")
	return &StmtBindResponse{StmtID: req.StmtID}
}

func (h *messageHandler) handleBindMessage(_ context.Context, req dealBinaryRequest, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	block := tools.AddPointer(req.p0, uintptr(24))
	columns := parser.RawBlockGetNumOfCols(block)
	rows := parser.RawBlockGetNumOfRows(block)
	logger.Tracef("bind message, stmt_id:%d columns:%d, rows:%d", req.id, columns, rows)
	stmtItem := h.stmts.Get(req.id)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.id)
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.id)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		logger.Errorf("stmt has been freed, stmt_id:%d", req.id)
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.id)
	}
	var data [][]driver.Value
	var fieldTypes []*types.ColumnType
	if stmtItem.isInsert {
		code, colNums, colFields := syncinterface.TaosStmtGetColFields(stmtItem.stmt, logger, isDebug)
		if code != httperror.SUCCESS {
			errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
			logger.Errorf("stmt get col fields error, err:%s", errStr)
			return wsStmtErrorMsg(code, errStr, req.id)
		}
		defer func() {
			wrapper.TaosStmtReclaimFields(stmtItem.stmt, colFields)
		}()
		if colNums == 0 {
			logger.Trace("no columns")
			return &StmtBindResponse{StmtID: req.id}
		}
		s = log.GetLogNow(isDebug)
		fields := wrapper.StmtParseFields(colNums, colFields)
		logger.Debugf("stmt parse fields cost:%s", log.GetLogDuration(isDebug, s))
		fieldTypes = make([]*types.ColumnType, colNums)
		var err error
		for i := 0; i < colNums; i++ {
			fieldTypes[i], err = fields[i].GetType()
			if err != nil {
				logger.Errorf("stmt get column type error, err:%s", err.Error())
				return wsStmtErrorMsg(0xffff, fmt.Sprintf("stmt get column type error, err:%s", err.Error()), req.id)
			}
		}
		if int(columns) != colNums {
			logger.Errorf("stmt column count not match %d != %d", columns, colNums)
			return wsStmtErrorMsg(0xffff, "stmt column count not match", req.id)
		}
		s = log.GetLogNow(isDebug)
		data = stmt.BlockConvert(block, int(rows), fields, fieldTypes)
		logger.Debugf("block convert cost:%s", log.GetLogDuration(isDebug, s))
	} else {
		var fields []*stmtCommon.StmtField
		var err error
		logger.Trace("parse row block info")
		fields, fieldTypes, err = parseRowBlockInfo(block, int(columns))
		if err != nil {
			logger.Errorf("parse row block info error, err:%s", err.Error())
			return wsStmtErrorMsg(0xffff, fmt.Sprintf("parse row block info error, err:%s", err.Error()), req.id)
		}
		logger.Trace("convert block to data")
		data = stmt.BlockConvert(block, int(rows), fields, fieldTypes)
		logger.Trace("convert block to data finish")
	}

	code := syncinterface.TaosStmtBindParamBatch(stmtItem.stmt, data, fieldTypes, logger, isDebug)
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("stmt bind param error, err:%s", errStr)
		return wsStmtErrorMsg(code, errStr, req.id)
	}
	logger.Trace("stmt bind param success")
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
		//case common.TSDB_DATA_TYPE_TIMESTAMP:// todo precision
		//	fields[i] = &stmtCommon.StmtField{FieldType:common.TSDB_DATA_TYPE_TIMESTAMP}
		//	fieldTypes[i] = &types.ColumnType{Type:types.TaosTimestampType}
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

func (h *messageHandler) handleStmtAddBatch(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, _ time.Time) (resp Response) {
	var req StmtAddBatchRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("unmarshal stmt add batch request %s error, err:%s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt add batch request error", req.StmtID)
	}

	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		logger.Errorf("stmt has been freed, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	code := syncinterface.TaosStmtAddBatch(stmtItem.stmt, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("stmt add batch error, err:%s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	logger.Trace("stmt add batch success")
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
		logger.Errorf("unmarshal stmt exec request %s error, err:%s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt exec request error", req.StmtID)
	}
	logger.Tracef("stmt execute, stmt_id:%d", req.StmtID)
	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		logger.Errorf("stmt has been freed, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	code := syncinterface.TaosStmtExecute(stmtItem.stmt, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("stmt execute error, err:%s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	s = log.GetLogNow(isDebug)
	affected := wrapper.TaosStmtAffectedRowsOnce(stmtItem.stmt)
	logger.Debugf("stmt_affected_rows_once, affected:%d, cost:%s", affected, log.GetLogDuration(isDebug, s))
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
		logger.Errorf("unmarshal stmt close request %s error, err:%s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt close request error", req.StmtID)
	}
	logger.Tracef("stmt close, stmt_id:%d", req.StmtID)
	err := h.stmts.FreeStmtByID(req.StmtID, false, logger)
	if err != nil {
		logger.Errorf("stmt close error, err:%s", err.Error())
		return wsStmtErrorMsg(0xffff, "unmarshal stmt close request error", req.StmtID)
	}
	resp = &BaseResponse{}
	resp.SetNull(true)
	logger.Tracef("stmt close success, stmt_id:%d", req.StmtID)
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
		logger.Errorf("unmarshal stmt get col request %s error, err:%s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt get col request error", req.StmtID)
	}
	logger.Tracef("stmt get col fields, stmt_id:%d", req.StmtID)
	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		logger.Errorf("stmt has been freed, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	code, colNums, colFields := syncinterface.TaosStmtGetColFields(stmtItem.stmt, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("stmt get col fields error, err:%s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	defer func() {
		wrapper.TaosStmtReclaimFields(stmtItem.stmt, colFields)
	}()
	if colNums == 0 {
		return &StmtGetColFieldsResponse{StmtID: req.StmtID}
	}
	s = log.GetLogNow(isDebug)
	fields := wrapper.StmtParseFields(colNums, colFields)
	logger.Debugf("stmt parse fields cost:%s", log.GetLogDuration(isDebug, s))
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
		logger.Errorf("unmarshal stmt get tags request %s error, err:%s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt get tags request error", req.StmtID)
	}
	logger.Tracef("stmt get tag fields, stmt_id:%d", req.StmtID)
	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		logger.Errorf("stmt has been freed, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	code, tagNums, tagFields := syncinterface.TaosStmtGetTagFields(stmtItem.stmt, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("stmt get tag fields error, err:%s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	defer func() {
		wrapper.TaosStmtReclaimFields(stmtItem.stmt, tagFields)
	}()
	if tagNums == 0 {
		return &StmtGetTagFieldsResponse{StmtID: req.StmtID}
	}
	s = log.GetLogNow(isDebug)
	fields := wrapper.StmtParseFields(tagNums, tagFields)
	logger.Debugf("stmt parse fields cost:%s", log.GetLogDuration(isDebug, s))
	return &StmtGetTagFieldsResponse{StmtID: req.StmtID, Fields: fields}
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
		logger.Errorf("unmarshal stmt use result request %s error, err:%s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt use result request error", req.StmtID)
	}
	logger.Tracef("stmt use result, stmt_id:%d", req.StmtID)
	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		logger.Errorf("stmt has been freed, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	logger.Trace("call stmt use result")
	result := wrapper.TaosStmtUseResult(stmtItem.stmt)
	if result == nil {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("stmt use result error, err:%s", errStr)
		return wsStmtErrorMsg(0xffff, errStr, req.StmtID)
	}

	fieldsCount := wrapper.TaosNumFields(result)
	rowsHeader, _ := wrapper.ReadColumn(result, fieldsCount)
	precision := wrapper.TaosResultPrecision(result)
	logger.Tracef("stmt use result success, stmt_id:%d, fields_count:%d, precision:%d", req.StmtID, fieldsCount, precision)
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
	logger.Tracef("set tags message, stmt_id:%d, columns:%d, rows:%d", req.id, columns, rows)
	if rows != 1 {
		return wsStmtErrorMsg(0xffff, "rows not equal 1", req.id)
	}

	stmtItem := h.stmts.Get(req.id)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.id)
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.id)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		logger.Errorf("stmt has been freed, stmt_id:%d", req.id)
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.id)
	}
	code, tagNums, tagFields := syncinterface.TaosStmtGetTagFields(stmtItem.stmt, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("stmt get tag fields error:%d %s", code, errStr)
		return wsStmtErrorMsg(code, errStr, req.id)
	}
	defer func() {
		wrapper.TaosStmtReclaimFields(stmtItem.stmt, tagFields)
	}()
	if tagNums == 0 {
		logger.Trace("no tags")
		return &StmtSetTagsResponse{StmtID: req.id}
	}
	if int(columns) != tagNums {
		logger.Tracef("stmt tags count not match %d != %d", columns, tagNums)
		return wsStmtErrorMsg(0xffff, "stmt tags count not match", req.id)
	}
	s = log.GetLogNow(isDebug)
	fields := wrapper.StmtParseFields(tagNums, tagFields)
	logger.Debugf("stmt parse fields cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	tags := stmt.BlockConvert(block, int(rows), fields, nil)
	logger.Debugf("block concert cost:%s", log.GetLogDuration(isDebug, s))
	reTags := make([]driver.Value, tagNums)
	for i := 0; i < tagNums; i++ {
		reTags[i] = tags[i][0]
	}
	code = syncinterface.TaosStmtSetTags(stmtItem.stmt, reTags, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("stmt set tags error, code:%d, msg:%s", code, errStr)
		return wsStmtErrorMsg(code, errStr, req.id)
	}

	return &StmtSetTagsResponse{StmtID: req.id}
}

func (h *messageHandler) handleTMQRawMessage(_ context.Context, req dealBinaryRequest, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	length := *(*uint32)(tools.AddPointer(req.p0, uintptr(24)))
	metaType := *(*uint16)(tools.AddPointer(req.p0, uintptr(28)))
	data := tools.AddPointer(req.p0, uintptr(30))
	logger.Tracef("get write raw message, length:%d, metaType:%d", length, metaType)
	logger.Trace("get global lock for raw message")
	h.Lock()
	logger.Debugf("get global lock cost:%s", log.GetLogDuration(isDebug, s))
	defer h.Unlock()
	if h.closed {
		logger.Trace("server closed")
		return
	}
	meta := wrapper.BuildRawMeta(length, metaType, data)
	code := syncinterface.TMQWriteRaw(h.conn, meta, logger, isDebug)
	if code != 0 {
		errStr := wrapper.TMQErr2Str(code)
		logger.Errorf("write raw meta error, code:%d, msg:%s", code, errStr)
		return wsCommonErrorMsg(int(code)&0xffff, errStr)
	}
	logger.Trace("write raw meta success")

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
	logger.Tracef("raw block message, table:%s, rows:%d", tableName, numOfRows)
	s = log.GetLogNow(isDebug)
	h.Lock()
	logger.Debugf("get global lock cost:%s", log.GetLogDuration(isDebug, s))
	defer h.Unlock()
	if h.closed {
		logger.Trace("server closed")
		return
	}
	code := syncinterface.TaosWriteRawBlockWithReqID(h.conn, int(numOfRows), rawBlock, string(tableName), int64(req.reqID), logger, isDebug)
	if code != 0 {
		errStr := wrapper.TMQErr2Str(int32(code))
		logger.Errorf("write raw meta error, code:%d, msg:%s", code, errStr)
		return wsCommonErrorMsg(int(code)&0xffff, errStr)
	}
	logger.Trace("write raw meta success")
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
	logger.Tracef("raw block message with fields, table:%s, rows:%d", tableName, numOfRows)
	s = log.GetLogNow(isDebug)
	h.Lock()
	defer h.Unlock()
	if h.closed {
		logger.Trace("server closed")
		return
	}
	code := syncinterface.TaosWriteRawBlockWithFieldsWithReqID(h.conn, int(numOfRows), rawBlock, string(tableName), fieldsBlock, numOfColumn, int64(req.reqID), logger, isDebug)
	if code != 0 {
		errStr := wrapper.TMQErr2Str(int32(code))
		logger.Errorf("write raw meta error, err:%s", errStr)
		return wsCommonErrorMsg(int(code)&0xffff, errStr)
	}
	logger.Trace("write raw meta success")
	return &BaseResponse{}
}

func (h *messageHandler) handleBinaryQuery(_ context.Context, req dealBinaryRequest, logger *logrus.Entry, isDebug bool, s time.Time) Response {
	message := req.message
	if len(message) < 31 {
		return wsCommonErrorMsg(0xffff, "message length is too short")
	}
	v := binary.LittleEndian.Uint16(message[24:])
	var sql []byte
	if v == BinaryProtocolVersion1 {
		sqlLen := binary.LittleEndian.Uint32(message[26:])
		remainMessageLength := len(message) - 30
		if remainMessageLength < int(sqlLen) {
			return wsCommonErrorMsg(0xffff, fmt.Sprintf("uncompleted message, sql length:%d, remainMessageLength:%d", sqlLen, remainMessageLength))
		}
		sql = message[30 : 30+sqlLen]
	} else {
		logger.Errorf("unknown binary query version:%d", v)
		return wsCommonErrorMsg(0xffff, "unknown binary query version:"+strconv.Itoa(int(v)))
	}
	logger.Debugf("binary query, sql:%s", log.GetLogSql(bytesutil.ToUnsafeString(sql)))
	sqlType := monitor.WSRecordRequest(bytesutil.ToUnsafeString(sql))
	s = log.GetLogNow(isDebug)
	handler := async.GlobalAsync.HandlerPool.Get()
	defer async.GlobalAsync.HandlerPool.Put(handler)
	logger.Debugf("get handler cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	result := async.GlobalAsync.TaosQuery(h.conn, logger, isDebug, bytesutil.ToUnsafeString(sql), handler, int64(req.reqID))
	logger.Debugf("query cost:%s", log.GetLogDuration(isDebug, s))
	code := wrapper.TaosError(result.Res)
	if code != httperror.SUCCESS {
		monitor.WSRecordResult(sqlType, false)
		errStr := wrapper.TaosErrorStr(result.Res)
		logger.Errorf("taos query error, code:%d, msg:%s, sql:%s", code, errStr, log.GetLogSql(bytesutil.ToUnsafeString(sql)))
		syncinterface.FreeResult(result.Res, logger, isDebug)
		return wsCommonErrorMsg(code, errStr)
	}
	monitor.WSRecordResult(sqlType, true)
	s = log.GetLogNow(isDebug)
	isUpdate := wrapper.TaosIsUpdateQuery(result.Res)
	logger.Debugf("get is_update_query %t, cost:%s", isUpdate, log.GetLogDuration(isDebug, s))
	if isUpdate {
		affectRows := wrapper.TaosAffectedRows(result.Res)
		logger.Debugf("affected_rows %d cost:%s", affectRows, log.GetLogDuration(isDebug, s))
		syncinterface.FreeResult(result.Res, logger, isDebug)
		return &QueryResponse{IsUpdate: true, AffectedRows: affectRows}
	}
	s = log.GetLogNow(isDebug)
	fieldsCount := wrapper.TaosNumFields(result.Res)
	logger.Debugf("num_fields cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	rowsHeader, _ := wrapper.ReadColumn(result.Res, fieldsCount)
	s = log.GetLogNow(isDebug)
	logger.Debugf("read column cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	precision := wrapper.TaosResultPrecision(result.Res)
	logger.Debugf("result_precision cost:%s", log.GetLogDuration(isDebug, s))
	queryResult := QueryResult{TaosResult: result.Res, FieldsCount: fieldsCount, Header: rowsHeader, precision: precision}
	idx := h.queryResults.Add(&queryResult)
	logger.Trace("query success")
	return &QueryResponse{
		ID:            idx,
		FieldsCount:   fieldsCount,
		FieldsNames:   rowsHeader.ColNames,
		FieldsLengths: rowsHeader.ColLength,
		FieldsTypes:   rowsHeader.ColTypes,
		Precision:     precision,
	}
}

func (h *messageHandler) handleFetchRawBlock(ctx context.Context, req dealBinaryRequest, logger *logrus.Entry, isDebug bool, s time.Time) Response {
	message := req.message
	if len(message) < 26 {
		return wsFetchRawBlockErrorMsg(0xffff, "message length is too short", req.reqID, req.id, uint64(wstool.GetDuration(ctx)))
	}
	v := binary.LittleEndian.Uint16(message[24:])
	if v != BinaryProtocolVersion1 {
		return wsFetchRawBlockErrorMsg(0xffff, "unknown fetch raw block version", req.reqID, req.id, uint64(wstool.GetDuration(ctx)))
	}
	item := h.queryResults.Get(req.id)
	logger.Tracef("fetch raw block, result_id:%d", req.id)
	if item == nil {
		logger.Errorf("result is nil, result_id:%d", req.id)
		return wsFetchRawBlockErrorMsg(0xffff, "result is nil", req.reqID, req.id, uint64(wstool.GetDuration(ctx)))
	}
	item.Lock()
	if item.TaosResult == nil {
		item.Unlock()
		logger.Errorf("result has been freed, result_id:%d", req.id)
		return wsFetchRawBlockErrorMsg(0xffff, "result has been freed", req.reqID, req.id, uint64(wstool.GetDuration(ctx)))
	}
	s = log.GetLogNow(isDebug)
	handler := async.GlobalAsync.HandlerPool.Get()
	defer async.GlobalAsync.HandlerPool.Put(handler)
	logger.Debugf("get handler cost:%s", log.GetLogDuration(isDebug, s))
	result := async.GlobalAsync.TaosFetchRawBlockA(item.TaosResult, logger, isDebug, handler)
	if result.N == 0 {
		logger.Trace("fetch raw block success")
		item.Unlock()
		h.queryResults.FreeResultByID(req.id, logger)
		return wsFetchRawBlockFinish(req.reqID, req.id, uint64(wstool.GetDuration(ctx)))
	}
	if result.N < 0 {
		item.Unlock()
		errStr := wrapper.TaosErrorStr(result.Res)
		logger.Errorf("fetch raw block error:%d %s", result.N, errStr)
		h.queryResults.FreeResultByID(req.id, logger)
		return wsFetchRawBlockErrorMsg(result.N, errStr, req.reqID, req.id, uint64(wstool.GetDuration(ctx)))
	}
	logger.Trace("call taos_get_raw_block")
	s = log.GetLogNow(isDebug)
	item.Block = wrapper.TaosGetRawBlock(item.TaosResult)
	logger.Debugf("get_raw_block cost:%s", log.GetLogDuration(isDebug, s))
	item.Size = result.N
	s = log.GetLogNow(isDebug)
	blockLength := int(parser.RawBlockGetLength(item.Block))
	if blockLength <= 0 {
		item.Unlock()
		return wsFetchRawBlockErrorMsg(0xffff, "block length illegal", req.reqID, req.id, uint64(wstool.GetDuration(ctx)))
	}
	item.buf = wsFetchRawBlockMessage(item.buf, req.reqID, req.id, uint64(wstool.GetDuration(ctx)), int32(blockLength), item.Block)
	logger.Debugf("handle binary content cost:%s", log.GetLogDuration(isDebug, s))
	resp := &BinaryResponse{Data: item.buf}
	resp.SetBinary(true)
	item.Unlock()
	logger.Trace("fetch raw block success")
	return resp
}

type Stmt2BindResponse struct {
	BaseResponse
	StmtID uint64 `json:"stmt_id"`
}

func (h *messageHandler) handleStmt2Bind(ctx context.Context, req dealBinaryRequest, logger *logrus.Entry, isDebug bool, s time.Time) Response {
	message := req.message
	if len(message) < 30 {
		return wsStmtErrorMsg(0xffff, "message length is too short", req.id)
	}
	v := binary.LittleEndian.Uint16(message[24:])
	if v != Stmt2BindProtocolVersion1 {
		return wsStmtErrorMsg(0xffff, "unknown stmt2 bind version", req.id)
	}
	colIndex := int32(binary.LittleEndian.Uint32(message[26:]))
	stmtItem := h.stmts.GetStmt2(req.id)
	if stmtItem == nil {
		logger.Errorf("stmt2 is nil, stmt_id:%d", req.id)
		return wsStmtErrorMsg(0xffff, "stmt2 is nil", req.id)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		logger.Errorf("stmt2 has been freed, stmt_id:%d", req.id)
		return wsStmtErrorMsg(0xffff, "stmt2 has been freed", req.id)
	}
	bindData := message[30:]
	err := syncinterface.TaosStmt2BindBinary(stmtItem.stmt, bindData, colIndex, logger, isDebug)
	if err != nil {
		logger.Errorf("stmt2 bind error, err:%s", err.Error())
		var tError *errors2.TaosError
		if errors.As(err, &tError) {
			return wsStmtErrorMsg(int(tError.Code), tError.ErrStr, req.id)
		}
		return wsStmtErrorMsg(0xffff, err.Error(), req.id)
	}
	logger.Trace("stmt2 bind success")
	return &Stmt2BindResponse{StmtID: req.id}
}

type GetCurrentDBResponse struct {
	BaseResponse
	DB string `json:"db"`
}

func (h *messageHandler) handleGetCurrentDB(_ context.Context, _ Request, logger *logrus.Entry, isDebug bool, _ time.Time) (resp Response) {
	db, err := syncinterface.TaosGetCurrentDB(h.conn, logger, isDebug)
	if err != nil {
		var taosErr *errors2.TaosError
		errors.As(err, &taosErr)
		logger.Errorf("get current db error, err:%s", taosErr.Error())
		return wsCommonErrorMsg(int(taosErr.Code), taosErr.Error())
	}
	return &GetCurrentDBResponse{DB: db}
}

type GetServerInfoResponse struct {
	BaseResponse
	Info string `json:"info"`
}

func (h *messageHandler) handleGetServerInfo(_ context.Context, _ Request, logger *logrus.Entry, isDebug bool, _ time.Time) (resp Response) {
	serverInfo := syncinterface.TaosGetServerInfo(h.conn, logger, isDebug)
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

func (h *messageHandler) handleNumFields(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, _ time.Time) (resp Response) {
	var req NumFieldsRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("unmarshal stmt num params request %s error, err:%s", request.Args, err)
		return wsCommonErrorMsg(0xffff, "unmarshal stmt num params request error")
	}
	logger.Tracef("num fields, result_id:%d", req.ResultID)
	item := h.queryResults.Get(req.ResultID)
	if item == nil {
		logger.Errorf("result is nil, result_id:%d", req.ResultID)
		return wsCommonErrorMsg(0xffff, "result is nil")
	}
	item.Lock()
	defer item.Unlock()
	if item.TaosResult == nil {
		logger.Errorf("result has been freed, result_id:%d", req.ResultID)
		return wsCommonErrorMsg(0xffff, "result has been freed")
	}
	num := wrapper.TaosNumFields(item.TaosResult)
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

func (h *messageHandler) handleStmtNumParams(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, _ time.Time) (resp Response) {
	var req StmtNumParamsRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("unmarshal stmt num params request %s error, err:%s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt num params request error", req.StmtID)
	}
	logger.Tracef("stmt num params, stmt_id:%d", req.StmtID)
	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		logger.Errorf("stmt has been freed, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	count, code := syncinterface.TaosStmtNumParams(stmtItem.stmt, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("stmt get col fields error, err:%s", errStr)
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

func (h *messageHandler) handleStmtGetParam(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, _ time.Time) (resp Response) {
	var req StmtGetParamRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("unmarshal stmt get param request %s error, err:%s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt get param request error", req.StmtID)
	}
	logger.Tracef("stmt get param, stmt_id:%d, index:%d", req.StmtID, req.Index)

	stmtItem := h.stmts.Get(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		logger.Errorf("stmt has been freed, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	dataType, length, err := syncinterface.TaosStmtGetParam(stmtItem.stmt, req.Index, logger, isDebug)
	if err != nil {
		var taosErr *errors2.TaosError
		errors.As(err, &taosErr)
		logger.Errorf("stmt get param error, err:%s", taosErr.Error())
		return wsStmtErrorMsg(int(taosErr.Code), taosErr.Error(), req.StmtID)
	}
	logger.Tracef("stmt get param success, data_type:%d, length:%d", dataType, length)
	return &StmtGetParamResponse{StmtID: req.StmtID, Index: req.Index, DataType: dataType, Length: length}
}

type Stmt2InitRequest struct {
	ReqID               uint64 `json:"req_id"`
	SingleStbInsert     bool   `json:"single_stb_insert"`
	SingleTableBindOnce bool   `json:"single_table_bind_once"`
}

type Stmt2InitResponse struct {
	BaseResponse
	StmtID uint64 `json:"stmt_id"`
}

func (h *messageHandler) handleStmt2Init(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, _ time.Time) (resp Response) {
	var req Stmt2InitRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("unmarshal stmt2 init request %s error, err:%s", request.Args, err)
		return wsCommonErrorMsg(0xffff, "unmarshal stmt2 init request error")
	}
	handle, caller := async.GlobalStmt2CallBackCallerPool.Get()
	stmtInit := syncinterface.TaosStmt2Init(h.conn, int64(req.ReqID), req.SingleStbInsert, req.SingleTableBindOnce, handle, logger, isDebug)
	if stmtInit == nil {
		async.GlobalStmt2CallBackCallerPool.Put(handle)
		errStr := wrapper.TaosStmtErrStr(stmtInit)
		logger.Errorf("stmt2 init error, err:%s", errStr)
		return wsCommonErrorMsg(0xffff, errStr)
	}
	stmtItem := &StmtItem{stmt: stmtInit, handler: handle, caller: caller, isStmt2: true}
	h.stmts.Add(stmtItem)
	logger.Tracef("stmt2 init sucess, stmt_id:%d, stmt pointer:%p", stmtItem.index, stmtInit)
	return &StmtInitResponse{StmtID: stmtItem.index}
}

type Stmt2PrepareRequest struct {
	ReqID     uint64 `json:"req_id"`
	StmtID    uint64 `json:"stmt_id"`
	SQL       string `json:"sql"`
	GetFields bool   `json:"get_fields"`
}

type PrepareFields struct {
	stmtCommon.StmtField
	BindType int8
}

type Stmt2PrepareResponse struct {
	BaseResponse
	StmtID      uint64           `json:"stmt_id"`
	IsInsert    bool             `json:"is_insert"`
	Fields      []*PrepareFields `json:"fields"`
	FieldsCount int              `json:"fields_count"`
}

func (h *messageHandler) handleStmt2Prepare(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, s time.Time) Response {
	var req Stmt2PrepareRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("unmarshal stmt2 prepare request %s error, err:%s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal connect request error", req.StmtID)
	}
	logger.Debugf("stmt2 prepare, stmt_id:%d, sql:%s", req.StmtID, req.SQL)
	stmtItem := h.stmts.GetStmt2(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt2 is nil, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt2 is nil", req.StmtID)
	}
	s = log.GetLogNow(isDebug)
	logger.Trace("get stmt2 lock")
	stmtItem.Lock()
	logger.Debugf("get stmt2 lock cost:%s", log.GetLogDuration(isDebug, s))
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		logger.Errorf("stmt2 has been freed, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	stmt2 := stmtItem.stmt
	code := syncinterface.TaosStmt2Prepare(stmt2, req.SQL, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmt2Error(stmt2)
		logger.Errorf("stmt2 prepare error, err:%s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	logger.Tracef("stmt2 prepare success, stmt_id:%d", req.StmtID)
	isInsert, code := syncinterface.TaosStmt2IsInsert(stmt2, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmt2Error(stmt2)
		logger.Errorf("check stmt2 is insert error, err:%s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	logger.Tracef("stmt2 is insert:%t", isInsert)
	stmtItem.isInsert = isInsert
	prepareResp := &Stmt2PrepareResponse{StmtID: req.StmtID, IsInsert: isInsert}
	if req.GetFields {
		if isInsert {
			var prepareFields []*PrepareFields
			// get table field
			_, count, code, errStr := getFields(stmt2, stmtCommon.TAOS_FIELD_TBNAME, logger, isDebug)
			if code != 0 {
				return wsStmtErrorMsg(code, fmt.Sprintf("get table names fields error, %s", errStr), req.StmtID)
			}
			if count == 1 {
				tableNameFields := &PrepareFields{
					StmtField: stmtCommon.StmtField{},
					BindType:  stmtCommon.TAOS_FIELD_TBNAME,
				}
				prepareFields = append(prepareFields, tableNameFields)
			}
			// get tags field
			tagFields, _, code, errStr := getFields(stmt2, stmtCommon.TAOS_FIELD_TAG, logger, isDebug)
			if code != 0 {
				return wsStmtErrorMsg(code, fmt.Sprintf("get tag fields error, %s", errStr), req.StmtID)
			}
			for i := 0; i < len(tagFields); i++ {
				prepareFields = append(prepareFields, &PrepareFields{StmtField: *tagFields[i], BindType: stmtCommon.TAOS_FIELD_TAG})
			}
			// get cols field
			colFields, _, code, errStr := getFields(stmt2, stmtCommon.TAOS_FIELD_COL, logger, isDebug)
			if code != 0 {
				return wsStmtErrorMsg(code, fmt.Sprintf("get col fields error, %s", errStr), req.StmtID)
			}
			for i := 0; i < len(colFields); i++ {
				prepareFields = append(prepareFields, &PrepareFields{StmtField: *colFields[i], BindType: stmtCommon.TAOS_FIELD_COL})
			}
			prepareResp.Fields = prepareFields
		} else {
			_, count, code, errStr := getFields(stmt2, stmtCommon.TAOS_FIELD_QUERY, logger, isDebug)
			if code != 0 {
				return wsStmtErrorMsg(code, fmt.Sprintf("get query fields error, %s", errStr), req.StmtID)
			}
			prepareResp.FieldsCount = count
		}
	}
	return prepareResp
}

func getFields(stmt2 unsafe.Pointer, fieldType int8, logger *logrus.Entry, isDebug bool) (fields []*stmtCommon.StmtField, count int, code int, errSt string) {
	var cFields unsafe.Pointer
	code, count, cFields = syncinterface.TaosStmt2GetFields(stmt2, int(fieldType), logger, isDebug)
	if code != 0 {
		errStr := wrapper.TaosStmt2Error(stmt2)
		logger.Errorf("stmt2 get fields error, field_type:%d, err:%s", fieldType, errStr)
		return nil, count, code, errStr
	}
	defer wrapper.TaosStmt2FreeFields(stmt2, cFields)
	if count > 0 && cFields != nil {
		s := log.GetLogNow(isDebug)
		fields = wrapper.StmtParseFields(count, cFields)
		logger.Debugf("stmt2 parse fields cost:%s", log.GetLogDuration(isDebug, s))
		return fields, count, 0, ""
	}
	return nil, count, 0, ""
}

type Stmt2GetFieldsRequest struct {
	ReqID      uint64 `json:"req_id"`
	StmtID     uint64 `json:"stmt_id"`
	FieldTypes []int8 `json:"field_types"`
}

type Stmt2GetFieldsResponse struct {
	BaseResponse
	StmtID     uint64                  `json:"stmt_id"`
	TableCount int32                   `json:"table_count"`
	QueryCount int32                   `json:"query_count"`
	ColFields  []*stmtCommon.StmtField `json:"col_fields"`
	TagFields  []*stmtCommon.StmtField `json:"tag_fields"`
}

func (h *messageHandler) handleStmt2GetFields(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	var req Stmt2GetFieldsRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("unmarshal stmt2 get fields request %s error, err:%s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt get fields request error", req.StmtID)
	}
	logger.Tracef("stmt2 get col fields, stmt_id:%d", req.StmtID)
	stmtItem := h.stmts.GetStmt2(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt2 is nil, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		logger.Errorf("stmt2 has been freed, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	stmt2GetFieldsResp := &Stmt2GetFieldsResponse{StmtID: req.StmtID}
	for i := 0; i < len(req.FieldTypes); i++ {
		switch req.FieldTypes[i] {
		case stmtCommon.TAOS_FIELD_COL:
			colFields, _, code, errStr := getFields(stmtItem.stmt, stmtCommon.TAOS_FIELD_COL, logger, isDebug)
			if code != 0 {
				return wsStmtErrorMsg(code, fmt.Sprintf("get col fields error, %s", errStr), req.StmtID)
			}
			stmt2GetFieldsResp.ColFields = colFields
		case stmtCommon.TAOS_FIELD_TAG:
			tagFields, _, code, errStr := getFields(stmtItem.stmt, stmtCommon.TAOS_FIELD_TAG, logger, isDebug)
			if code != 0 {
				return wsStmtErrorMsg(code, fmt.Sprintf("get tag fields error, %s", errStr), req.StmtID)
			}
			stmt2GetFieldsResp.TagFields = tagFields
		case stmtCommon.TAOS_FIELD_TBNAME:
			_, count, code, errStr := getFields(stmtItem.stmt, stmtCommon.TAOS_FIELD_TBNAME, logger, isDebug)
			if code != 0 {
				return wsStmtErrorMsg(code, fmt.Sprintf("get table names fields error, %s", errStr), req.StmtID)
			}
			stmt2GetFieldsResp.TableCount = int32(count)
		case stmtCommon.TAOS_FIELD_QUERY:
			_, count, code, errStr := getFields(stmtItem.stmt, stmtCommon.TAOS_FIELD_QUERY, logger, isDebug)
			if code != 0 {
				return wsStmtErrorMsg(code, fmt.Sprintf("get query fields error, %s", errStr), req.StmtID)
			}
			stmt2GetFieldsResp.QueryCount = int32(count)
		}
	}
	return stmt2GetFieldsResp
}

type Stmt2ExecRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type Stmt2ExecResponse struct {
	BaseResponse
	StmtID   uint64 `json:"stmt_id"`
	Affected int    `json:"affected"`
}

func (h *messageHandler) handleStmt2Exec(_ context.Context, request Request, logger *logrus.Entry, isDebug bool, s time.Time) (resp Response) {
	var req Stmt2ExecRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("unmarshal stmt2 exec request %s error, err:%s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt2 exec request error", req.StmtID)
	}
	logger.Tracef("stmt2 execute, stmt_id:%d", req.StmtID)
	stmtItem := h.stmts.GetStmt2(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt2 is nil, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt2 is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		logger.Errorf("stmt2 has been freed, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt has been freed", req.StmtID)
	}
	code := syncinterface.TaosStmt2Exec(stmtItem.stmt, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("stmt2 execute error, err:%s", errStr)
		return wsStmtErrorMsg(code, errStr, req.StmtID)
	}
	s = log.GetLogNow(isDebug)
	logger.Tracef("stmt2 execute wait callback, stmt_id:%d", req.StmtID)
	result := <-stmtItem.caller.ExecResult
	logger.Debugf("stmt2 execute wait callback finish, affected:%d, res:%p, n:%d, cost:%s", result.Affected, result.Res, result.N, log.GetLogDuration(isDebug, s))
	stmtItem.result = result.Res
	return &Stmt2ExecResponse{StmtID: req.StmtID, Affected: result.Affected}
}

type Stmt2CloseRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type Stmt2CloseResponse struct {
	BaseResponse
	StmtID uint64 `json:"stmt_id"`
}

func (h *messageHandler) handleStmt2Close(_ context.Context, request Request, logger *logrus.Entry, _ bool, _ time.Time) (resp Response) {
	var req StmtCloseRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("unmarshal stmt close request %s error, err:%s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt close request error", req.StmtID)
	}
	logger.Tracef("stmt2 close, stmt_id:%d", req.StmtID)
	err := h.stmts.FreeStmtByID(req.StmtID, true, logger)
	if err != nil {
		logger.Errorf("stmt2 close error, err:%s", err.Error())
		return wsStmtErrorMsg(0xffff, "unmarshal stmt close request error", req.StmtID)
	}
	resp = &Stmt2CloseResponse{StmtID: req.StmtID}
	logger.Tracef("stmt2 close success, stmt_id:%d", req.StmtID)
	return resp
}

type Stmt2UseResultRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type Stmt2UseResultResponse struct {
	BaseResponse
	StmtID        uint64             `json:"stmt_id"`
	ResultID      uint64             `json:"result_id"`
	FieldsCount   int                `json:"fields_count"`
	FieldsNames   []string           `json:"fields_names"`
	FieldsTypes   jsontype.JsonUint8 `json:"fields_types"`
	FieldsLengths []int64            `json:"fields_lengths"`
	Precision     int                `json:"precision"`
}

func (h *messageHandler) handleStmt2UseResult(_ context.Context, request Request, logger *logrus.Entry, _ bool, _ time.Time) (resp Response) {
	var req Stmt2UseResultRequest
	if err := json.Unmarshal(request.Args, &req); err != nil {
		logger.Errorf("unmarshal stmt2 use result request %s error, err:%s", request.Args, err)
		return wsStmtErrorMsg(0xffff, "unmarshal stmt2 use result request error", req.StmtID)
	}
	logger.Tracef("stmt2 use result, stmt_id:%d", req.StmtID)
	stmtItem := h.stmts.GetStmt2(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt2 is nil, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt2 is nil", req.StmtID)
	}
	stmtItem.Lock()
	defer stmtItem.Unlock()
	if stmtItem.stmt == nil {
		logger.Errorf("stmt2 has been freed, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt2 has been freed", req.StmtID)
	}

	if stmtItem.result == nil {
		logger.Errorf("stmt2 result is nil, stmt_id:%d", req.StmtID)
		return wsStmtErrorMsg(0xffff, "stmt result is nil", req.StmtID)
	}
	result := stmtItem.result
	fieldsCount := wrapper.TaosNumFields(result)
	rowsHeader, _ := wrapper.ReadColumn(result, fieldsCount)
	precision := wrapper.TaosResultPrecision(result)
	logger.Tracef("stmt use result success, stmt_id:%d, fields_count:%d, precision:%d", req.StmtID, fieldsCount, precision)
	queryResult := QueryResult{TaosResult: result, FieldsCount: fieldsCount, Header: rowsHeader, precision: precision, inStmt: true}
	idx := h.queryResults.Add(&queryResult)

	return &Stmt2UseResultResponse{
		StmtID:        req.StmtID,
		ResultID:      idx,
		FieldsCount:   fieldsCount,
		FieldsNames:   rowsHeader.ColNames,
		FieldsTypes:   rowsHeader.ColTypes,
		FieldsLengths: rowsHeader.ColLength,
		Precision:     precision,
	}
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

func wsFetchRawBlockErrorMsg(code int, message string, reqID uint64, resultID uint64, t uint64) *BinaryResponse {
	bufLength := 8 + 8 + 2 + 8 + 8 + 4 + 4 + len(message) + 8 + 1
	buf := make([]byte, bufLength)
	binary.LittleEndian.PutUint64(buf, 0xffffffffffffffff)
	binary.LittleEndian.PutUint64(buf[8:], uint64(FetchRawBlockMessage))
	binary.LittleEndian.PutUint16(buf[16:], 1)
	binary.LittleEndian.PutUint64(buf[18:], t)
	binary.LittleEndian.PutUint64(buf[26:], reqID)
	binary.LittleEndian.PutUint32(buf[34:], uint32(code&0xffff))
	binary.LittleEndian.PutUint32(buf[38:], uint32(len(message)))
	copy(buf[42:], message)
	binary.LittleEndian.PutUint64(buf[42+len(message):], resultID)
	buf[42+len(message)+8] = 1
	resp := &BinaryResponse{Data: buf}
	resp.SetBinary(true)
	return resp
}

func wsFetchRawBlockFinish(reqID uint64, resultID uint64, t uint64) *BinaryResponse {
	bufLength := 8 + 8 + 2 + 8 + 8 + 4 + 4 + 8 + 1
	buf := make([]byte, bufLength)
	binary.LittleEndian.PutUint64(buf, 0xffffffffffffffff)
	binary.LittleEndian.PutUint64(buf[8:], uint64(FetchRawBlockMessage))
	binary.LittleEndian.PutUint16(buf[16:], 1)
	binary.LittleEndian.PutUint64(buf[18:], t)
	binary.LittleEndian.PutUint64(buf[26:], reqID)
	binary.LittleEndian.PutUint32(buf[34:], 0)
	binary.LittleEndian.PutUint32(buf[38:], 0)
	binary.LittleEndian.PutUint64(buf[42:], resultID)
	buf[50] = 1
	resp := &BinaryResponse{Data: buf}
	resp.SetBinary(true)
	return resp
}

func wsFetchRawBlockMessage(buf []byte, reqID uint64, resultID uint64, t uint64, blockLength int32, rawBlock unsafe.Pointer) []byte {
	bufLength := 8 + 8 + 2 + 8 + 8 + 4 + 4 + 8 + 1 + 4 + int(blockLength)
	if cap(buf) < bufLength {
		buf = make([]byte, 0, bufLength)
	}
	buf = buf[:bufLength]
	binary.LittleEndian.PutUint64(buf, 0xffffffffffffffff)
	binary.LittleEndian.PutUint64(buf[8:], uint64(FetchRawBlockMessage))
	binary.LittleEndian.PutUint16(buf[16:], 1)
	binary.LittleEndian.PutUint64(buf[18:], t)
	binary.LittleEndian.PutUint64(buf[26:], reqID)
	binary.LittleEndian.PutUint32(buf[34:], 0)
	binary.LittleEndian.PutUint32(buf[38:], 0)
	binary.LittleEndian.PutUint64(buf[42:], resultID)
	buf[50] = 0
	binary.LittleEndian.PutUint32(buf[51:], uint32(blockLength))
	bytesutil.Copy(rawBlock, buf, 55, int(blockLength))
	return buf
}

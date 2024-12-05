package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

type messageHandler struct {
	conn         unsafe.Pointer
	logger       *logrus.Entry
	closed       uint32
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
			if h.isClosed() {
				logger.Trace("server closed")
				h.Unlock()
				return
			}
			logger.Info("user dropped, close connection")
			h.signalExit(logger, isDebug)
			return
		case <-h.whitelistChangeChan:
			logger.Info("get whitelist change signal")
			isDebug := log.IsDebug()
			h.lock(logger, isDebug)
			if h.isClosed() {
				logger.Trace("server closed")
				h.Unlock()
				return
			}
			logger.Trace("get whitelist")
			whitelist, err := tool.GetWhitelist(h.conn)
			if err != nil {
				logger.Errorf("get whitelist error, close connection, err:%s", err)
				h.signalExit(logger, isDebug)
				return
			}
			logger.Tracef("check whitelist, ip:%s, whitelist:%s", h.ipStr, tool.IpNetSliceToString(whitelist))
			valid := tool.CheckWhitelist(whitelist, h.ip)
			if !valid {
				logger.Errorf("ip not in whitelist! close connection, ip:%s, whitelist:%s", h.ipStr, tool.IpNetSliceToString(whitelist))
				h.signalExit(logger, isDebug)
				return
			}
			h.Unlock()
		case <-h.exit:
			return
		}
	}
}

func (h *messageHandler) isClosed() bool {
	return atomic.LoadUint32(&h.closed) == 1
}

func (h *messageHandler) setClosed() {
	atomic.StoreUint32(&h.closed, 1)
}

func (h *messageHandler) signalExit(logger *logrus.Entry, isDebug bool) {
	logger.Trace("close session")
	s := log.GetLogNow(isDebug)
	_ = h.session.Close()
	logger.Debugf("close session cost:%s", log.GetLogDuration(isDebug, s))
	h.Unlock()
	logger.Trace("close handler")
	s = log.GetLogNow(isDebug)
	h.Close()
	logger.Debugf("close handler cost:%s", log.GetLogDuration(isDebug, s))
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

	if h.isClosed() {
		h.logger.Trace("server closed")
		return
	}
	h.setClosed()
	h.stop()
	close(h.exit)
}

type Request struct {
	Action string          `json:"action"`
	Args   json.RawMessage `json:"args"`
}

func (h *messageHandler) stop() {
	h.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		waitCh := make(chan struct{})
		go func() {
			h.wait.Wait()
			close(waitCh)
		}()

		select {
		case <-ctx.Done():
			h.logger.Warn("wait stop over 1 minute")
			<-waitCh
			break
		case <-waitCh:
		}
		h.logger.Debugf("wait stop done")
		// clean query result and stmt
		h.queryResults.FreeAll(h.logger)
		h.stmts.FreeAll(h.logger)
		// clean connection
		if h.conn != nil {
			syncinterface.TaosClose(h.conn, h.logger, log.IsDebug())
		}
	})
}

func (h *messageHandler) handleMessage(session *melody.Session, data []byte) {
	ctx := context.WithValue(context.Background(), wstool.StartTimeKey, time.Now().UnixNano())
	h.logger.Debugf("get ws message data:%s", data)
	var request Request
	err := json.Unmarshal(data, &request)
	if err != nil {
		h.logger.Errorf("unmarshal request error, request:%s, err:%s", data, err)
		commonErrorResponse(ctx, session, h.logger, "", 0, 0xffff, "unmarshal request error")
		return
	}
	action := request.Action
	if request.Action == "" {
		reqID := getReqID(request.Args)
		commonErrorResponse(ctx, session, h.logger, "", reqID, 0xffff, "request no action")
		return
	}

	// no need connection actions
	switch request.Action {
	case wstool.ClientVersion:
		wstool.WSWriteVersion(session, h.logger)
		return
	case Connect:
		action = Connect
		var req connRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal connect request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, Connect, reqID, 0xffff, "unmarshal connect request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.connect(ctx, session, action, req, logger, log.IsDebug())
		return
	}

	// check connection
	if h.conn == nil {
		h.logger.Errorf("server not connected")
		reqID := getReqID(request.Args)
		commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "server not connected")
		return
	}

	// need connection actions
	switch action {
	// query
	case WSQuery:
		action = WSQuery
		var req queryRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal query request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal query request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.query(ctx, session, action, req, logger, log.IsDebug())
	case WSFetch:
		action = WSFetch
		var req fetchRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal fetch request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal fetch request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.fetch(ctx, session, action, req, logger, log.IsDebug())
	case WSFetchBlock:
		action = WSFetchBlock
		var req fetchBlockRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal fetch block request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal fetch block request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.fetchBlock(ctx, session, action, req, logger, log.IsDebug())
	case WSFreeResult:
		action = WSFreeResult
		var req freeResultRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal free result request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal free result request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.freeResult(req, logger)
	case WSNumFields:
		action = WSNumFields
		var req numFieldsRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal num fields request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal num fields request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.numFields(ctx, session, action, req, logger, log.IsDebug())
	// schemaless
	case SchemalessWrite:
		action = SchemalessWrite
		var req schemalessWriteRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal schemaless insert request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal schemaless insert request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.schemalessWrite(ctx, session, action, req, logger, log.IsDebug())
	// stmt
	case STMTInit:
		action = STMTInit
		var req stmtInitRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal stmt init request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal stmt init request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.stmtInit(ctx, session, action, req, logger, log.IsDebug())
	case STMTPrepare:
		action = STMTPrepare
		var req stmtPrepareRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal stmt prepare request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal stmt prepare request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.stmtPrepare(ctx, session, action, req, logger, log.IsDebug())
	case STMTSetTableName:
		action = STMTSetTableName
		var req stmtSetTableNameRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal stmt set table name request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal stmt set table name request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.stmtSetTableName(ctx, session, action, req, logger, log.IsDebug())
	case STMTSetTags:
		action = STMTSetTags
		var req stmtSetTagsRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal stmt set tags request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal stmt set tags request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.stmtSetTags(ctx, session, action, req, logger, log.IsDebug())
	case STMTBind:
		action = STMTBind
		var req stmtBindRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal stmt bind request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal stmt bind request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.stmtBind(ctx, session, action, req, logger, log.IsDebug())
	case STMTAddBatch:
		action = STMTAddBatch
		var req stmtAddBatchRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal stmt add batch request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal stmt add batch request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.stmtAddBatch(ctx, session, action, req, logger, log.IsDebug())
	case STMTExec:
		action = STMTExec
		var req stmtExecRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal stmt exec request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal stmt exec request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.stmtExec(ctx, session, action, req, logger, log.IsDebug())
	case STMTClose:
		action = STMTClose
		var req stmtCloseRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal stmt close request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal stmt close request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.stmtClose(ctx, session, action, req, logger)
	case STMTGetTagFields:
		action = STMTGetTagFields
		var req stmtGetTagFieldsRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal stmt get tag fields request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal stmt get tag fields request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.stmtGetTagFields(ctx, session, action, req, logger, log.IsDebug())
	case STMTGetColFields:
		action = STMTGetColFields
		var req stmtGetColFieldsRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal stmt get col fields request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal stmt get col fields request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.stmtGetColFields(ctx, session, action, req, logger, log.IsDebug())
	case STMTUseResult:
		action = STMTUseResult
		var req stmtUseResultRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal stmt use result request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal stmt use result request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.stmtUseResult(ctx, session, action, req, logger, log.IsDebug())
	case STMTNumParams:
		action = STMTNumParams
		var req stmtNumParamsRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal stmt num params request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal stmt num params request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.stmtNumParams(ctx, session, action, req, logger, log.IsDebug())
	case STMTGetParam:
		action = STMTGetParam
		var req stmtGetParamRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal stmt get param request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal stmt get param request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.stmtGetParam(ctx, session, action, req, logger, log.IsDebug())
	// stmt2
	case STMT2Init:
		action = STMT2Init
		var req stmt2InitRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal stmt2 init request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal stmt2 init request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.stmt2Init(ctx, session, action, req, logger, log.IsDebug())
	case STMT2Prepare:
		action = STMT2Prepare
		var req stmt2PrepareRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal stmt2 prepare request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal stmt2 prepare request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.stmt2Prepare(ctx, session, action, req, logger, log.IsDebug())
	case STMT2Exec:
		action = STMT2Exec
		var req stmt2ExecRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal stmt2 exec request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal stmt2 exec request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.stmt2Exec(ctx, session, action, req, logger, log.IsDebug())
	case STMT2Result:
		action = STMT2Result
		var req stmt2UseResultRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal stmt2 result request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal stmt2 result request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.stmt2UseResult(ctx, session, action, req, logger, log.IsDebug())
	case STMT2Close:
		action = STMT2Close
		var req stmt2CloseRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal stmt2 close request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal stmt2 close request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.stmt2Close(ctx, session, action, req, logger)
	// misc
	case WSGetCurrentDB:
		action = WSGetCurrentDB
		var req getCurrentDBRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal get current db request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal get current db request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.getCurrentDB(ctx, session, action, req, logger, log.IsDebug())
	case WSGetServerInfo:
		action = WSGetServerInfo
		var req getServerInfoRequest
		if err := json.Unmarshal(request.Args, &req); err != nil {
			h.logger.Errorf("unmarshal get server info request error, request:%s, err:%s", request.Args, err)
			reqID := getReqID(request.Args)
			commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, "unmarshal get server info request error")
			return
		}
		logger := h.logger.WithFields(logrus.Fields{
			actionKey:       action,
			config.ReqIDKey: req.ReqID,
		})
		h.getServerInfo(ctx, session, action, req, logger, log.IsDebug())
	default:
		h.logger.Errorf("unknown action %s", action)
		reqID := getReqID(request.Args)
		commonErrorResponse(ctx, session, h.logger, action, reqID, 0xffff, fmt.Sprintf("unknown action %s", action))
	}
}

func (h *messageHandler) handleMessageBinary(session *melody.Session, message []byte) {
	//p0 uin64  req_id
	//p0+8 uint64  resource_id(result_id or stmt_id)
	//p0+16 uint64 (1 (set tag) 2 (bind))
	h.logger.Tracef("get ws block message data:%+v", message)
	p0 := unsafe.Pointer(&message[0])
	reqID := *(*uint64)(p0)
	resourceID := *(*uint64)(tools.AddPointer(p0, uintptr(8)))
	action := *(*uint64)(tools.AddPointer(p0, uintptr(16)))
	h.logger.Debugf("get ws message binary QID:0x%x, resourceID:%d, action:%d", reqID, resourceID, action)

	ctx := context.WithValue(context.Background(), wstool.StartTimeKey, time.Now().UnixNano())
	actionStr := getActionString(action)
	logger := h.logger.WithField(actionKey, actionStr).WithField(config.ReqIDKey, reqID)

	// check error connection
	if h.conn == nil {
		logger.Errorf("server not connected")
		commonErrorResponse(ctx, session, h.logger, actionStr, reqID, 0xffff, "server not connected")
		return
	}
	switch action {
	case SetTagsMessage:
		h.stmtBinarySetTags(ctx, session, actionStr, reqID, resourceID, message, logger, log.IsDebug())
	case BindMessage:
		h.stmtBinaryBind(ctx, session, actionStr, reqID, resourceID, message, logger, log.IsDebug())
	case TMQRawMessage:
		h.binaryTMQRawMessage(ctx, session, actionStr, reqID, message, logger, log.IsDebug())
	case RawBlockMessage:
		h.binaryRawBlockMessage(ctx, session, actionStr, reqID, message, logger, log.IsDebug())
	case RawBlockMessageWithFields:
		h.binaryRawBlockMessageWithFields(ctx, session, actionStr, reqID, message, logger, log.IsDebug())
	case BinaryQueryMessage:
		h.binaryQuery(ctx, session, actionStr, reqID, message, logger, log.IsDebug())
	case FetchRawBlockMessage:
		h.fetchRawBlock(ctx, session, reqID, resourceID, message, logger, log.IsDebug())
	case Stmt2BindMessage:
		h.stmt2BinaryBind(ctx, session, actionStr, reqID, resourceID, message, logger, log.IsDebug())
	default:
		h.logger.Errorf("unknown binary action %d", action)
		commonErrorResponse(ctx, session, h.logger, actionStr, reqID, 0xffff, fmt.Sprintf("unknown binary action %d", action))
	}
}

var jsonIter = jsoniter.ConfigCompatibleWithStandardLibrary

func getReqID(value json.RawMessage) uint64 {
	return jsonIter.Get(value, "req_id").ToUint64()
}

type VersionResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	Version string `json:"version"`
}

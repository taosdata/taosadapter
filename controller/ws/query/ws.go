package query

import (
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/tools/generator"

	"github.com/gin-gonic/gin"
	"github.com/huskar-t/melody"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v3/common/parser"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
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
)

type QueryController struct {
	queryM *melody.Melody
}

func NewQueryController() *QueryController {
	queryM := melody.New()
	queryM.UpGrader.EnableCompression = true
	queryM.Config.MaxMessageSize = 0

	queryM.HandleConnect(func(session *melody.Session) {
		logger := wstool.GetLogger(session)
		ipAddr := iptool.GetRealIP(session.Request)
		logger.WithField("ip", ipAddr.String()).Debug("ws connect")
		session.Set(TaosSessionKey, NewTaos(session, logger))
	})

	queryM.HandleMessage(func(session *melody.Session, data []byte) {
		if queryM.IsClosed() {
			return
		}
		t := session.MustGet(TaosSessionKey).(*Taos)
		if t.closed {
			return
		}
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			ctx := context.WithValue(context.Background(), wstool.StartTimeKey, time.Now().UnixNano())
			logger := wstool.GetLogger(session)
			logger.Debugf("get ws message data: %s", data)
			var action WSAction
			err := json.Unmarshal(data, &action)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal ws request")
				return
			}
			switch action.Action {
			case wstool.ClientVersion:
				_ = session.Write(wstool.VersionResp)
			case WSConnect:
				var wsConnect WSConnectReq
				err = json.Unmarshal(action.Args, &wsConnect)
				if err != nil {
					logger.WithError(err).Errorln("unmarshal connect request args")
					return
				}
				t.connect(ctx, session, &wsConnect)
			case WSQuery:
				var wsQuery WSQueryReq
				err = json.Unmarshal(action.Args, &wsQuery)
				if err != nil {
					logger.WithError(err).WithField(config.ReqIDKey, wsQuery.ReqID).Errorln("unmarshal query args")
					return
				}
				t.query(ctx, session, &wsQuery)
			case WSFetch:
				var wsFetch WSFetchReq
				err = json.Unmarshal(action.Args, &wsFetch)
				if err != nil {
					logger.WithError(err).WithField(config.ReqIDKey, wsFetch.ReqID).Errorln("unmarshal fetch args")
					return
				}
				t.fetch(ctx, session, &wsFetch)
			case WSFetchBlock:
				var fetchBlock WSFetchBlockReq
				err = json.Unmarshal(action.Args, &fetchBlock)
				if err != nil {
					logger.WithError(err).WithField(config.ReqIDKey, fetchBlock.ReqID).Errorln("unmarshal fetch_block args")
					return
				}
				t.fetchBlock(ctx, session, &fetchBlock)
			case WSFreeResult:
				var fetchJson WSFreeResultReq
				err = json.Unmarshal(action.Args, &fetchJson)
				if err != nil {
					logger.WithError(err).WithField(config.ReqIDKey, fetchJson.ReqID).Errorln("unmarshal fetch_json args")
					return
				}
				t.freeResult(&fetchJson)
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
		t := session.MustGet(TaosSessionKey).(*Taos)
		if t.closed {
			return
		}
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			ctx := context.WithValue(context.Background(), wstool.StartTimeKey, time.Now().UnixNano())
			logger := wstool.GetLogger(session)
			logger.Tracef("get ws block message data:%+v", data)
			p0 := unsafe.Pointer(&data[0])
			reqID := *(*uint64)(p0)
			messageID := *(*uint64)(tools.AddPointer(p0, uintptr(8)))
			action := *(*uint64)(tools.AddPointer(p0, uintptr(16)))
			switch action {
			case TMQRawMessage:
				length := *(*uint32)(tools.AddPointer(p0, uintptr(24)))
				metaType := *(*uint16)(tools.AddPointer(p0, uintptr(28)))
				b := tools.AddPointer(p0, uintptr(30))
				t.writeRaw(ctx, session, reqID, messageID, length, metaType, b)
			case RawBlockMessage:
				numOfRows := *(*int32)(tools.AddPointer(p0, uintptr(24)))
				tableNameLength := *(*uint16)(tools.AddPointer(p0, uintptr(28)))
				tableName := make([]byte, tableNameLength)
				for i := 0; i < int(tableNameLength); i++ {
					tableName[i] = *(*byte)(tools.AddPointer(p0, uintptr(30+i)))
				}
				b := tools.AddPointer(p0, uintptr(30+tableNameLength))
				t.writeRawBlock(ctx, session, reqID, int(numOfRows), string(tableName), b)
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
				t.writeRawBlockWithFields(ctx, session, reqID, int(numOfRows), string(tableName), b, fieldsBlock, numOfColumn)
			}
		}()
	})

	queryM.HandleClose(func(session *melody.Session, i int, s string) error {
		//message := melody.FormatCloseMessage(i, "")
		//session.WriteControl(websocket.CloseMessage, message, time.Now().Add(time.Second))
		logger := wstool.GetLogger(session)
		logger.Debugf("ws close, code:%d, msg %s", i, s)
		t, exist := session.Get(TaosSessionKey)
		if exist && t != nil {
			t.(*Taos).Close()
		}
		return nil
	})

	queryM.HandleError(func(session *melody.Session, err error) {
		wstool.LogWSError(session, err)
		t, exist := session.Get(TaosSessionKey)
		if exist && t != nil {
			t.(*Taos).Close()
		}
	})

	queryM.HandleDisconnect(func(session *melody.Session) {
		logger := wstool.GetLogger(session)
		logger.Debug("ws disconnect")
		t, exist := session.Get(TaosSessionKey)
		if exist && t != nil {
			t.(*Taos).Close()
		}
	})
	return &QueryController{queryM: queryM}
}

func (s *QueryController) Init(ctl gin.IRouter) {
	ctl.GET("rest/ws", func(c *gin.Context) {
		// generate session id
		sessionID := generator.GetSessionID()
		logger := log.GetLogger("TMQ").WithFields(logrus.Fields{
			config.SessionIDKey: sessionID})
		_ = s.queryM.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": logger})
	})
}

type Taos struct {
	conn                  unsafe.Pointer
	resultLocker          sync.RWMutex
	Results               *list.List
	resultIndex           uint64
	logger                *logrus.Entry
	closed                bool
	exit                  chan struct{}
	whitelistChangeChan   chan int64
	dropUserChan          chan struct{}
	session               *melody.Session
	ip                    net.IP
	wg                    sync.WaitGroup
	ipStr                 string
	whitelistChangeHandle cgo.Handle
	dropUserHandle        cgo.Handle
	sync.Mutex
}

func NewTaos(session *melody.Session, logger *logrus.Entry) *Taos {
	ipAddr := iptool.GetRealIP(session.Request)
	whitelistChangeChan, whitelistChangeHandle := tool.GetRegisterChangeWhiteListHandle()
	dropUserChan, dropUserHandle := tool.GetRegisterDropUserHandle()
	return &Taos{
		Results:               list.New(),
		exit:                  make(chan struct{}, 1),
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

func (t *Taos) waitSignal(logger *logrus.Entry) {
	defer func() {
		logger.Trace("exit wait signal")
		tool.PutRegisterChangeWhiteListHandle(t.whitelistChangeHandle)
		tool.PutRegisterDropUserHandle(t.dropUserHandle)
	}()
	for {
		select {
		case <-t.dropUserChan:
			logger.Info("get drop user signal")
			isDebug := log.IsDebug()
			t.lock(logger, isDebug)
			if t.closed {
				logger.Trace("server closed")
				t.Unlock()
				return
			}
			logger.WithField("clientIP", t.ipStr).Info("user dropped! close connection!")
			logger.Trace("close session")
			s := log.GetLogNow(isDebug)
			_ = t.session.Close()
			logger.Debugf("close session cost:%s", log.GetLogDuration(isDebug, s))
			t.Unlock()
			logger.Trace("close handler")
			s = log.GetLogNow(isDebug)
			t.Close()
			logger.Debugf("close handler cost:%s", log.GetLogDuration(isDebug, s))
			return
		case <-t.whitelistChangeChan:
			logger.Info("get whitelist change signal")
			isDebug := log.IsDebug()
			t.lock(logger, isDebug)
			if t.closed {
				logger.Trace("server closed")
				t.Unlock()
				return
			}
			logger.Trace("get whitelist")
			s := log.GetLogNow(isDebug)
			whitelist, err := tool.GetWhitelist(t.conn)
			logger.Debugf("get whitelist cost:%s", log.GetLogDuration(isDebug, s))
			if err != nil {
				logger.WithField("clientIP", t.ipStr).WithError(err).Errorln("get whitelist error! close connection!")
				s = log.GetLogNow(isDebug)
				_ = t.session.Close()
				logger.Debugf("close session cost:%s", log.GetLogDuration(isDebug, s))
				t.Unlock()
				logger.Trace("close handler")
				s = log.GetLogNow(isDebug)
				t.Close()
				logger.Debugf("close handler cost:%s", log.GetLogDuration(isDebug, s))
				return
			}
			logger.Tracef("check whitelist, ip: %s, whitelist: %s", t.ipStr, tool.IpNetSliceToString(whitelist))
			valid := tool.CheckWhitelist(whitelist, t.ip)
			if !valid {
				logger.WithField("clientIP", t.ipStr).Errorln("ip not in whitelist! close connection!")
				logger.Trace("close session")
				s = log.GetLogNow(isDebug)
				_ = t.session.Close()
				logger.Debugf("close session cost:%s", log.GetLogDuration(isDebug, s))
				t.Unlock()
				logger.Trace("close handler")
				s = log.GetLogNow(isDebug)
				t.Close()
				logger.Debugf("close handler cost:%s", log.GetLogDuration(isDebug, s))
				return
			}
			t.Unlock()
		case <-t.exit:
			return
		}
	}
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
	logger      *logrus.Entry
	sync.Mutex
}

func (r *Result) FreeResult(logger *logrus.Entry) {
	r.Lock()
	defer r.Unlock()
	r.FieldsCount = 0
	r.Header = nil
	r.Lengths = nil
	r.Size = 0
	r.precision = 0
	r.Block = nil
	if logger == nil {
		logger = r.logger
	}
	if r.TaosResult != nil {
		syncinterface.FreeResult(r.TaosResult, logger, log.IsDebug())
		r.TaosResult = nil
	}
}

func (t *Taos) addResult(result *Result) {
	index := atomic.AddUint64(&t.resultIndex, 1)
	result.index = index
	result.logger = t.logger.WithField("resultID", index)
	t.logger.Trace("get result locker")
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.resultLocker.Lock()
	t.logger.Debugf("get result locker cost:%s", log.GetLogDuration(isDebug, s))
	t.Results.PushBack(result)
	t.logger.Trace("add result to list finished")
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
	defer t.resultLocker.Unlock()
	t.Results.Remove(item)
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
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSConnect, config.ReqIDKey: req.ReqID},
	)
	isDebug := log.IsDebug()
	t.lock(logger, isDebug)
	defer t.Unlock()
	if t.closed {
		logger.Trace("server closed")
		return
	}
	if t.conn != nil {
		logger.Trace("duplicate connections")
		wsErrorMsg(ctx, session, 0xffff, "duplicate connections", WSConnect, req.ReqID)
		return
	}
	conn, err := syncinterface.TaosConnect("", req.User, req.Password, req.DB, 0, logger, isDebug)
	if err != nil {
		logger.WithError(err).Errorln("connect to TDengine error")
		wstool.WSError(ctx, session, logger, err, WSConnect, req.ReqID)
		return
	}
	logger.Trace("get whitelist")
	s := log.GetLogNow(isDebug)
	whitelist, err := tool.GetWhitelist(conn)
	logger.Debugf("get whitelist cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.WithError(err).Errorln("get whitelist error")
		syncinterface.TaosClose(conn, logger, isDebug)
		wstool.WSError(ctx, session, logger, err, WSConnect, req.ReqID)
		return
	}
	logger.Tracef("check whitelist, ip: %s, whitelist: %s", t.ipStr, tool.IpNetSliceToString(whitelist))
	valid := tool.CheckWhitelist(whitelist, t.ip)
	if !valid {
		logger.Errorf("ip not in whitelist, ip: %s, whitelist: %s", t.ipStr, tool.IpNetSliceToString(whitelist))
		syncinterface.TaosClose(conn, logger, isDebug)
		wstool.WSErrorMsg(ctx, session, logger, 0xffff, "whitelist prohibits current IP access", WSConnect, req.ReqID)
		return
	}
	s = log.GetLogNow(isDebug)
	logger.Trace("register whitelist change")
	err = tool.RegisterChangeWhitelist(conn, t.whitelistChangeHandle)
	logger.Debugf("register whitelist change cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.WithError(err).Errorln("register whitelist change error")
		syncinterface.TaosClose(conn, logger, isDebug)
		wstool.WSError(ctx, session, logger, err, WSConnect, req.ReqID)
		return
	}
	s = log.GetLogNow(isDebug)
	logger.Trace("register drop user")
	err = tool.RegisterDropUser(conn, t.dropUserHandle)
	logger.Debugf("register drop user cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.WithError(err).Errorln("register drop user error")
		syncinterface.TaosClose(conn, logger, isDebug)
		wstool.WSError(ctx, session, logger, err, WSConnect, req.ReqID)
		return
	}
	t.conn = conn
	logger.Trace("start wait signal goroutine")
	go t.waitSignal(t.logger)
	wstool.WSWriteJson(session, logger, &WSConnectResp{
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
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSQuery, config.ReqIDKey: req.ReqID},
	)
	if t.conn == nil {
		logger.Trace("server not connected")
		wsErrorMsg(ctx, session, 0xffff, "server not connected", WSQuery, req.ReqID)
		return
	}
	logger.Tracef("req_id: 0x%x,query sql: %s", req.ReqID, req.SQL)
	sqlType := monitor.WSRecordRequest(req.SQL)
	isDebug := log.IsDebug()
	logger.Trace("get handler lock")
	s := log.GetLogNow(isDebug)
	handler := async.GlobalAsync.HandlerPool.Get()
	logger.Debugf("get handler cost:%s", log.GetLogDuration(isDebug, s))
	defer async.GlobalAsync.HandlerPool.Put(handler)
	logger.Trace("execute query")
	s = log.GetLogNow(isDebug)
	result := async.GlobalAsync.TaosQuery(t.conn, logger, isDebug, req.SQL, handler, int64(req.ReqID))
	logger.Debugf("query cost:%s", log.GetLogDuration(isDebug, s))
	code := wrapper.TaosError(result.Res)
	if code != httperror.SUCCESS {
		monitor.WSRecordResult(sqlType, false)
		errStr := wrapper.TaosErrorStr(result.Res)
		logger.Errorf("query error, code: %d, message: %s", code, errStr)
		logger.Trace("get thread lock for free result")
		syncinterface.FreeResult(result.Res, logger, isDebug)
		wsErrorMsg(ctx, session, code, errStr, WSQuery, req.ReqID)
		return
	}
	monitor.WSRecordResult(sqlType, true)
	logger.Trace("check is_update_query")
	s = log.GetLogNow(isDebug)
	isUpdate := wrapper.TaosIsUpdateQuery(result.Res)
	logger.Debugf("is_update_query %t cost: %s", isUpdate, log.GetLogDuration(isDebug, s))
	queryResult := &WSQueryResult{Action: WSQuery, ReqID: req.ReqID}
	if isUpdate {
		var affectRows int
		s = log.GetLogNow(isDebug)
		affectRows = wrapper.TaosAffectedRows(result.Res)
		logger.Debugf("affected_rows %d cost: %s", affectRows, log.GetLogDuration(isDebug, s))
		queryResult.IsUpdate = true
		queryResult.AffectedRows = affectRows
		logger.Trace("get thread lock for free result")
		syncinterface.FreeResult(result.Res, logger, isDebug)
		queryResult.Timing = wstool.GetDuration(ctx)
		wstool.WSWriteJson(session, logger, queryResult)
		return
	}
	// query
	s = log.GetLogNow(isDebug)
	fieldsCount := wrapper.TaosNumFields(result.Res)
	logger.Debugf("num_fields %d cost: %s", fieldsCount, log.GetLogDuration(isDebug, s))
	queryResult.FieldsCount = fieldsCount
	s = log.GetLogNow(isDebug)
	rowsHeader, _ := wrapper.ReadColumn(result.Res, fieldsCount)
	logger.Debugf("read column cost:%s", log.GetLogDuration(isDebug, s))
	queryResult.FieldsNames = rowsHeader.ColNames
	queryResult.FieldsLengths = rowsHeader.ColLength
	queryResult.FieldsTypes = rowsHeader.ColTypes
	s = log.GetLogNow(isDebug)
	precision := wrapper.TaosResultPrecision(result.Res)
	logger.Debugf("result_precision %d cost: %s ", precision, log.GetLogDuration(isDebug, s))
	queryResult.Precision = precision
	resultItem := &Result{
		TaosResult:  result.Res,
		FieldsCount: fieldsCount,
		Header:      rowsHeader,
		precision:   precision,
	}
	logger.Trace("add result to list")
	t.addResult(resultItem)
	queryResult.ID = resultItem.index
	queryResult.Timing = wstool.GetDuration(ctx)
	wstool.WSWriteJson(session, logger, queryResult)
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
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSWriteRaw, config.ReqIDKey: reqID},
	)
	isDebug := log.IsDebug()
	t.lock(logger, isDebug)
	defer t.Unlock()
	if t.closed {
		logger.Trace("server closed")
		return
	}
	if t.conn == nil {
		logger.Error("server not connected")
		wsTMQErrorMsg(ctx, session, 0xffff, "server not connected", WSWriteRaw, reqID, &messageID)
		return
	}
	meta := wrapper.BuildRawMeta(length, metaType, data)
	s := log.GetLogNow(isDebug)
	logger.Trace("get thread lock for write raw meta")
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	logger.Trace("write raw meta")
	errCode := wrapper.TMQWriteRaw(t.conn, meta)
	logger.Debugf("write_raw_meta cost:%s", log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		logger.Errorf("write raw meta error, code: %d, message: %s", errCode, errStr)
		wsErrorMsg(ctx, session, int(errCode)&0xffff, errStr, WSWriteRaw, reqID)
		return
	}
	resp := &WSWriteMetaResp{Action: WSWriteRaw, ReqID: reqID, MessageID: messageID, Timing: wstool.GetDuration(ctx)}
	wstool.WSWriteJson(session, logger, resp)
}

type WSWriteRawBlockResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func (t *Taos) writeRawBlock(ctx context.Context, session *melody.Session, reqID uint64, numOfRows int, tableName string, rawBlock unsafe.Pointer) {
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSWriteRawBlock, config.ReqIDKey: reqID},
	)
	isDebug := log.IsDebug()
	t.lock(logger, isDebug)
	defer t.Unlock()
	if t.closed {
		logger.Trace("server closed")
		return
	}
	if t.conn == nil {
		wsErrorMsg(ctx, session, 0xffff, "server not connected", WSWriteRawBlock, reqID)
		return
	}
	logger.Trace("get thread lock for write raw block")
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	logger.Trace("write raw block")
	errCode := wrapper.TaosWriteRawBlockWithReqID(t.conn, numOfRows, rawBlock, tableName, int64(reqID))
	thread.SyncLocker.Unlock()
	logger.Debugf("write raw cost:%s", log.GetLogDuration(isDebug, s))
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(int32(errCode))
		logger.Errorf("write raw block error, code: %d, message: %s", errCode, errStr)
		wsErrorMsg(ctx, session, errCode&0xffff, errStr, WSWriteRawBlock, reqID)
		return
	}
	resp := &WSWriteRawBlockResp{Action: WSWriteRawBlock, ReqID: reqID, Timing: wstool.GetDuration(ctx)}
	wstool.WSWriteJson(session, logger, resp)
}

type WSWriteRawBlockWithFieldsResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func (t *Taos) writeRawBlockWithFields(ctx context.Context, session *melody.Session, reqID uint64, numOfRows int, tableName string, rawBlock unsafe.Pointer, fields unsafe.Pointer, numFields int) {
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSWriteRawBlockWithFields, config.ReqIDKey: reqID},
	)
	isDebug := log.IsDebug()
	t.lock(logger, isDebug)
	defer t.Unlock()
	if t.closed {
		logger.Trace("server closed")
		return
	}
	if t.conn == nil {
		logger.Errorf("server not connected")
		wsErrorMsg(ctx, session, 0xffff, "server not connected", WSWriteRawBlockWithFields, reqID)
		return
	}
	logger.Trace("get thread lock for write raw block with fields")
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	logger.Trace("write raw block with fields")
	errCode := wrapper.TaosWriteRawBlockWithFieldsWithReqID(t.conn, numOfRows, rawBlock, tableName, fields, numFields, int64(reqID))
	thread.SyncLocker.Unlock()
	logger.Debugf("write raw block with fields cost:%s", log.GetLogDuration(isDebug, s))
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(int32(errCode))
		logger.Errorf("write raw block with fields error, code: %d, message: %s", errCode, errStr)
		wsErrorMsg(ctx, session, errCode&0xffff, errStr, WSWriteRawBlockWithFields, reqID)
		return
	}
	resp := &WSWriteRawBlockWithFieldsResp{Action: WSWriteRawBlockWithFields, ReqID: reqID, Timing: wstool.GetDuration(ctx)}
	wstool.WSWriteJson(session, logger, resp)
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
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSFetch, config.ReqIDKey: req.ReqID},
	)
	if t.conn == nil {
		logger.Errorf("server not connected")
		wsErrorMsg(ctx, session, 0xffff, "server not connected", WSFetch, req.ReqID)
		return
	}
	isDebug := log.IsDebug()
	resultItem := t.getResult(req.ID)
	if resultItem == nil {
		logger.Errorf("result is nil")
		wsErrorMsg(ctx, session, 0xffff, "result is nil", WSFetch, req.ReqID)
		return
	}
	resultS := resultItem.Value.(*Result)
	resultS.Lock()
	if resultS.TaosResult == nil {
		resultS.Unlock()
		logger.Errorf("result is nil")
		wsErrorMsg(ctx, session, 0xffff, "result is nil", WSFetch, req.ReqID)
		return
	}
	s := log.GetLogNow(isDebug)
	handler := async.GlobalAsync.HandlerPool.Get()
	logger.Debugf("get handler cost:%s", log.GetLogDuration(isDebug, s))
	defer async.GlobalAsync.HandlerPool.Put(handler)
	s = log.GetLogNow(isDebug)
	logger.Trace("call fetch_raw_block_a")
	result := async.GlobalAsync.TaosFetchRawBlockA(resultS.TaosResult, logger, isDebug, handler)
	logger.Debugf("fetch_raw_block_a cost:%s", log.GetLogDuration(isDebug, s))
	if result.N == 0 {
		logger.Trace("fetch raw block completed")
		resultS.Unlock()
		t.FreeResult(resultItem, logger)
		wstool.WSWriteJson(session, logger, &WSFetchResp{
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
		logger.Errorf("fetch raw block error, code: %d, message: %s", result.N, errStr)
		resultS.Unlock()
		t.FreeResult(resultItem, logger)
		wsErrorMsg(ctx, session, result.N&0xffff, errStr, WSFetch, req.ReqID)
		return
	}
	s = log.GetLogNow(isDebug)
	resultS.Lengths = wrapper.FetchLengths(resultS.TaosResult, resultS.FieldsCount)
	logger.Debugf("fetch_lengths %d cost: %s", resultS.Lengths, log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	logger.Trace("get raw block")
	block := wrapper.TaosGetRawBlock(resultS.TaosResult)
	logger.Debugf("get_raw_block cost:%s", log.GetLogDuration(isDebug, s))
	resultS.Block = block
	resultS.Size = result.N
	resultS.Unlock()
	wstool.WSWriteJson(session, logger, &WSFetchResp{
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
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSFetchBlock, config.ReqIDKey: req.ReqID},
	)
	if t.conn == nil {
		logger.Error("server not connected")
		wsErrorMsg(ctx, session, 0xffff, "server not connected", WSFetchBlock, req.ReqID)
		return
	}
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	resultItem := t.getResult(req.ID)
	if resultItem == nil {
		wsErrorMsg(ctx, session, 0xffff, "result is nil", WSFetchBlock, req.ReqID)
		return
	}
	resultS := resultItem.Value.(*Result)
	resultS.Lock()
	if resultS.TaosResult == nil {
		resultS.Unlock()
		wsErrorMsg(ctx, session, 0xffff, "result is nil", WSFetchBlock, req.ReqID)
		return
	}
	if resultS.Block == nil {
		resultS.Unlock()
		wsErrorMsg(ctx, session, 0xffff, "block is nil", WSFetchBlock, req.ReqID)
		return
	}
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
	logger.Debugf("handle binary content cost:%s", log.GetLogDuration(isDebug, s))
	wstool.WSWriteBinary(session, b, logger)
}

type WSFreeResultReq struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

func (t *Taos) freeResult(req *WSFreeResultReq) {
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSFreeResult, config.ReqIDKey: req.ReqID},
	)
	if t.conn == nil {
		logger.Trace("server not connected")
		return
	}
	resultItem := t.getResult(req.ID)
	if resultItem == nil {
		logger.Trace("result not found")
		return
	}
	resultS, ok := resultItem.Value.(*Result)
	if ok && resultS != nil {
		t.removeResult(resultItem)
		resultS.FreeResult(logger)
	}
}

type Writer struct {
	session *melody.Session
}

func (w *Writer) Write(p []byte) (int, error) {
	err := w.session.Write(p)
	return 0, err
}

func (t *Taos) FreeResult(element *list.Element, logger *logrus.Entry) {
	if element == nil {
		return
	}
	r := element.Value.(*Result)
	if r != nil {
		r.FreeResult(logger)
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
	root.Value.(*Result).FreeResult(t.logger)
	item := root.Next()
	for {
		if item == nil || item == root {
			return
		}
		item.Value.(*Result).FreeResult(t.logger)
		item = item.Next()
	}
}

func (t *Taos) Close() {
	isDebug := log.IsDebug()
	t.lock(t.logger, isDebug)
	defer t.Unlock()
	if t.closed {
		t.logger.Trace("server closed")
		return
	}
	t.closed = true
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	done := make(chan struct{})
	go func() {
		t.logger.Trace("wait task to finish")
		t.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		t.logger.Warn("wait task to finish timeout! force close!")
	case <-done:
		t.logger.Trace("all task finished")
	}
	t.logger.Trace("free all result")
	s := log.GetLogNow(isDebug)
	t.freeAllResult()
	t.logger.Debugf("free all result cost:%s", log.GetLogDuration(isDebug, s))
	if t.conn != nil {
		t.logger.Trace("get thread lock for close")
		syncinterface.TaosClose(t.conn, t.logger, isDebug)
		t.conn = nil
	}
	t.logger.Trace("close exit channel")
	close(t.exit)
}

func (t *Taos) lock(logger *logrus.Entry, isDebug bool) {
	s := log.GetLogNow(isDebug)
	logger.Trace("get handler lock")
	t.Lock()
	logger.Debugf("get handler lock cost:%s", log.GetLogDuration(isDebug, s))
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
	wstool.GetLogger(session).Tracef("write error message: %s", b)
	_ = session.Write(b)
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
	wstool.GetLogger(session).Tracef("write error message: %s", b)
	_ = session.Write(b)
}

func init() {
	c := NewQueryController()
	controller.AddController(c)
}

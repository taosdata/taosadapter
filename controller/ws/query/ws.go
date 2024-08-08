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

	"github.com/taosdata/driver-go/v3/common"

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
		logger.WithField("ip", ipAddr.String()).Debugln("ws connect")
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
				t.freeResult(session, &fetchJson)
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
		logger.Debugln("ws close", i, s)
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
		// generate session id
		sessionID := common.GetReqID()
		logger := log.GetLogger("ws").WithFields(logrus.Fields{
			"wsType":            "query",
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
		logger.Traceln("exit wait signal")
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
				logger.Traceln("server closed")
				t.Unlock()
				return
			}
			logger.WithField("clientIP", t.ipStr).Info("user dropped! close connection!")
			logger.Traceln("close session")
			s := log.GetLogNow(isDebug)
			t.session.Close()
			logger.Debugln("close session cost:", log.GetLogDuration(isDebug, s))
			t.Unlock()
			logger.Traceln("close handler")
			s = log.GetLogNow(isDebug)
			t.Close()
			logger.Debugln("close handler cost:", log.GetLogDuration(isDebug, s))
			return
		case <-t.whitelistChangeChan:
			logger.Info("get whitelist change signal")
			isDebug := log.IsDebug()
			s := log.GetLogNow(isDebug)
			t.lock(logger, isDebug)
			if t.closed {
				logger.Traceln("server closed")
				t.Unlock()
				return
			}
			logger.Traceln("get whitelist")
			s = log.GetLogNow(isDebug)
			whitelist, err := tool.GetWhitelist(t.conn)
			logger.Debugln("get whitelist cost:", log.GetLogDuration(isDebug, s))
			if err != nil {
				logger.WithField("clientIP", t.ipStr).WithError(err).Errorln("get whitelist error! close connection!")
				s = log.GetLogNow(isDebug)
				t.session.Close()
				logger.Debugln("close session cost:", log.GetLogDuration(isDebug, s))
				t.Unlock()
				logger.Traceln("close handler")
				s = log.GetLogNow(isDebug)
				t.Close()
				logger.Debugln("close handler cost:", log.GetLogDuration(isDebug, s))
				return
			}
			logger.Tracef("check whitelist, ip: %d, whitelist: %s\n", t.ip, tool.IpNetSliceToString(whitelist))
			valid := tool.CheckWhitelist(whitelist, t.ip)
			if !valid {
				logger.WithField("clientIP", t.ipStr).Errorln("ip not in whitelist! close connection!")
				logger.Traceln("close session")
				s = log.GetLogNow(isDebug)
				t.session.Close()
				logger.Debugln("close session cost:", log.GetLogDuration(isDebug, s))
				t.Unlock()
				logger.Traceln("close handler")
				s = log.GetLogNow(isDebug)
				t.Close()
				logger.Debugln("close handler cost:", log.GetLogDuration(isDebug, s))
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
		tool.FreeResult(r.TaosResult, logger, log.IsDebug())
	}
	r.logger = nil
	r.Unlock()
}

func (t *Taos) addResult(result *Result) {
	index := atomic.AddUint64(&t.resultIndex, 1)
	result.index = index
	result.logger = t.logger.WithField("resultID", index)
	t.logger.Traceln("get result locker")
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.resultLocker.Lock()
	t.logger.Debugln("get result locker cost:", log.GetLogDuration(isDebug, s))
	t.Results.PushBack(result)
	t.logger.Traceln("add result to list finished")
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
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSConnect, config.ReqIDKey: req.ReqID},
	)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.lock(logger, isDebug)
	defer t.Unlock()
	if t.closed {
		logger.Traceln("server closed")
		return
	}
	if t.conn != nil {
		logger.Traceln("duplicate connections")
		wsErrorMsg(ctx, session, 0xffff, "duplicate connections", WSConnect, req.ReqID)
		return
	}
	s = log.GetLogNow(isDebug)
	logger.Traceln("get thread lock for connect")
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	logger.Traceln("connect to TDengine")
	conn, err := wrapper.TaosConnect("", req.User, req.Password, req.DB, 0)
	logger.Debugln("connect cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if err != nil {
		logger.WithError(err).Errorln("connect to TDengine error")
		wstool.WSError(ctx, session, err, WSConnect, req.ReqID)
		return
	}
	logger.Traceln("get whitelist")
	s = log.GetLogNow(isDebug)
	whitelist, err := tool.GetWhitelist(conn)
	logger.Debugln("get whitelist cost:", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.WithError(err).Errorln("get whitelist error")
		tool.TaosClose(conn, logger, isDebug)
		wstool.WSError(ctx, session, err, WSConnect, req.ReqID)
		return
	}
	logger.Tracef("check whitelist, ip: %d, whitelist: %s\n", t.ip, tool.IpNetSliceToString(whitelist))
	valid := tool.CheckWhitelist(whitelist, t.ip)
	if !valid {
		logger.Errorf("ip not in whitelist, ip: %d, whitelist: %s\n", t.ip, tool.IpNetSliceToString(whitelist))
		tool.TaosClose(conn, logger, isDebug)
		wstool.WSErrorMsg(ctx, session, 0xffff, "whitelist prohibits current IP access", WSConnect, req.ReqID)
		return
	}
	s = log.GetLogNow(isDebug)
	logger.Traceln("register whitelist change")
	err = tool.RegisterChangeWhitelist(conn, t.whitelistChangeHandle)
	logger.Debugln("register whitelist change cost:", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.WithError(err).Errorln("register whitelist change error")
		tool.TaosClose(conn, logger, isDebug)
		wstool.WSError(ctx, session, err, WSConnect, req.ReqID)
		return
	}
	s = log.GetLogNow(isDebug)
	logger.Traceln("register drop user")
	err = tool.RegisterDropUser(conn, t.dropUserHandle)
	logger.Debugln("register drop user cost:", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.WithError(err).Errorln("register drop user error")
		tool.TaosClose(conn, logger, isDebug)
		wstool.WSError(ctx, session, err, WSConnect, req.ReqID)
		return
	}
	t.conn = conn
	logger.Traceln("start wait signal goroutine")
	go t.waitSignal(t.logger)
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
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSQuery, config.ReqIDKey: req.ReqID},
	)
	if t.conn == nil {
		logger.Traceln("server not connected")
		wsErrorMsg(ctx, session, 0xffff, "server not connected", WSQuery, req.ReqID)
		return
	}
	logger.Tracef("req_id: %d,query sql: %s\n", req.ReqID, req.SQL)
	sqlType := monitor.WSRecordRequest(req.SQL)
	isDebug := log.IsDebug()
	logger.Traceln("get handler lock")
	s := log.GetLogNow(isDebug)
	handler := async.GlobalAsync.HandlerPool.Get()
	logger.Debugln("get handler cost:", log.GetLogDuration(isDebug, s))
	defer async.GlobalAsync.HandlerPool.Put(handler)
	logger.Debugln("execute query")
	s = log.GetLogNow(isDebug)
	result := async.GlobalAsync.TaosQuery(t.conn, logger, isDebug, req.SQL, handler, int64(req.ReqID))
	logger.Debugln("query cost ", log.GetLogDuration(isDebug, s))
	code := wrapper.TaosError(result.Res)
	if code != httperror.SUCCESS {
		monitor.WSRecordResult(sqlType, false)
		errStr := wrapper.TaosErrorStr(result.Res)
		logger.Errorf("query error, code: %d, message: %s\n", code, errStr)
		s = log.GetLogNow(isDebug)
		logger.Traceln("get thread lock for free result")
		tool.FreeResult(result.Res, logger, isDebug)
		wsErrorMsg(ctx, session, code, errStr, WSQuery, req.ReqID)
		return
	}
	monitor.WSRecordResult(sqlType, true)
	logger.Traceln("check is_update_query")
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
		s = log.GetLogNow(isDebug)
		logger.Traceln("get thread lock for free result")
		tool.FreeResult(result.Res, logger, isDebug)
		queryResult.Timing = wstool.GetDuration(ctx)
		wstool.WSWriteJson(session, queryResult)
		return
	} else {
		s = log.GetLogNow(isDebug)
		fieldsCount := wrapper.TaosNumFields(result.Res)
		logger.Debugf("num_fields %d cost: %s\n", fieldsCount, log.GetLogDuration(isDebug, s))
		queryResult.FieldsCount = fieldsCount
		s = log.GetLogNow(isDebug)
		rowsHeader, _ := wrapper.ReadColumn(result.Res, fieldsCount)
		logger.Debugln("read column cost:", log.GetLogDuration(isDebug, s))
		queryResult.FieldsNames = rowsHeader.ColNames
		queryResult.FieldsLengths = rowsHeader.ColLength
		queryResult.FieldsTypes = rowsHeader.ColTypes
		s = log.GetLogNow(isDebug)
		precision := wrapper.TaosResultPrecision(result.Res)
		logger.Debugf("result_precision %d cost: %s \n", precision, log.GetLogDuration(isDebug, s))
		queryResult.Precision = precision
		result := &Result{
			TaosResult:  result.Res,
			FieldsCount: fieldsCount,
			Header:      rowsHeader,
			precision:   precision,
		}
		logger.Traceln("add result to list")
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
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSWriteRaw, config.ReqIDKey: reqID},
	)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.lock(logger, isDebug)
	defer t.Unlock()
	if t.closed {
		logger.Traceln("server closed")
		return
	}
	if t.conn == nil {
		logger.Errorln("server not connected")
		wsTMQErrorMsg(ctx, session, 0xffff, "server not connected", WSWriteRaw, reqID, &messageID)
		return
	}
	meta := wrapper.BuildRawMeta(length, metaType, data)
	s = log.GetLogNow(isDebug)
	logger.Traceln("get thread lock for write raw meta")
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	logger.Traceln("write raw meta")
	errCode := wrapper.TMQWriteRaw(t.conn, meta)
	logger.Debugln("write_raw_meta cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		logger.Errorf("write raw meta error, code: %d, message: %s\n", errCode, errStr)
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
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSWriteRawBlock, config.ReqIDKey: reqID},
	)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.lock(logger, isDebug)
	defer t.Unlock()
	if t.closed {
		logger.Traceln("server closed")
		return
	}
	if t.conn == nil {
		wsErrorMsg(ctx, session, 0xffff, "server not connected", WSWriteRawBlock, reqID)
		return
	}
	logger.Traceln("get thread lock for write raw block")
	s = log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	logger.Traceln("write raw block")
	errCode := wrapper.TaosWriteRawBlockWithReqID(t.conn, numOfRows, rawBlock, tableName, int64(reqID))
	thread.Unlock()
	logger.Debugln("write raw cost:", log.GetLogDuration(isDebug, s))
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(int32(errCode))
		logger.Errorf("write raw block error, code: %d, message: %s\n", errCode, errStr)
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
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSWriteRawBlockWithFields, config.ReqIDKey: reqID},
	)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.lock(logger, isDebug)
	defer t.Unlock()
	if t.closed {
		logger.Traceln("server closed")
		return
	}
	if t.conn == nil {
		logger.Errorf("server not connected")
		wsErrorMsg(ctx, session, 0xffff, "server not connected", WSWriteRawBlockWithFields, reqID)
		return
	}
	logger.Traceln("get thread lock for write raw block with fields")
	s = log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	logger.Traceln("write raw block with fields")
	errCode := wrapper.TaosWriteRawBlockWithFieldsWithReqID(t.conn, numOfRows, rawBlock, tableName, fields, numFields, int64(reqID))
	thread.Unlock()
	logger.Debugln("write raw block with fields cost:", log.GetLogDuration(isDebug, s))
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(int32(errCode))
		logger.Errorf("write raw block with fields error, code: %d, message: %s\n", errCode, errStr)
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
	s := log.GetLogNow(isDebug)
	handler := async.GlobalAsync.HandlerPool.Get()
	logger.Debugln("get handler cost:", log.GetLogDuration(isDebug, s))
	defer async.GlobalAsync.HandlerPool.Put(handler)
	s = log.GetLogNow(isDebug)
	logger.Traceln("call fetch_raw_block_a")
	result := async.GlobalAsync.TaosFetchRawBlockA(resultS.TaosResult, logger, isDebug, handler)
	logger.Debugln("fetch_raw_block_a cost:", log.GetLogDuration(isDebug, s))
	if result.N == 0 {
		logger.Traceln("fetch raw block completed")
		t.FreeResult(resultItem, logger)
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
		logger.Errorf("fetch raw block error, code: %d, message: %s\n", result.N, errStr)
		t.FreeResult(resultItem, logger)
		wsErrorMsg(ctx, session, result.N&0xffff, errStr, WSFetch, req.ReqID)
		return
	}
	s = log.GetLogNow(isDebug)
	resultS.Lengths = wrapper.FetchLengths(resultS.TaosResult, resultS.FieldsCount)
	logger.Debugf("fetch_lengths %d cost: %s", resultS.Lengths, log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	logger.Traceln("get raw block")
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
	wstool.WSWriteBinary(session, b)
}

type WSFreeResultReq struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

func (t *Taos) freeResult(session *melody.Session, req *WSFreeResultReq) {
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSFreeResult, config.ReqIDKey: req.ReqID},
	)
	if t.conn == nil {
		logger.Traceln("server not connected")
		return
	}
	resultItem := t.getResult(req.ID)
	if resultItem == nil {
		logger.Traceln("result not found")
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
	s := log.GetLogNow(isDebug)
	t.lock(t.logger, isDebug)
	defer t.Unlock()
	if t.closed {
		t.logger.Traceln("server closed")
		return
	}
	t.closed = true
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	done := make(chan struct{})
	go func() {
		t.logger.Traceln("wait task to finish")
		t.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		t.logger.Warn("wait task to finish timeout! force close!")
	case <-done:
		t.logger.Traceln("all task finished")
	}
	t.logger.Traceln("free all result")
	s = log.GetLogNow(isDebug)
	t.freeAllResult()
	t.logger.Debugln("free all result cost:", log.GetLogDuration(isDebug, s))
	if t.conn != nil {
		t.logger.Traceln("get thread lock for close")
		tool.TaosClose(t.conn, t.logger, isDebug)
		t.conn = nil
	}
	t.logger.Traceln("close exit channel")
	close(t.exit)
}

func (t *Taos) lock(logger *logrus.Entry, isDebug bool) {
	s := log.GetLogNow(isDebug)
	logger.Traceln("get handler lock")
	t.Lock()
	logger.Debugln("get handler lock cost:", log.GetLogDuration(isDebug, s))
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
	wstool.GetLogger(session).Tracef("write error message: %s", b)
	session.Write(b)
}

func init() {
	c := NewQueryController()
	controller.AddController(c)
}

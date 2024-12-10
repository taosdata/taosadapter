package tmq

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/asynctmq"
	"github.com/taosdata/taosadapter/v3/db/asynctmq/tmqhandle"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	taoserrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/thread"
	"github.com/taosdata/taosadapter/v3/tools/bytesutil"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
	"github.com/taosdata/taosadapter/v3/tools/jsontype"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

type TMQController struct {
	tmqM *melody.Melody
}

func NewTMQController() *TMQController {
	tmqM := melody.New()
	tmqM.Upgrader.EnableCompression = true
	tmqM.Config.MaxMessageSize = 0

	tmqM.HandleConnect(func(session *melody.Session) {
		logger := wstool.GetLogger(session)
		logger.Debug("ws connect")
		session.Set(TaosTMQKey, NewTaosTMQ(session))
	})

	tmqM.HandleMessage(func(session *melody.Session, data []byte) {
		t := session.MustGet(TaosTMQKey).(*TMQ)
		if t.isClosed() {
			return
		}
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			if t.isClosed() {
				return
			}
			ctx := context.WithValue(context.Background(), wstool.StartTimeKey, time.Now().UnixNano())
			logger := wstool.GetLogger(session)
			logger.Debugf("get ws message data:%s", data)
			var action wstool.WSAction
			err := json.Unmarshal(data, &action)
			if err != nil {
				logger.Errorf("unmarshal ws request error, err:%s", err)
				return
			}
			switch action.Action {
			case wstool.ClientVersion:
				wstool.WSWriteVersion(session, logger)
			case TMQSubscribe:
				var req TMQSubscribeReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal subscribe args, err:%s, args:%s", err.Error(), action.Args)
					return
				}
				t.subscribe(ctx, session, &req)
			case TMQPoll:
				var req TMQPollReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal poll args, err:%s, args:%s", err.Error(), action.Args)
					return
				}
				t.poll(ctx, session, &req)
			case TMQFetch:
				var req TMQFetchReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal fetch args, err:%s, args:%s", err, action.Args)
					return
				}
				t.fetch(ctx, session, &req)
			case TMQFetchBlock:
				var req TMQFetchBlockReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal fetch_block args, err:%s, args:%s", err, action.Args)
					return
				}
				t.fetchBlock(ctx, session, &req)
			case TMQCommit:
				var req TMQCommitReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal commit args, err:%s, args:%s", err, action.Args)
					return
				}
				t.commit(ctx, session, &req)
			case TMQFetchJsonMeta:
				var req TMQFetchJsonMetaReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal fetch_json_meta args, err:%s, args:%s", err, action.Args)
					return
				}
				t.fetchJsonMeta(ctx, session, &req)
			case TMQFetchRaw:
				var req TMQFetchRawReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal fetch_raw args, err:%s, args:%s", err, action.Args)
					return
				}
				t.fetchRawBlock(ctx, session, &req)
			case TMQFetchRawNew:
				var req TMQFetchRawReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal fetch_raw_data args, err:%s, args:%s", err, action.Args)
					return
				}
				t.fetchRawBlockNew(ctx, session, &req)
			case TMQUnsubscribe:
				var req TMQUnsubscribeReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal unsubscribe args, err:%s, args:%s", err, action.Args)
					return
				}
				t.unsubscribe(ctx, session, &req)
			case TMQGetTopicAssignment:
				var req TMQGetTopicAssignmentReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal assignment args, err:%s, args:%s", err, action.Args)
					return
				}
				t.assignment(ctx, session, &req)
			case TMQSeek:
				var req TMQOffsetSeekReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal seek args, err:%s, args:%s", err, action.Args)
					return
				}
				t.offsetSeek(ctx, session, &req)
			case TMQCommitted:
				var req TMQCommittedReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal committed args, err:%s, args:%s", err, action.Args)
					return
				}
				session.MustGet(TaosTMQKey).(*TMQ).committed(ctx, session, &req)
			case TMQPosition:
				var req TMQPositionReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal position args, err:%s, args:%s", err, action.Args)
					return
				}
				session.MustGet(TaosTMQKey).(*TMQ).position(ctx, session, &req)
			case TMQListTopics:
				var req TMQListTopicsReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal list_topics args, err:%s, args:%s", err, action.Args)
					return
				}
				session.MustGet(TaosTMQKey).(*TMQ).listTopics(ctx, session, &req)
			case TMQCommitOffset:
				var req TMQCommitOffsetReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal commit_offset args, err:%s, args:%s", err, action.Args)
					return
				}
				session.MustGet(TaosTMQKey).(*TMQ).commitOffset(ctx, session, &req)
			default:
				logger.Errorf("unknown action:%s", action.Action)
				return
			}
		}()
	})
	tmqM.HandleClose(func(session *melody.Session, i int, s string) error {
		//message := melody.FormatCloseMessage(i, "")
		//session.WriteControl(websocket.CloseMessage, message, time.Now().Add(time.Second))
		logger := wstool.GetLogger(session)
		logger.Debugf("ws close, code:%d, msg %s", i, s)
		t, exist := session.Get(TaosTMQKey)
		if exist && t != nil {
			t.(*TMQ).Close(logger)
		}
		return nil
	})

	tmqM.HandleError(func(session *melody.Session, err error) {
		wstool.LogWSError(session, err)
		logger := wstool.GetLogger(session)
		t, exist := session.Get(TaosTMQKey)
		if exist && t != nil {
			t.(*TMQ).Close(logger)
		}
	})

	tmqM.HandleDisconnect(func(session *melody.Session) {
		logger := wstool.GetLogger(session)
		logger.Debug("ws disconnect")
		t, exist := session.Get(TaosTMQKey)
		if exist && t != nil {
			t.(*TMQ).Close(logger)
		}
	})
	return &TMQController{tmqM: tmqM}
}

func (s *TMQController) Init(ctl gin.IRouter) {
	ctl.GET("rest/tmq", func(c *gin.Context) {
		sessionID := generator.GetSessionID()
		logger := log.GetLogger("TMQ").WithFields(logrus.Fields{
			config.SessionIDKey: sessionID})
		_ = s.tmqM.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": logger})
	})
}

type TMQ struct {
	Session               *melody.Session
	consumer              unsafe.Pointer
	logger                *logrus.Entry
	tmpMessage            *Message
	asyncLocker           sync.Mutex
	thread                unsafe.Pointer
	handler               *tmqhandle.TMQHandler
	isAutoCommit          bool
	unsubscribed          bool
	closed                bool
	closedLock            sync.RWMutex
	autocommitInterval    time.Duration
	nextTime              time.Time
	exit                  chan struct{}
	dropUserChan          chan struct{}
	whitelistChangeChan   chan int64
	session               *melody.Session
	ip                    net.IP
	ipStr                 string
	wg                    sync.WaitGroup
	conn                  unsafe.Pointer
	whitelistChangeHandle cgo.Handle
	dropUserHandle        cgo.Handle
	sync.Mutex
}

func (t *TMQ) isClosed() bool {
	t.closedLock.RLock()
	defer t.closedLock.RUnlock()
	return t.closed
}

type Message struct {
	Index    uint64
	Topic    string
	VGroupID int32
	Offset   int64
	Type     int32
	CPointer unsafe.Pointer
	buffer   []byte
	sync.Mutex
}

func NewTaosTMQ(session *melody.Session) *TMQ {
	logger := wstool.GetLogger(session)
	ipAddr := iptool.GetRealIP(session.Request)
	whitelistChangeChan, whitelistChangeHandle := tool.GetRegisterChangeWhiteListHandle()
	dropUserChan, dropUserHandle := tool.GetRegisterDropUserHandle()
	return &TMQ{
		tmpMessage:            &Message{},
		handler:               tmqhandle.GlobalTMQHandlerPoll.Get(),
		thread:                asynctmq.InitTMQThread(),
		isAutoCommit:          true,
		autocommitInterval:    time.Second * 5,
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

func (t *TMQ) waitSignal(logger *logrus.Entry) {
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
			if t.isClosed() {
				logger.Trace("server closed")
				t.Unlock()
				return
			}
			logger.Info("user dropped! close connection!")
			t.signalExit(logger, isDebug)
			return
		case <-t.whitelistChangeChan:
			logger.Info("get whitelist change signal")
			isDebug := log.IsDebug()
			t.lock(logger, isDebug)
			if t.isClosed() {
				logger.Trace("server closed")
				t.Unlock()
				return
			}
			logger.Trace("get whitelist")
			s := log.GetLogNow(isDebug)
			whitelist, err := tool.GetWhitelist(t.conn)
			logger.Debugf("get whitelist cost:%s", log.GetLogDuration(isDebug, s))
			if err != nil {
				logger.Errorf("get whitelist error, close connection, err:%s", err)
				t.signalExit(logger, isDebug)
				return
			}
			logger.Tracef("check whitelist, ip:%s, whitelist:%s", t.ipStr, tool.IpNetSliceToString(whitelist))
			valid := tool.CheckWhitelist(whitelist, t.ip)
			if !valid {
				logger.Errorf("ip not in whitelist, close connection, ip:%s, whitelist:%s", t.ipStr, tool.IpNetSliceToString(whitelist))
				t.signalExit(logger, isDebug)
				return
			}
			t.Unlock()
		case <-t.exit:
			return
		}
	}
}

func (t *TMQ) signalExit(logger *logrus.Entry, isDebug bool) {
	logger.Trace("close session")
	s := log.GetLogNow(isDebug)
	_ = t.session.Close()
	logger.Debugf("close session cost:%s", log.GetLogDuration(isDebug, s))
	t.Unlock()
	s = log.GetLogNow(isDebug)
	t.Close(logger)
	logger.Debugf("close handler cost:%s", log.GetLogDuration(isDebug, s))
}

func (t *TMQ) lock(logger *logrus.Entry, isDebug bool) {
	s := log.GetLogNow(isDebug)
	logger.Trace("get handler lock")
	t.Lock()
	logger.Debugf("get handler lock cost:%s", log.GetLogDuration(isDebug, s))
}

type TMQSubscribeReq struct {
	ReqID                uint64   `json:"req_id"`
	User                 string   `json:"user"`
	Password             string   `json:"password"`
	DB                   string   `json:"db"`
	GroupID              string   `json:"group_id"`
	ClientID             string   `json:"client_id"`
	OffsetRest           string   `json:"offset_rest"` // typo
	OffsetReset          string   `json:"offset_reset"`
	Topics               []string `json:"topics"`
	AutoCommit           string   `json:"auto_commit"`
	AutoCommitIntervalMS string   `json:"auto_commit_interval_ms"`
	SnapshotEnable       string   `json:"snapshot_enable"`
	WithTableName        string   `json:"with_table_name"`
	EnableBatchMeta      string   `json:"enable_batch_meta"`
	MsgConsumeExcluded   string   `json:"msg_consume_excluded"`
	SessionTimeoutMS     string   `json:"session_timeout_ms"`
	MaxPollIntervalMS    string   `json:"max_poll_interval_ms"`
	TZ                   string   `json:"tz"`
	App                  string   `json:"app"`
	IP                   string   `json:"ip"`
}

type TMQSubscribeResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func (t *TMQ) subscribe(ctx context.Context, session *melody.Session, req *TMQSubscribeReq) {
	action := TMQSubscribe
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	isDebug := log.IsDebug()
	logger.Tracef("subscribe request:%+v", req)
	// lock for consumer and unsubscribed
	// used for subscribe,unsubscribe and
	t.lock(logger, isDebug)
	defer t.Unlock()
	if t.isClosed() {
		logger.Trace("server closed")
		return
	}
	if t.consumer != nil {
		if t.unsubscribed {
			logger.Trace("tmq resubscribe")
			topicList := wrapper.TMQListNew()
			defer func() {
				wrapper.TMQListDestroy(topicList)
			}()
			for _, topic := range req.Topics {
				errCode := wrapper.TMQListAppend(topicList, topic)
				if errCode != 0 {
					errStr := wrapper.TMQErr2Str(errCode)
					logger.Errorf("tmq list append error, code:%d, msg:%s", errCode, errStr)
					wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, action, req.ReqID, nil)
					return
				}
			}
			errCode := t.wrapperSubscribe(logger, isDebug, t.consumer, topicList)
			if errCode != 0 {
				errStr := wrapper.TMQErr2Str(errCode)
				wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, action, req.ReqID, nil)
				return
			}
			t.unsubscribed = false
			wstool.WSWriteJson(session, logger, &TMQSubscribeResp{
				Action: action,
				ReqID:  req.ReqID,
				Timing: wstool.GetDuration(ctx),
			})
			return
		}
		logger.Errorf("tmq should have unsubscribed first")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq should have unsubscribed first", action, req.ReqID, nil)
		return

	}
	tmqConfig := wrapper.TMQConfNew()
	defer func() {
		wrapper.TMQConfDestroy(tmqConfig)
	}()
	var tmqOptions = make(map[string]string)
	if len(req.GroupID) != 0 {
		tmqOptions["group.id"] = req.GroupID
	}
	if len(req.ClientID) != 0 {
		tmqOptions["client.id"] = req.ClientID
	}
	if len(req.DB) != 0 {
		tmqOptions["td.connect.db"] = req.DB
	}
	offsetReset := req.OffsetRest

	if len(req.OffsetReset) != 0 {
		offsetReset = req.OffsetReset
	}
	if len(offsetReset) != 0 {
		tmqOptions["auto.offset.reset"] = offsetReset
	}
	tmqOptions["td.connect.user"] = req.User
	tmqOptions["td.connect.pass"] = req.Password
	if len(req.WithTableName) != 0 {
		tmqOptions["msg.with.table.name"] = req.WithTableName
	}
	if len(req.MsgConsumeExcluded) != 0 {
		tmqOptions["msg.consume.excluded"] = req.MsgConsumeExcluded
	}
	// autocommit always false
	tmqOptions["enable.auto.commit"] = "false"
	if len(req.AutoCommit) != 0 {
		var err error
		t.isAutoCommit, err = strconv.ParseBool(req.AutoCommit)
		if err != nil {
			logger.Errorf("parse auto commit:%s as bool error:%s", req.AutoCommit, err)
			wsTMQErrorMsg(ctx, session, logger, 0xffff, err.Error(), action, req.ReqID, nil)
			return
		}
	}
	if len(req.AutoCommitIntervalMS) != 0 {
		autocommitIntervalMS, err := strconv.ParseInt(req.AutoCommitIntervalMS, 10, 64)
		if err != nil {
			logger.Errorf("parse auto commit interval:%s as int error:%s", req.AutoCommitIntervalMS, err)
			wsTMQErrorMsg(ctx, session, logger, 0xffff, err.Error(), action, req.ReqID, nil)
			return
		}
		t.autocommitInterval = time.Duration(autocommitIntervalMS) * time.Millisecond
	}
	if len(req.SnapshotEnable) != 0 {
		tmqOptions["experimental.snapshot.enable"] = req.SnapshotEnable
	}
	if len(req.EnableBatchMeta) != 0 {
		tmqOptions["msg.enable.batchmeta"] = req.EnableBatchMeta
	}
	if len(req.SessionTimeoutMS) != 0 {
		tmqOptions["session.timeout.ms"] = req.SessionTimeoutMS
	}
	if len(req.MaxPollIntervalMS) != 0 {
		tmqOptions["max.poll.interval.ms"] = req.MaxPollIntervalMS
	}
	var errCode int32
	for k, v := range tmqOptions {
		errCode = wrapper.TMQConfSet(tmqConfig, k, v)
		if errCode != httperror.SUCCESS {
			errStr := wrapper.TMQErr2Str(errCode)
			logger.Errorf("tmq conf set error, k:%s, v:%s, code:%d, msg:%s", k, v, errCode, errStr)
			wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, action, req.ReqID, nil)
			return
		}
	}
	cPointer, err := t.wrapperConsumerNew(logger, isDebug, tmqConfig)
	if err != nil {
		logger.Errorf("tmq consumer new error:%s", err)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, err.Error(), action, req.ReqID, nil)
		return
	}
	topicList := wrapper.TMQListNew()
	defer func() {
		wrapper.TMQListDestroy(topicList)
	}()
	for _, topic := range req.Topics {
		logger.Tracef("tmq append topic:%s", topic)
		errCode = wrapper.TMQListAppend(topicList, topic)
		if errCode != 0 {
			t.closeConsumerWithErrLog(ctx, cPointer, session, logger, isDebug, action, req.ReqID, taoserrors.NewError(int(errCode), wrapper.TMQErr2Str(errCode)), fmt.Sprintf("tmq list append error, tpic:%s", topic))
			return
		}
	}

	errCode = t.wrapperSubscribe(logger, isDebug, cPointer, topicList)
	if errCode != 0 {
		t.closeConsumerWithErrLog(ctx, cPointer, session, logger, isDebug, action, req.ReqID, taoserrors.NewError(int(errCode), wrapper.TMQErr2Str(errCode)), "tmq subscribe error")
		return
	}
	conn := wrapper.TMQGetConnect(cPointer)
	logger.Trace("get whitelist")
	whitelist, err := tool.GetWhitelist(conn)
	if err != nil {
		t.closeConsumerWithErrLog(ctx, cPointer, session, logger, isDebug, action, req.ReqID, err, "get whitelist error")
		return
	}
	logger.Tracef("check whitelist, ip:%s, whitelist:%s", t.ipStr, tool.IpNetSliceToString(whitelist))
	valid := tool.CheckWhitelist(whitelist, t.ip)
	if !valid {
		errorExt := fmt.Sprintf("whitelist prohibits current IP access, ip:%s, whitelist:%s", t.ipStr, tool.IpNetSliceToString(whitelist))
		err = errors.New("whitelist prohibits current IP access")
		t.closeConsumerWithErrLog(ctx, cPointer, session, logger, isDebug, action, req.ReqID, err, errorExt)
		return
	}
	logger.Trace("register change whitelist")
	err = tool.RegisterChangeWhitelist(conn, t.whitelistChangeHandle)
	if err != nil {
		t.closeConsumerWithErrLog(ctx, cPointer, session, logger, isDebug, action, req.ReqID, err, "register change whitelist error")
		return
	}
	logger.Trace("register drop user")
	err = tool.RegisterDropUser(conn, t.dropUserHandle)
	if err != nil {
		t.closeConsumerWithErrLog(ctx, cPointer, session, logger, isDebug, action, req.ReqID, err, "register drop user error")
		return
	}

	// set connection ip
	clientIP := t.ipStr
	if req.IP != "" {
		clientIP = req.IP
	}
	logger.Tracef("set connection ip, ip:%s", clientIP)
	code := syncinterface.TaosOptionsConnection(conn, common.TSDB_OPTION_CONNECTION_USER_IP, &clientIP, logger, isDebug)
	logger.Trace("set connection ip done")
	if code != 0 {
		t.closeConsumerWithErrLog(ctx, cPointer, session, logger, isDebug, action, req.ReqID, taoserrors.NewError(code, wrapper.TaosErrorStr(nil)), "set connection ip error")
		return
	}
	// set timezone
	if req.TZ != "" {
		logger.Tracef("set timezone, tz:%s", req.TZ)
		code = syncinterface.TaosOptionsConnection(conn, common.TSDB_OPTION_CONNECTION_TIMEZONE, &req.TZ, logger, isDebug)
		logger.Trace("set timezone done")
		if code != 0 {
			t.closeConsumerWithErrLog(ctx, cPointer, session, logger, isDebug, action, req.ReqID, taoserrors.NewError(code, wrapper.TaosErrorStr(nil)), "set timezone error")
			return
		}
	}
	// set connection app
	if req.App != "" {
		logger.Tracef("set app, app:%s", req.App)
		code = syncinterface.TaosOptionsConnection(conn, common.TSDB_OPTION_CONNECTION_USER_APP, &req.App, logger, isDebug)
		logger.Trace("set app done")
		if code != 0 {
			t.closeConsumerWithErrLog(ctx, cPointer, session, logger, isDebug, action, req.ReqID, taoserrors.NewError(code, wrapper.TaosErrorStr(nil)), "set app error")
			return
		}
	}

	t.conn = conn
	t.consumer = cPointer
	logger.Trace("start to wait signal")
	go t.waitSignal(t.logger)
	wstool.WSWriteJson(session, logger, &TMQSubscribeResp{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
	})
}

func (t *TMQ) closeConsumerWithErrLog(
	ctx context.Context,
	consumer unsafe.Pointer,
	session *melody.Session,
	logger *logrus.Entry,
	isDebug bool,
	action string,
	reqID uint64,
	err error,
	errorExt string,
) {
	logger.Errorf("%s, err: %s", errorExt, err)
	errCode := t.wrapperCloseConsumer(logger, isDebug, consumer)
	if errCode != 0 {
		errMsg := wrapper.TMQErr2Str(errCode)
		logger.Errorf("tmq close consumer error, consumer:%p, code:%d, msg:%s", t.consumer, errCode, errMsg)
	}
	wstool.WSError(ctx, session, logger, err, action, reqID)
}

type TMQCommitReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"` // unused
}

type TMQCommitResp struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	Action    string `json:"action"`
	ReqID     uint64 `json:"req_id"`
	Timing    int64  `json:"timing"`
	MessageID uint64 `json:"message_id"`
}

func (t *TMQ) commit(ctx context.Context, session *melody.Session, req *TMQCommitReq) {
	action := TMQCommit
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("commit request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, nil)
		return
	}
	// commit all
	isDebug := log.IsDebug()
	errCode, closed := t.wrapperCommit(logger, isDebug)
	if closed {
		logger.Trace("server closed")
		return
	}
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, action, req.ReqID, nil)
		return
	}
	resp := &TMQCommitResp{
		Action:    action,
		ReqID:     req.ReqID,
		MessageID: req.MessageID,
		Timing:    wstool.GetDuration(ctx),
	}
	wstool.WSWriteJson(session, logger, resp)
}

type TMQPollReq struct {
	ReqID        uint64 `json:"req_id"`
	BlockingTime int64  `json:"blocking_time"`
}

type TMQPollResp struct {
	Code        int    `json:"code"`
	Message     string `json:"message"`
	Action      string `json:"action"`
	ReqID       uint64 `json:"req_id"`
	Timing      int64  `json:"timing"`
	HaveMessage bool   `json:"have_message"`
	Topic       string `json:"topic"`
	Database    string `json:"database"`
	VgroupID    int32  `json:"vgroup_id"`
	MessageType int32  `json:"message_type"`
	MessageID   uint64 `json:"message_id"`
	Offset      int64  `json:"offset"`
}

func (t *TMQ) poll(ctx context.Context, session *melody.Session, req *TMQPollReq) {
	action := TMQPoll
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("poll request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, nil)
		return
	}
	now := time.Now()
	isDebug := log.IsDebug()
	if t.isAutoCommit && now.After(t.nextTime) {
		errCode, closed := t.wrapperCommit(logger, isDebug)
		if closed {
			logger.Trace("server closed")
			return
		}
		if errCode != 0 {
			errStr := wrapper.TMQErr2Str(errCode)
			logger.Errorf("tmq autocommit error:%s", taoserrors.NewError(int(errCode), errStr))
		}
		t.nextTime = now.Add(t.autocommitInterval)
	}
	message, closed := t.wrapperPoll(logger, isDebug, req.BlockingTime)
	if closed {
		logger.Trace("server closed")
	}
	resp := &TMQPollResp{
		Action: action,
		ReqID:  req.ReqID,
	}
	if message != nil {
		messageType := wrapper.TMQGetResType(message)
		if messageTypeIsValid(messageType) {
			t.tmpMessage.Lock()
			defer t.tmpMessage.Unlock()
			if t.tmpMessage.CPointer != nil {
				logger.Tracef("free old message, p:%p", t.tmpMessage.CPointer)
				closed = t.wrapperFreeResult(logger, isDebug)
				if closed {
					logger.Tracef("server closed, free result directly, message:%p", message)
					syncinterface.FreeResult(message, logger, isDebug)
					logger.Trace("free result directly finish")
					return
				}
			}
			index := atomic.AddUint64(&t.tmpMessage.Index, 1)

			t.tmpMessage.Index = index
			t.tmpMessage.Topic = wrapper.TMQGetTopicName(message)
			t.tmpMessage.VGroupID = wrapper.TMQGetVgroupID(message)
			t.tmpMessage.Offset = wrapper.TMQGetVgroupOffset(message)
			t.tmpMessage.Type = messageType
			t.tmpMessage.CPointer = message

			resp.HaveMessage = true
			resp.Topic = t.tmpMessage.Topic
			resp.Database = wrapper.TMQGetDBName(message)
			resp.VgroupID = t.tmpMessage.VGroupID
			resp.MessageID = t.tmpMessage.Index
			resp.MessageType = messageType
			resp.Offset = t.tmpMessage.Offset
			logger.Tracef("get message %d, topic:%s, vgroup:%d, offset:%d, db:%s", uintptr(message), t.tmpMessage.Topic, t.tmpMessage.VGroupID, t.tmpMessage.Offset, resp.Database)
		} else {
			logger.Errorf("unavailable tmq type:%d", messageType)
			syncinterface.FreeResult(message, logger, isDebug)
			logger.Trace("free result directly finish")
			wsTMQErrorMsg(ctx, session, logger, 0xffff, "unavailable tmq type:"+strconv.Itoa(int(messageType)), action, req.ReqID, nil)
			return
		}
	}

	resp.Timing = wstool.GetDuration(ctx)
	wstool.WSWriteJson(session, logger, resp)
}

type TMQFetchReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}
type TMQFetchResp struct {
	Code          int                `json:"code"`
	Message       string             `json:"message"`
	Action        string             `json:"action"`
	ReqID         uint64             `json:"req_id"`
	Timing        int64              `json:"timing"`
	MessageID     uint64             `json:"message_id"`
	Completed     bool               `json:"completed"`
	TableName     string             `json:"table_name"`
	Rows          int                `json:"rows"`
	FieldsCount   int                `json:"fields_count"`
	FieldsNames   []string           `json:"fields_names"`
	FieldsTypes   jsontype.JsonUint8 `json:"fields_types"`
	FieldsLengths []int64            `json:"fields_lengths"`
	Precision     int                `json:"precision"`
}

func (t *TMQ) fetch(ctx context.Context, session *melody.Session, req *TMQFetchReq) {
	action := TMQFetch
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("fetch request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, &req.MessageID)
		return
	}
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.tmpMessage.Lock()
	defer t.tmpMessage.Unlock()
	logger.Debugf("get message lock cost:%s", log.GetLogDuration(isDebug, s))
	if t.tmpMessage.CPointer == nil {
		logger.Error("message has been freed")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message has been freed", action, req.ReqID, &req.MessageID)
		return
	}
	message := t.tmpMessage
	if message.Index != req.MessageID {
		logger.Errorf("message ID are not equal, req:%d, message:%d", req.MessageID, message.Index)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message ID are not equal", action, req.ReqID, &req.MessageID)
		return
	}

	if !canGetData(message.Type) {
		logger.Errorf("message type is not data, type:%d", message.Type)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message type is not data", action, req.ReqID, &req.MessageID)
		return
	}
	rawBlock, closed := t.wrapperFetchRawBlock(logger, isDebug, message.CPointer)
	if closed {
		logger.Trace("server closed")
		return
	}
	errCode := rawBlock.Code
	blockSize := rawBlock.BlockSize
	block := rawBlock.Block
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(int32(errCode))
		logger.Errorf("tmq fetch raw block error, code:%d, msg:%s", errCode, errStr)
		wsTMQErrorMsg(ctx, session, logger, errCode, errStr, action, req.ReqID, &req.MessageID)
		return
	}
	resp := &TMQFetchResp{
		Action:    action,
		ReqID:     req.ReqID,
		MessageID: req.MessageID,
	}
	if blockSize == 0 {
		resp.Completed = true
		wstool.WSWriteJson(session, logger, resp)
		return
	}
	s = log.GetLogNow(isDebug)
	resp.TableName = wrapper.TMQGetTableName(message.CPointer)
	logger.Debugf("tmq_get_table_name cost:%s", log.GetLogDuration(isDebug, s))
	resp.Rows = blockSize
	s = log.GetLogNow(isDebug)
	resp.FieldsCount = wrapper.TaosNumFields(message.CPointer)
	logger.Debugf("num_fields cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	rowsHeader, _ := wrapper.ReadColumn(message.CPointer, resp.FieldsCount)
	logger.Debugf("read column cost:%s", log.GetLogDuration(isDebug, s))
	resp.FieldsNames = rowsHeader.ColNames
	resp.FieldsTypes = rowsHeader.ColTypes
	resp.FieldsLengths = rowsHeader.ColLength
	s = log.GetLogNow(isDebug)
	resp.Precision = wrapper.TaosResultPrecision(message.CPointer)
	logger.Debugf("result_precision cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	blockLength := int(parser.RawBlockGetLength(block))
	if cap(message.buffer) < blockLength+24 {
		message.buffer = make([]byte, 0, blockLength+24)
	}
	message.buffer = message.buffer[:blockLength+24]
	binary.LittleEndian.PutUint64(message.buffer, 0)
	binary.LittleEndian.PutUint64(message.buffer[8:], req.ReqID)
	binary.LittleEndian.PutUint64(message.buffer[16:], req.MessageID)
	bytesutil.Copy(block, message.buffer, 24, blockLength)
	resp.Timing = wstool.GetDuration(ctx)
	logger.Debugf("handle data cost:%s", log.GetLogDuration(isDebug, s))
	wstool.WSWriteJson(session, logger, resp)
}

type TMQFetchBlockReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}

func (t *TMQ) fetchBlock(ctx context.Context, session *melody.Session, req *TMQFetchBlockReq) {
	action := TMQFetchBlock
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("fetch block request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, &req.MessageID)
		return
	}
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.tmpMessage.Lock()
	defer t.tmpMessage.Unlock()
	logger.Debugf("get message lock cost:%s", log.GetLogDuration(isDebug, s))
	if t.tmpMessage.CPointer == nil {
		logger.Error("message has been freed")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message has been freed", action, req.ReqID, &req.MessageID)
		return
	}
	message := t.tmpMessage
	if message.Index != req.MessageID {
		logger.Errorf("message ID are not equal, req:%d, message:%d", req.MessageID, message.Index)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message is nil", action, req.ReqID, &req.MessageID)
		return
	}
	if !canGetData(message.Type) {
		logger.Errorf("message type is not data, type:%d", message.Type)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message type is not data", action, req.ReqID, &req.MessageID)
		return
	}
	if len(message.buffer) == 0 {
		logger.Errorf("no fetch data")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "no fetch data", action, req.ReqID, &req.MessageID)
		return
	}
	s = log.GetLogNow(isDebug)
	binary.LittleEndian.PutUint64(message.buffer, uint64(wstool.GetDuration(ctx)))
	logger.Debugf("handle data cost:%s", log.GetLogDuration(isDebug, s))
	wstool.WSWriteBinary(session, message.buffer, logger)
}

type TMQFetchRawReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}

func (t *TMQ) fetchRawBlock(ctx context.Context, session *melody.Session, req *TMQFetchRawReq) {
	action := TMQFetchRaw
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("fetch raw request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, &req.MessageID)
		return
	}
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.tmpMessage.Lock()
	defer t.tmpMessage.Unlock()
	logger.Debugf("get message lock cost:%s", log.GetLogDuration(isDebug, s))
	if t.tmpMessage.CPointer == nil {
		logger.Error("message has been freed")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message has been freed", action, req.ReqID, &req.MessageID)
		return
	}
	message := t.tmpMessage
	if message.Index != req.MessageID {
		logger.Errorf("message ID are not equal, req:%d, message:%d", req.MessageID, message.Index)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message ID is not equal", action, req.ReqID, &req.MessageID)
		return
	}
	rawData := asynctmq.TaosaInitTMQRaw()
	defer asynctmq.TaosaFreeTMQRaw(rawData)
	errCode, closed := t.wrapperGetRaw(logger, isDebug, message.CPointer, rawData)
	if closed {
		logger.Trace("server closed")
		return
	}
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		logger.Errorf("tmq get raw error, code:%d, msg:%s", errCode, errStr)
		wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, action, req.ReqID, &req.MessageID)
		return
	}
	s = log.GetLogNow(isDebug)
	length, metaType, data := wrapper.ParseRawMeta(rawData)
	blockLength := int(length)
	if cap(message.buffer) < blockLength+38 {
		message.buffer = make([]byte, 0, blockLength+38)
	}
	message.buffer = message.buffer[:blockLength+38]
	binary.LittleEndian.PutUint64(message.buffer, uint64(wstool.GetDuration(ctx)))
	binary.LittleEndian.PutUint64(message.buffer[8:], req.ReqID)
	binary.LittleEndian.PutUint64(message.buffer[16:], req.MessageID)
	binary.LittleEndian.PutUint64(message.buffer[24:], TMQRawMessage)
	binary.LittleEndian.PutUint32(message.buffer[32:], length)
	binary.LittleEndian.PutUint16(message.buffer[36:], metaType)
	bytesutil.Copy(data, message.buffer, 38, blockLength)
	s1 := log.GetLogNow(isDebug)
	wrapper.TMQFreeRaw(rawData)
	logger.Debugf("tmq_free_raw cost:%s", log.GetLogDuration(isDebug, s1))
	logger.Debugf("handle binary data cost:%s", log.GetLogDuration(isDebug, s))
	wstool.WSWriteBinary(session, message.buffer, logger)
}

func (t *TMQ) fetchRawBlockNew(ctx context.Context, session *melody.Session, req *TMQFetchRawReq) {
	action := TMQFetchRawNew
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("fetch raw request:%+v", req)
	if t.consumer == nil {
		logger.Trace("tmq not init")
		tmqFetchRawBlockErrorMsg(ctx, session, logger, 0xffff, "tmq not init", req.ReqID, req.MessageID)
		return
	}
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.tmpMessage.Lock()
	defer t.tmpMessage.Unlock()
	logger.Debugf("get message lock cost:%s", log.GetLogDuration(isDebug, s))
	if t.tmpMessage.CPointer == nil {
		logger.Error("message has been freed")
		tmqFetchRawBlockErrorMsg(ctx, session, logger, 0xffff, "message has been freed", req.ReqID, req.MessageID)
		return
	}
	message := t.tmpMessage
	if message.Index != req.MessageID {
		logger.Errorf("message ID are not equal, req:%d, message:%d", req.MessageID, message.Index)
		tmqFetchRawBlockErrorMsg(ctx, session, logger, 0xffff, "message ID is not equal", req.ReqID, req.MessageID)
		return
	}
	rawData := asynctmq.TaosaInitTMQRaw()
	defer asynctmq.TaosaFreeTMQRaw(rawData)
	errCode, closed := t.wrapperGetRaw(logger, isDebug, message.CPointer, rawData)
	if closed {
		logger.Trace("server closed")
		return
	}
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		logger.Errorf("tmq get raw error, code:%d, msg:%s", errCode, errStr)
		tmqFetchRawBlockErrorMsg(ctx, session, logger, int(errCode), errStr, req.ReqID, req.MessageID)
		return
	}
	s = log.GetLogNow(isDebug)
	length, metaType, data := wrapper.ParseRawMeta(rawData)
	message.buffer = wsFetchRawBlockMessage(ctx, message.buffer, req.ReqID, req.MessageID, metaType, length, data)
	s1 := log.GetLogNow(isDebug)
	wrapper.TMQFreeRaw(rawData)
	logger.Debugf("tmq_free_raw cost:%s", log.GetLogDuration(isDebug, s1))
	logger.Debugf("handle binary data cost:%s", log.GetLogDuration(isDebug, s))
	wstool.WSWriteBinary(session, message.buffer, logger)
}

type TMQFetchJsonMetaReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}
type TMQFetchJsonMetaResp struct {
	Code      int             `json:"code"`
	Message   string          `json:"message"`
	Action    string          `json:"action"`
	ReqID     uint64          `json:"req_id"`
	Timing    int64           `json:"timing"`
	MessageID uint64          `json:"message_id"`
	Data      json.RawMessage `json:"data"`
}

func (t *TMQ) fetchJsonMeta(ctx context.Context, session *melody.Session, req *TMQFetchJsonMetaReq) {
	action := TMQFetchJsonMeta
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("fetch json meta request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, &req.MessageID)
		return
	}
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.tmpMessage.Lock()
	defer t.tmpMessage.Unlock()
	logger.Debugf("get message lock cost:%s", log.GetLogDuration(isDebug, s))
	if t.tmpMessage.CPointer == nil {
		logger.Error("message has been freed")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message has been freed", action, req.ReqID, &req.MessageID)
		return
	}
	message := t.tmpMessage
	if message.Index != req.MessageID {
		logger.Errorf("message ID are not equal, req:%d, message:%d", req.MessageID, message.Index)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message ID is not equal", action, req.ReqID, &req.MessageID)
		return
	}
	jsonMeta, closed := t.wrapperGetJsonMeta(logger, isDebug, message.CPointer)
	if closed {
		logger.Trace("server closed")
		return
	}
	resp := TMQFetchJsonMetaResp{
		Action:    action,
		ReqID:     req.ReqID,
		MessageID: req.MessageID,
	}
	if jsonMeta == nil {
		resp.Data = nil
	} else {
		var binaryVal []byte
		i := 0
		c := byte(0)
		for {
			c = *((*byte)(unsafe.Pointer(uintptr(jsonMeta) + uintptr(i))))
			if c != 0 {
				binaryVal = append(binaryVal, c)
				i += 1
			} else {
				break
			}
		}
		resp.Data = binaryVal
	}
	s = log.GetLogNow(isDebug)
	wrapper.TMQFreeJsonMeta(jsonMeta)
	logger.Debugf("tmq_free_json_meta cost:%s", log.GetLogDuration(isDebug, s))
	resp.Timing = wstool.GetDuration(ctx)
	wstool.WSWriteJson(session, logger, resp)
}

type TMQUnsubscribeReq struct {
	ReqID uint64 `json:"req_id"`
}

type TMQUnsubscribeResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func (t *TMQ) unsubscribe(ctx context.Context, session *melody.Session, req *TMQUnsubscribeReq) {
	action := TMQUnsubscribe
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("unsubscribe request:%+v", req)
	isDebug := log.IsDebug()
	t.lock(logger, isDebug)
	defer t.Unlock()
	if t.isClosed() {
		logger.Trace("server closed")
		return
	}
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, nil)
		return
	}
	errCode, closed := t.wrapperUnsubscribe(logger, isDebug)
	if closed {
		logger.Trace("server closed")
		return
	}
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		logger.Errorf("tmq unsubscribe error,consumer:%p, code:%d, msg:%s", t.consumer, errCode, errStr)
		wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, action, req.ReqID, nil)
		return
	}
	logger.Trace("free all result")
	t.freeMessage(false)
	logger.Trace("free all result finished")
	t.unsubscribed = true
	wstool.WSWriteJson(session, logger, &TMQUnsubscribeResp{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
	})
}

type TMQGetTopicAssignmentReq struct {
	ReqID uint64 `json:"req_id"`
	Topic string `json:"topic"`
}

type TMQGetTopicAssignmentResp struct {
	Code       int                     `json:"code"`
	Message    string                  `json:"message"`
	Action     string                  `json:"action"`
	ReqID      uint64                  `json:"req_id"`
	Timing     int64                   `json:"timing"`
	Assignment []*tmqhandle.Assignment `json:"assignment"`
}

func (t *TMQ) assignment(ctx context.Context, session *melody.Session, req *TMQGetTopicAssignmentReq) {
	action := TMQGetTopicAssignment
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("assignment request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, nil)
		return
	}
	result, closed := t.wrapperGetTopicAssignment(logger, log.IsDebug(), req.Topic)
	if closed {
		logger.Trace("server closed")
		return
	}
	if result.Code != 0 {
		errStr := wrapper.TMQErr2Str(result.Code)
		logger.Errorf("tmq assignment error,consumer:%p, code:%d, msg:%s", t.consumer, result.Code, errStr)
		wsTMQErrorMsg(ctx, session, logger, int(result.Code), errStr, action, req.ReqID, nil)
		return
	}
	wstool.WSWriteJson(session, logger, TMQGetTopicAssignmentResp{
		Action:     action,
		ReqID:      req.ReqID,
		Timing:     wstool.GetDuration(ctx),
		Assignment: result.Assignment,
	})
}

type TMQOffsetSeekReq struct {
	ReqID    uint64 `json:"req_id"`
	Topic    string `json:"topic"`
	VgroupID int32  `json:"vgroup_id"`
	Offset   int64  `json:"offset"`
}

type TMQOffsetSeekResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func (t *TMQ) offsetSeek(ctx context.Context, session *melody.Session, req *TMQOffsetSeekReq) {
	action := TMQSeek
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("offset seek request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, nil)
		return
	}
	isDebug := log.IsDebug()
	errCode, closed := t.wrapperOffsetSeek(logger, isDebug, req.Topic, req.VgroupID, req.Offset)
	if closed {
		logger.Trace("server closed")
		return
	}
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		logger.Errorf("tmq offset seek error, consumer:%p, code:%d, msg:%s", t.consumer, errCode, errStr)
		wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, action, req.ReqID, nil)
		return
	}
	wstool.WSWriteJson(session, logger, TMQOffsetSeekResp{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
	})
}

func (t *TMQ) Close(logger *logrus.Entry) {
	t.Lock()
	defer t.Unlock()
	if t.isClosed() {
		return
	}
	t.closedLock.Lock()
	t.closed = true
	t.closedLock.Unlock()
	start := time.Now()
	logger.Info("tmq close")
	defer func() {
		logger.Infof("tmq close end, cost:%s", time.Since(start).String())
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	done := make(chan struct{})
	go func() {
		t.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		logger.Warn("wait stop over 1 minute")
		<-done
	case <-done:
	}
	logger.Debug("wait stop done")
	isDebug := log.IsDebug()

	defer func() {
		s := log.GetLogNow(isDebug)
		t.asyncLocker.Lock()
		asynctmq.DestroyTMQThread(t.thread)
		t.asyncLocker.Unlock()
		logger.Tracef("destroy tmq thread cost:%s", log.GetLogDuration(isDebug, s))
		tmqhandle.GlobalTMQHandlerPoll.Put(t.handler)
	}()

	defer func() {
		if t.consumer != nil {
			if !t.unsubscribed {
				errCode, _ := t.wrapperUnsubscribe(logger, isDebug)
				if errCode != 0 {
					errMsg := wrapper.TMQErr2Str(errCode)
					logger.Errorf("tmq unsubscribe consumer error, consumer:%p, code:%d, msg:%s", t.consumer, errCode, errMsg)
				}
			}
			errCode := t.wrapperCloseConsumer(logger, log.IsDebug(), t.consumer)
			if errCode != 0 {
				errMsg := wrapper.TMQErr2Str(errCode)
				logger.Errorf("tmq close consumer error, consumer:%p, code:%d, msg:%s", t.consumer, errCode, errMsg)
			}
		}
		close(t.exit)
	}()
	logger.Trace("start to free message")
	s := log.GetLogNow(isDebug)
	t.freeMessage(true)
	logger.Debugf("free message cost:%s", log.GetLogDuration(isDebug, s))
}

func (t *TMQ) freeMessage(closing bool) {
	t.tmpMessage.Lock()
	defer t.tmpMessage.Unlock()
	if t.tmpMessage.CPointer != nil {
		t.asyncLocker.Lock()
		if !closing && t.isClosed() {
			t.asyncLocker.Unlock()
			return
		}
		asynctmq.TaosaTMQFreeResultA(t.thread, t.tmpMessage.CPointer, t.handler.Handler)
		<-t.handler.Caller.FreeResult
		t.asyncLocker.Unlock()
		t.tmpMessage.CPointer = nil
	}
	t.tmpMessage.buffer = nil
}

type WSTMQErrorResp struct {
	Code      int     `json:"code"`
	Message   string  `json:"message"`
	Action    string  `json:"action"`
	ReqID     uint64  `json:"req_id"`
	Timing    int64   `json:"timing"`
	MessageID *uint64 `json:"message_id,omitempty"`
}

func wsTMQErrorMsg(ctx context.Context, session *melody.Session, logger *logrus.Entry, code int, message string, action string, reqID uint64, messageID *uint64) {
	data := &WSTMQErrorResp{
		Code:      code & 0xffff,
		Message:   message,
		Action:    action,
		ReqID:     reqID,
		Timing:    wstool.GetDuration(ctx),
		MessageID: messageID,
	}
	wstool.WSWriteJson(session, logger, data)
}

func canGetData(messageType int32) bool {
	return messageType == common.TMQ_RES_DATA || messageType == common.TMQ_RES_METADATA
}

func messageTypeIsValid(messageType int32) bool {
	switch messageType {
	case common.TMQ_RES_DATA, common.TMQ_RES_TABLE_META, common.TMQ_RES_METADATA:
		return true
	}
	return false
}

type TMQCommittedReq struct {
	ReqID          uint64          `json:"req_id"`
	TopicVgroupIDs []TopicVgroupID `json:"topic_vgroup_ids"`
}

type TMQCommittedResp struct {
	Code      int     `json:"code"`
	Message   string  `json:"message"`
	Action    string  `json:"action"`
	ReqID     uint64  `json:"req_id"`
	Timing    int64   `json:"timing"`
	Committed []int64 `json:"committed"`
}

func (t *TMQ) committed(ctx context.Context, session *melody.Session, req *TMQCommittedReq) {
	action := TMQCommitted
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("committed request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, nil)
		return
	}
	isDebug := log.IsDebug()
	offsets := make([]int64, 0, len(req.TopicVgroupIDs))
	for _, tv := range req.TopicVgroupIDs {
		res, closed := t.wrapperCommitted(logger, isDebug, tv.Topic, tv.VgroupID)
		if closed {
			logger.Trace("server closed")
			return
		}
		if res < 0 && res != OffsetInvalid {
			errStr := wrapper.TMQErr2Str(int32(res))
			logger.Errorf("tmq get committed error, code:%d, msg:%s", res, errStr)
			wsTMQErrorMsg(ctx, session, logger, int(res), errStr, action, req.ReqID, nil)
			return
		}
		offsets = append(offsets, res)
	}
	wstool.WSWriteJson(session, logger, TMQCommittedResp{
		Action:    action,
		ReqID:     req.ReqID,
		Timing:    wstool.GetDuration(ctx),
		Committed: offsets,
	})
}

type TopicVgroupID struct {
	Topic    string `json:"topic"`
	VgroupID int32  `json:"vgroup_id"`
}

type TMQPositionReq struct {
	ReqID          uint64          `json:"req_id"`
	TopicVgroupIDs []TopicVgroupID `json:"topic_vgroup_ids"`
}

type TMQPositionResp struct {
	Code     int     `json:"code"`
	Message  string  `json:"message"`
	Action   string  `json:"action"`
	ReqID    uint64  `json:"req_id"`
	Timing   int64   `json:"timing"`
	Position []int64 `json:"position"`
}

func (t *TMQ) position(ctx context.Context, session *melody.Session, req *TMQPositionReq) {
	action := TMQPosition
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("position request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, nil)
		return
	}
	isDebug := log.IsDebug()
	positions := make([]int64, 0, len(req.TopicVgroupIDs))
	for _, tv := range req.TopicVgroupIDs {
		res, closed := t.wrapperPosition(logger, isDebug, tv.Topic, tv.VgroupID)
		if closed {
			logger.Trace("server closed")
			return
		}
		if res < 0 && res != OffsetInvalid {
			errStr := wrapper.TMQErr2Str(int32(res))
			logger.Errorf("get position error, code:%d, msg:%s", res, errStr)
			wsTMQErrorMsg(ctx, session, logger, int(res), errStr, action, req.ReqID, nil)
			return
		}
		positions = append(positions, res)
	}

	wstool.WSWriteJson(session, logger, TMQPositionResp{
		Action:   action,
		ReqID:    req.ReqID,
		Timing:   wstool.GetDuration(ctx),
		Position: positions,
	})
}

type TMQListTopicsReq struct {
	ReqID uint64 `json:"req_id"`
}

type TMQListTopicsResp struct {
	Code    int      `json:"code"`
	Message string   `json:"message"`
	Action  string   `json:"action"`
	ReqID   uint64   `json:"req_id"`
	Timing  int64    `json:"timing"`
	Topics  []string `json:"topics"`
}

func (t *TMQ) listTopics(ctx context.Context, session *melody.Session, req *TMQListTopicsReq) {
	action := TMQListTopics
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("list topics request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, nil)
		return
	}
	if t.isClosed() {
		logger.Trace("server closed")
		return
	}
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	logger.Trace("subscription get thread lock")
	thread.SyncLocker.Lock()
	logger.Debugf("subscription get thread lock cost:%s", log.GetLogDuration(isDebug, s))
	if t.isClosed() {
		thread.SyncLocker.Unlock()
		logger.Trace("server closed")
		return
	}
	s = log.GetLogNow(isDebug)
	code, topicsPointer := wrapper.TMQSubscription(t.consumer)
	thread.SyncLocker.Unlock()
	logger.Debugf("subscription cost:%s", log.GetLogDuration(isDebug, s))
	defer wrapper.TMQListDestroy(topicsPointer)
	if code != 0 {
		errStr := wrapper.TMQErr2Str(code)
		logger.Errorf("tmq list topic error, code:%d, msg:%s", code, errStr)
		wsTMQErrorMsg(ctx, session, logger, int(code), errStr, action, req.ReqID, nil)
		return
	}
	s = log.GetLogNow(isDebug)
	topics := wrapper.TMQListToCArray(topicsPointer, int(wrapper.TMQListGetSize(topicsPointer)))
	logger.Debugf("tmq list topic cost:%s", log.GetLogDuration(log.IsDebug(), s))
	wstool.WSWriteJson(session, logger, TMQListTopicsResp{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
		Topics: topics,
	})
}

type TMQCommitOffsetReq struct {
	ReqID    uint64 `json:"req_id"`
	Topic    string `json:"topic"`
	VgroupID int32  `json:"vgroup_id"`
	Offset   int64  `json:"offset"`
}

type TMQCommitOffsetResp struct {
	Code     int    `json:"code"`
	Message  string `json:"message"`
	Action   string `json:"action"`
	ReqID    uint64 `json:"req_id"`
	Timing   int64  `json:"timing"`
	Topic    string `json:"topic"`
	VgroupID int32  `json:"vgroup_id"`
	Offset   int64  `json:"offset"`
}

func (t *TMQ) commitOffset(ctx context.Context, session *melody.Session, req *TMQCommitOffsetReq) {
	action := TMQCommitOffset
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("commit offset request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, nil)
		return
	}

	code, closed := t.wrapperCommitOffset(logger, log.IsDebug(), req.Topic, req.VgroupID, req.Offset)
	if closed {
		logger.Trace("server closed")
		return
	}
	if code != 0 {
		errStr := wrapper.TMQErr2Str(code)
		logger.Errorf("tmq commit offset error, code:%d, msg:%s", code, errStr)
		wsTMQErrorMsg(ctx, session, logger, int(code), errStr, action, req.ReqID, nil)
		return
	}

	wstool.WSWriteJson(session, logger, TMQCommitOffsetResp{
		Action:   action,
		ReqID:    req.ReqID,
		Timing:   wstool.GetDuration(ctx),
		Topic:    req.Topic,
		VgroupID: req.VgroupID,
		Offset:   req.Offset,
	})
}

func (t *TMQ) wrapperCloseConsumer(logger *logrus.Entry, isDebug bool, consumer unsafe.Pointer) int32 {
	logger.Tracef("call tmq_consumer_close, consumer:%p", consumer)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Debugf("tmq_consumer_close get async locker cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQConsumerCloseA(t.thread, consumer, t.handler.Handler)
	code := <-t.handler.Caller.ConsumerCloseResult
	t.asyncLocker.Unlock()
	logger.Debugf("tmq_consumer_close finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	return code
}

func (t *TMQ) wrapperSubscribe(logger *logrus.Entry, isDebug bool, consumer, topicList unsafe.Pointer) int32 {
	logger.Tracef("call tmq_subscribe, consumer:%p", consumer)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Debugf("tmq_subscribe get async lock cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQSubscribeA(t.thread, consumer, topicList, t.handler.Handler)
	errCode := <-t.handler.Caller.SubscribeResult
	t.asyncLocker.Unlock()
	logger.Debugf("tmq_subscribe finish, code:%d, cost:%s", errCode, log.GetLogDuration(isDebug, s))
	return errCode
}

func (t *TMQ) wrapperConsumerNew(logger *logrus.Entry, isDebug bool, tmqConfig unsafe.Pointer) (consumer unsafe.Pointer, err error) {
	logger.Tracef("call tmq_consumer_new, tmqConfig:%p", tmqConfig)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Debugf("tmq_consumer_new get async lock cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQNewConsumerA(t.thread, tmqConfig, t.handler.Handler)
	result := <-t.handler.Caller.NewConsumerResult
	logger.Tracef("new consumer result %x", uintptr(result.Consumer))
	if len(result.ErrStr) > 0 {
		err = taoserrors.NewError(-1, result.ErrStr)
	}
	if result.Consumer == nil {
		err = taoserrors.NewError(-1, "new consumer return nil")
	}
	t.asyncLocker.Unlock()
	logger.Debugf("tmq_consumer_new finish, consumer:%p, err:%v, cost:%s", result.Consumer, err, log.GetLogDuration(isDebug, s))
	return result.Consumer, err
}

func (t *TMQ) wrapperCommit(logger *logrus.Entry, isDebug bool) (int32, bool) {
	logger.Tracef("call tmq_commit_sync, consumer:%p", t.consumer)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return 0, true
	}
	s := log.GetLogNow(isDebug)
	logger.Debugf("tmq_commit_sync get async lock cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQCommitA(t.thread, t.consumer, nil, t.handler.Handler)
	errCode := <-t.handler.Caller.CommitResult
	t.asyncLocker.Unlock()
	logger.Debugf("tmq_commit_sync finish, code:%d, cost:%s", errCode, log.GetLogDuration(isDebug, s))
	return errCode, false
}

func (t *TMQ) wrapperPoll(logger *logrus.Entry, isDebug bool, blockingTime int64) (unsafe.Pointer, bool) {
	logger.Tracef("call tmq_poll, consumer:%p", t.consumer)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return nil, true
	}
	logger.Debugf("tmq_poll get async lock cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQPollA(t.thread, t.consumer, blockingTime, t.handler.Handler)
	message := <-t.handler.Caller.PollResult
	t.asyncLocker.Unlock()
	logger.Debugf("tmq_poll finish, res:%p, cost:%s", message, log.GetLogDuration(isDebug, s))
	return message, false
}

func (t *TMQ) wrapperFreeResult(logger *logrus.Entry, isDebug bool) bool {
	logger.Tracef("call free result, consumer:%p", t.consumer)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return true
	}
	if t.tmpMessage.CPointer == nil {
		return false
	}
	logger.Debugf("free result get async lock cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQFreeResultA(t.thread, t.tmpMessage.CPointer, t.handler.Handler)
	<-t.handler.Caller.FreeResult
	t.asyncLocker.Unlock()
	logger.Debugf("free result finish, cost:%s", log.GetLogDuration(isDebug, s))
	return false
}

func (t *TMQ) wrapperFetchRawBlock(logger *logrus.Entry, isDebug bool, res unsafe.Pointer) (*tmqhandle.FetchRawBlockResult, bool) {
	logger.Tracef("call fetch_raw_block, res:%p", res)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return nil, true
	}
	logger.Debugf("fetch_raw_block get async lock cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQFetchRawBlockA(t.thread, res, t.handler.Handler)
	rawBlock := <-t.handler.Caller.FetchRawBlockResult
	t.asyncLocker.Unlock()
	logger.Debugf("fetch_raw_block finish, code:%d, block_size:%d, block:%p, cost:%s", rawBlock.Code, rawBlock.BlockSize, rawBlock.Block, log.GetLogDuration(isDebug, s))
	return rawBlock, false
}

func (t *TMQ) wrapperGetRaw(logger *logrus.Entry, isDebug bool, res unsafe.Pointer, rawData unsafe.Pointer) (int32, bool) {
	logger.Tracef("call tmq_get_raw, res:%p, rawData:%p", res, rawData)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return 0, true
	}
	logger.Debugf("tmq_get_raw get async lock cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQGetRawA(t.thread, res, rawData, t.handler.Handler)
	errCode := <-t.handler.Caller.GetRawResult
	t.asyncLocker.Unlock()
	logger.Debugf("tmq_get_raw finish, code:%d, cost:%s", errCode, log.GetLogDuration(isDebug, s))
	return errCode, false
}

func (t *TMQ) wrapperGetJsonMeta(logger *logrus.Entry, isDebug bool, res unsafe.Pointer) (unsafe.Pointer, bool) {
	logger.Tracef("call tmq_get_json_meta, result:%p", res)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return nil, true
	}
	logger.Debugf("tmq_get_json_meta get async lock cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQGetJsonMetaA(t.thread, res, t.handler.Handler)
	jsonMeta := <-t.handler.Caller.GetJsonMetaResult
	t.asyncLocker.Unlock()
	logger.Debugf("tmq_get_json_meta finish, result:%p, cost:%s", jsonMeta, log.GetLogDuration(isDebug, s))
	return jsonMeta, false
}

func (t *TMQ) wrapperUnsubscribe(logger *logrus.Entry, isDebug bool) (int32, bool) {
	logger.Tracef("call tmq_unsubscribe, consumer:%p", t.consumer)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return 0, true
	}
	logger.Debugf("unsubscribe get async lock cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQUnsubscribeA(t.thread, t.consumer, t.handler.Handler)
	errCode := <-t.handler.Caller.UnsubscribeResult
	t.asyncLocker.Unlock()
	logger.Debugf("tmq_unsubscribe finish, code:%d, cost:%s", errCode, log.GetLogDuration(isDebug, s))
	return errCode, false
}

func (t *TMQ) wrapperGetTopicAssignment(logger *logrus.Entry, isDebug bool, topic string) (*tmqhandle.GetTopicAssignmentResult, bool) {
	logger.Tracef("call get_topic_assignment, consumer:%p, topic:%s", t.consumer, topic)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return nil, true
	}
	logger.Debugf("get_topic_assignment get async lock cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQGetTopicAssignmentA(t.thread, t.consumer, topic, t.handler.Handler)
	result := <-t.handler.Caller.GetTopicAssignmentResult
	t.asyncLocker.Unlock()
	logger.Debugf("get_topic_assignment finish, result:%+v, cost:%s", result, log.GetLogDuration(isDebug, s))
	return result, false
}

func (t *TMQ) wrapperOffsetSeek(logger *logrus.Entry, isDebug bool, topic string, vgroupID int32, offset int64) (int32, bool) {
	logger.Tracef("call offset_seek, comsumer:%p, topic:%s, vgroup_id:%d, offset:%d", t.consumer, topic, vgroupID, offset)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return 0, true
	}
	logger.Debugf("offset_seek get async lock cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQOffsetSeekA(t.thread, t.consumer, topic, vgroupID, offset, t.handler.Handler)
	errCode := <-t.handler.Caller.OffsetSeekResult
	t.asyncLocker.Unlock()
	logger.Debugf("offset_seek finish, code:%d, cost:%s", errCode, log.GetLogDuration(isDebug, s))
	return errCode, false
}

func (t *TMQ) wrapperCommitted(logger *logrus.Entry, isDebug bool, topic string, vgroupID int32) (int64, bool) {
	logger.Tracef("call tmq_committed, consumer:%p, topic:%s, vgroup_id:%d", t.consumer, topic, vgroupID)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return 0, true
	}
	logger.Debugf("tmq_committed get async lock cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQCommitted(t.thread, t.consumer, topic, vgroupID, t.handler.Handler)
	offset := <-t.handler.Caller.CommittedResult
	t.asyncLocker.Unlock()
	logger.Debugf("tmq_committed finish, offset:%d, cost:%s", offset, log.GetLogDuration(isDebug, s))
	return offset, false
}

func (t *TMQ) wrapperPosition(logger *logrus.Entry, isDebug bool, topic string, vgroupID int32) (int64, bool) {
	logger.Tracef("call tmq_position, consumer:%p, topic:%s, vgroup_id:%d", t.consumer, topic, vgroupID)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return 0, true
	}
	logger.Debugf("tmq_position get async lock cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQPosition(t.thread, t.consumer, topic, vgroupID, t.handler.Handler)
	res := <-t.handler.Caller.PositionResult
	t.asyncLocker.Unlock()
	logger.Debugf("tmq_position finish, position:%d, cost:%s", res, log.GetLogDuration(isDebug, s))
	return res, false
}

func (t *TMQ) wrapperCommitOffset(logger *logrus.Entry, isDebug bool, topic string, vgroupID int32, offset int64) (int32, bool) {
	logger.Tracef("call commit_offset, consumer:%p, topic:%s, vgroup_id:%d, offset:%d", t.consumer, topic, vgroupID, offset)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return 0, true
	}
	logger.Debugf("commit_offset get async lock cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQCommitOffset(t.thread, t.consumer, topic, vgroupID, offset, t.handler.Handler)
	code := <-t.handler.Caller.CommitResult
	t.asyncLocker.Unlock()
	logger.Debugf("commit_offset finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	return code, false
}

/*
	Flag           uint64 //8               0
	Action         uint64 //8               8
	Version        uint16 //2               16
	Time           uint64 //8               18
	ReqID          uint64 //8               26
	Code           uint32 //4               34
	MessageLen     uint32 //4               38
	Message        string //MessageLen      42
	MessageID      uint64 //8               42 + MessageLen
	MetaType       uint16 //2               50 + MessageLen
	RawBlockLength uint32 //4               52 + MessageLen
	TMQRawBlock    []byte //RawBlockLength  56 + MessageLen + RawBlockLength
*/

func tmqFetchRawBlockErrorMsg(ctx context.Context, session *melody.Session, logger *logrus.Entry, code int, message string, reqID uint64, messageID uint64) {
	bufLength := 8 + 8 + 2 + 8 + 8 + 4 + 4 + len(message) + 8
	buf := make([]byte, bufLength)
	binary.LittleEndian.PutUint64(buf, 0xffffffffffffffff)
	binary.LittleEndian.PutUint64(buf[8:], uint64(TMQFetchRawNewMessage))
	binary.LittleEndian.PutUint16(buf[16:], 1)
	binary.LittleEndian.PutUint64(buf[18:], uint64(wstool.GetDuration(ctx)))
	binary.LittleEndian.PutUint64(buf[26:], reqID)
	binary.LittleEndian.PutUint32(buf[34:], uint32(code&0xffff))
	binary.LittleEndian.PutUint32(buf[38:], uint32(len(message)))
	copy(buf[42:], message)
	binary.LittleEndian.PutUint64(buf[42+len(message):], messageID)
	wstool.WSWriteBinary(session, buf, logger)
}

func wsFetchRawBlockMessage(ctx context.Context, buf []byte, reqID uint64, resultID uint64, MetaType uint16, blockLength uint32, rawBlock unsafe.Pointer) []byte {
	bufLength := 8 + 8 + 2 + 8 + 8 + 4 + 4 + 8 + 2 + 4 + int(blockLength)
	if cap(buf) < bufLength {
		buf = make([]byte, 0, bufLength)
	}
	buf = buf[:bufLength]
	binary.LittleEndian.PutUint64(buf, 0xffffffffffffffff)
	binary.LittleEndian.PutUint64(buf[8:], uint64(TMQFetchRawNewMessage))
	binary.LittleEndian.PutUint16(buf[16:], 1)
	binary.LittleEndian.PutUint64(buf[18:], uint64(wstool.GetDuration(ctx)))
	binary.LittleEndian.PutUint64(buf[26:], reqID)
	binary.LittleEndian.PutUint32(buf[34:], 0)
	binary.LittleEndian.PutUint32(buf[38:], 0)
	binary.LittleEndian.PutUint64(buf[42:], resultID)
	binary.LittleEndian.PutUint16(buf[50:], MetaType)
	binary.LittleEndian.PutUint32(buf[52:], blockLength)
	bytesutil.Copy(rawBlock, buf, 56, int(blockLength))
	return buf
}

func init() {
	c := NewTMQController()
	controller.AddController(c)
}

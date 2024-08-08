package tmq

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/huskar-t/melody"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/parser"
	taoserrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/asynctmq"
	"github.com/taosdata/taosadapter/v3/db/asynctmq/tmqhandle"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/thread"
	"github.com/taosdata/taosadapter/v3/tools/bytesutil"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
	"github.com/taosdata/taosadapter/v3/tools/jsontype"
)

type TMQController struct {
	tmqM *melody.Melody
}

func NewTMQController() *TMQController {
	tmqM := melody.New()
	tmqM.UpGrader.EnableCompression = true
	tmqM.Config.MaxMessageSize = 0

	tmqM.HandleConnect(func(session *melody.Session) {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws connect")
		session.Set(TaosTMQKey, NewTaosTMQ(session))
	})

	tmqM.HandleMessage(func(session *melody.Session, data []byte) {
		if tmqM.IsClosed() {
			return
		}
		t := session.MustGet(TaosTMQKey).(*TMQ)
		if t.isClosed() {
			return
		}
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			ctx := context.WithValue(context.Background(), wstool.StartTimeKey, time.Now().UnixNano())
			logger := session.MustGet("logger").(*logrus.Entry)
			logger.Debugln("get ws message data:", string(data))
			var action wstool.WSAction
			err := json.Unmarshal(data, &action)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal ws request")
				return
			}
			switch action.Action {
			case wstool.ClientVersion:
				session.Write(wstool.VersionResp)
			case TMQSubscribe:
				var req TMQSubscribeReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal subscribe args")
					return
				}
				t.subscribe(ctx, session, &req)
			case TMQPoll:
				var req TMQPollReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal pool args")
					return
				}
				t.poll(ctx, session, &req)
			case TMQFetch:
				var req TMQFetchReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal fetch args")
					return
				}
				t.fetch(ctx, session, &req)
			case TMQFetchBlock:
				var req TMQFetchBlockReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal fetch block args")
					return
				}
				t.fetchBlock(ctx, session, &req)
			case TMQCommit:
				var req TMQCommitReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal commit args")
					return
				}
				t.commit(ctx, session, &req)
			case TMQFetchJsonMeta:
				var req TMQFetchJsonMetaReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal fetch json meta args")
					return
				}
				t.fetchJsonMeta(ctx, session, &req)
			case TMQFetchRaw:
				var req TMQFetchRawReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal fetch raw meta args")
					return
				}
				t.fetchRawBlock(ctx, session, &req)
			case TMQFetchRawNew:
				var req TMQFetchRawReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal fetch raw meta args")
					return
				}
				t.fetchRawBlockNew(ctx, session, &req)
			case TMQUnsubscribe:
				var req TMQUnsubscribeReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal unsubscribe args")
					return
				}
				t.unsubscribe(ctx, session, &req)
			case TMQGetTopicAssignment:
				var req TMQGetTopicAssignmentReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal unsubscribe args")
					return
				}
				t.assignment(ctx, session, &req)
			case TMQSeek:
				var req TMQOffsetSeekReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal unsubscribe args")
					return
				}
				t.offsetSeek(ctx, session, &req)
			case TMQCommitted:
				var req TMQCommittedReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal committed args")
					return
				}
				session.MustGet(TaosTMQKey).(*TMQ).committed(ctx, session, &req)
			case TMQPosition:
				var req TMQPositionReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal position args")
					return
				}
				session.MustGet(TaosTMQKey).(*TMQ).position(ctx, session, &req)
			case TMQListTopics:
				var req TMQListTopicsReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal list topics args")
					return
				}
				session.MustGet(TaosTMQKey).(*TMQ).listTopics(ctx, session, &req)
			case TMQCommitOffset:
				var req TMQCommitOffsetReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal commit offset args")
					return
				}
				session.MustGet(TaosTMQKey).(*TMQ).commitOffset(ctx, session, &req)
			default:
				logger.WithError(err).Errorln("unknown action: " + action.Action)
				return
			}
		}()
	})
	tmqM.HandleClose(func(session *melody.Session, i int, s string) error {
		//message := melody.FormatCloseMessage(i, "")
		//session.WriteControl(websocket.CloseMessage, message, time.Now().Add(time.Second))
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws close", i, s)
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
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws disconnect")
		t, exist := session.Get(TaosTMQKey)
		if exist && t != nil {
			t.(*TMQ).Close(logger)
		}
	})
	return &TMQController{tmqM: tmqM}
}

func (s *TMQController) Init(ctl gin.IRouter) {
	ctl.GET("rest/tmq", func(c *gin.Context) {
		sessionID := common.GetReqID()
		logger := log.GetLogger("ws").WithFields(logrus.Fields{
			"wsType":            "tmq",
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
			if t.isClosed() {
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
			t.Close(t.logger)
			logger.Debugln("close handler cost:", log.GetLogDuration(isDebug, s))
			return
		case <-t.whitelistChangeChan:
			logger.Info("get whitelist change signal")
			isDebug := log.IsDebug()
			s := log.GetLogNow(isDebug)
			t.lock(logger, isDebug)
			if t.isClosed() {
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
				t.Close(t.logger)
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
				t.Close(t.logger)
				logger.Debugln("close handler cost:", log.GetLogDuration(isDebug, s))
				return
			}
			t.Unlock()
		case <-t.exit:
			return
		}
	}
}

func (t *TMQ) lock(logger *logrus.Entry, isDebug bool) {
	s := log.GetLogNow(isDebug)
	logger.Traceln("get handler lock")
	t.Lock()
	logger.Debugln("get handler lock cost:", log.GetLogDuration(isDebug, s))
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
}

type TMQSubscribeResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func (t *TMQ) subscribe(ctx context.Context, session *melody.Session, req *TMQSubscribeReq) {
	logger := t.logger.WithField("action", TMQSubscribe).WithField(config.ReqIDKey, req.ReqID)
	ctx = context.WithValue(ctx, LoggerKey, logger)
	isDebug := log.IsDebug()
	logger.Tracef("subscribe request: %+v\n", req)
	// lock for consumer and unsubscribed
	// used for subscribe,unsubscribe and
	t.lock(logger, isDebug)
	defer t.Unlock()
	if t.isClosed() {
		logger.Traceln("server closed")
		return
	}
	if t.consumer != nil {
		if t.unsubscribed {
			logger.Traceln("tmq resubscribe")
			topicList := wrapper.TMQListNew()
			defer func() {
				wrapper.TMQListDestroy(topicList)
			}()
			for _, topic := range req.Topics {
				errCode := wrapper.TMQListAppend(topicList, topic)
				if errCode != 0 {
					errStr := wrapper.TMQErr2Str(errCode)
					logger.Errorf("tmq list append error, code: %d, err: %s", errCode, errStr)
					t.wrapperCloseConsumer(logger, isDebug)
					wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
					return
				}
			}
			errCode := t.wrapperSubscribe(logger, isDebug, topicList)
			if errCode != 0 {
				errStr := wrapper.TMQErr2Str(errCode)
				wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
				return
			}
			t.unsubscribed = false
			wstool.WSWriteJson(session, &TMQSubscribeResp{
				Action: TMQSubscribe,
				ReqID:  req.ReqID,
				Timing: wstool.GetDuration(ctx),
			})
			return
		} else {
			logger.Errorf("tmq should have unsubscribed first")
			wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq should have unsubscribed first", TMQSubscribe, req.ReqID, nil)
			return
		}
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
			logger.Errorf("parse auto commit: %s as bool error: %s", req.AutoCommit, err.Error())
			wsTMQErrorMsg(ctx, session, logger, 0xffff, err.Error(), TMQSubscribe, req.ReqID, nil)
			return
		}
	}
	if len(req.AutoCommitIntervalMS) != 0 {
		autocommitIntervalMS, err := strconv.ParseInt(req.AutoCommitIntervalMS, 10, 64)
		if err != nil {
			logger.Errorf("parse auto commit interval: %s as int error: %s", req.AutoCommitIntervalMS, err.Error())
			wsTMQErrorMsg(ctx, session, logger, 0xffff, err.Error(), TMQSubscribe, req.ReqID, nil)
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
	var errCode int32
	for k, v := range tmqOptions {
		errCode = wrapper.TMQConfSet(tmqConfig, k, v)
		if errCode != httperror.SUCCESS {
			errStr := wrapper.TMQErr2Str(errCode)
			logger.Errorf("tmq conf set error,k: %s, v: %s, code: %d, err: %s", k, v, errCode, errStr)
			wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
	}
	cPointer, err := t.wrapperConsumerNew(logger, isDebug, tmqConfig)
	if err != nil {
		logger.Errorf("tmq consumer new error: %s", err.Error())
		wsTMQErrorMsg(ctx, session, logger, 0xffff, err.Error(), TMQSubscribe, req.ReqID, nil)
		return
	}
	conn := wrapper.TMQGetConnect(cPointer)
	logger.Traceln("get whitelist")
	whitelist, err := tool.GetWhitelist(conn)
	if err != nil {
		logger.Errorf("get whitelist error: %s", err.Error())
		t.wrapperCloseConsumer(logger, isDebug)
		wstool.WSError(ctx, session, err, TMQSubscribe, req.ReqID)
		return
	}
	logger.Tracef("check whitelist, ip: %d, whitelist: %s\n", t.ip, tool.IpNetSliceToString(whitelist))
	valid := tool.CheckWhitelist(whitelist, t.ip)
	if !valid {
		logger.Errorf("whitelist prohibits current IP access, ip: %d, whitelist: %s\n", t.ip, tool.IpNetSliceToString(whitelist))
		t.wrapperCloseConsumer(logger, isDebug)
		wstool.WSErrorMsg(ctx, session, 0xffff, "whitelist prohibits current IP access", TMQSubscribe, req.ReqID)
		return
	}
	logger.Traceln("register change whitelist")
	err = tool.RegisterChangeWhitelist(conn, t.whitelistChangeHandle)
	if err != nil {
		logger.Error("register change whitelist error: ", err)
		t.wrapperCloseConsumer(logger, isDebug)
		wstool.WSError(ctx, session, err, TMQSubscribe, req.ReqID)
		return
	}
	logger.Traceln("register drop user")
	err = tool.RegisterDropUser(conn, t.dropUserHandle)
	if err != nil {
		logger.Errorf("register drop user error: %s", err.Error())
		t.wrapperCloseConsumer(logger, isDebug)
		wstool.WSError(ctx, session, err, TMQSubscribe, req.ReqID)
		return
	}
	t.conn = conn
	logger.Traceln("start to wait signal")
	go t.waitSignal(t.logger)
	topicList := wrapper.TMQListNew()
	defer func() {
		wrapper.TMQListDestroy(topicList)
	}()
	for _, topic := range req.Topics {
		logger.Tracef("tmq append topic: %s", topic)
		errCode = wrapper.TMQListAppend(topicList, topic)
		if errCode != 0 {
			errStr := wrapper.TMQErr2Str(errCode)
			logger.Errorf("tmq list append error,tpic: %s, code: %d, err: %s", topic, errCode, errStr)
			t.wrapperCloseConsumer(logger, isDebug)
			wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
	}

	errCode = t.wrapperSubscribe(logger, isDebug, topicList)
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		logger.Errorf("tmq subscribe error:%d %s", errCode, errStr)
		t.wrapperCloseConsumer(logger, isDebug)
		wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
		return
	}
	t.consumer = cPointer
	wstool.WSWriteJson(session, &TMQSubscribeResp{
		Action: TMQSubscribe,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
	})
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
	logger := t.logger.WithField("action", TMQCommit).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("commit request: %+v\n", req)
	if t.consumer == nil {
		logger.Errorln("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", TMQCommit, req.ReqID, nil)
		return
	}
	// commit all
	isDebug := log.IsDebug()
	errCode, closed := t.wrapperCommit(logger, isDebug)
	if closed {
		logger.Traceln("server closed")
		return
	}
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, TMQCommit, req.ReqID, nil)
		return
	}
	resp := &TMQCommitResp{
		Action:    TMQCommit,
		ReqID:     req.ReqID,
		MessageID: req.MessageID,
		Timing:    wstool.GetDuration(ctx),
	}
	wstool.WSWriteJson(session, resp)
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
	logger := t.logger.WithField("action", TMQPoll).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("poll request: %+v\n", req)
	if t.consumer == nil {
		logger.Errorln("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", TMQPoll, req.ReqID, nil)
		return
	}
	now := time.Now()
	isDebug := log.IsDebug()
	if t.isAutoCommit && now.After(t.nextTime) {
		errCode, closed := t.wrapperCommit(logger, isDebug)
		if closed {
			logger.Traceln("server closed")
			return
		}
		if errCode != 0 {
			errStr := wrapper.TMQErr2Str(errCode)
			logger.Errorln("tmq autocommit error:", taoserrors.NewError(int(errCode), errStr))
		}
		t.nextTime = now.Add(t.autocommitInterval)
	}
	message, closed := t.wrapperPoll(logger, isDebug, req.BlockingTime)
	if closed {
		logger.Traceln("server closed")
	}
	resp := &TMQPollResp{
		Action: TMQPoll,
		ReqID:  req.ReqID,
	}
	if message != nil {
		messageType := wrapper.TMQGetResType(message)
		if messageTypeIsValid(messageType) {
			t.tmpMessage.Lock()
			defer t.tmpMessage.Unlock()
			if t.tmpMessage.CPointer != nil {
				logger.Traceln("free old message")
				closed = t.wrapperFreeResult(logger, isDebug)
				if closed {
					logger.Tracef("server closed,free result directly %d\n", message)
					thread.Lock()
					wrapper.TaosFreeResult(message)
					thread.Unlock()
					logger.Tracef("free result directly finish")
					return
				}
			}
			logger.Traceln("")
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
			logger.Tracef("get message %d, topic: %s, vgroup: %d, offset: %d, db: %s", uintptr(message), t.tmpMessage.Topic, t.tmpMessage.VGroupID, t.tmpMessage.Offset, resp.Database)
		} else {
			logger.Errorf("unavailable tmq type: %d\n", messageType)
			logger.Tracef("unavailable tmq type,free result directly %d\n", message)
			thread.Lock()
			wrapper.TaosFreeResult(message)
			thread.Unlock()
			logger.Tracef("free result directly finish")
			wsTMQErrorMsg(ctx, session, logger, 0xffff, "unavailable tmq type:"+strconv.Itoa(int(messageType)), TMQPoll, req.ReqID, nil)
			return
		}
	}

	resp.Timing = wstool.GetDuration(ctx)
	wstool.WSWriteJson(session, resp)
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
	logger := t.logger.WithField("action", TMQFetch).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("fetch request: %+v\n", req)
	if t.consumer == nil {
		logger.Errorln("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", TMQFetch, req.ReqID, &req.MessageID)
		return
	}
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.tmpMessage.Lock()
	defer t.tmpMessage.Unlock()
	logger.Debugln("get message lock cost:", log.GetLogDuration(isDebug, s))
	if t.tmpMessage.CPointer == nil {
		logger.Errorln("message has been freed")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message has been freed", TMQFetch, req.ReqID, &req.MessageID)
		return
	}
	message := t.tmpMessage
	if message.Index != req.MessageID {
		logger.Errorf("message ID are not equal, req: %d, message: %d", req.MessageID, message.Index)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message ID are not equal", TMQFetch, req.ReqID, &req.MessageID)
		return
	}

	if !canGetData(message.Type) {
		logger.Errorf("message type is not data, type: %d", message.Type)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message type is not data", TMQFetch, req.ReqID, &req.MessageID)
		return
	}
	rawBlock, closed := t.wrapperFetchRawBlock(logger, isDebug, message.CPointer)
	if closed {
		logger.Traceln("server closed")
		return
	}
	errCode := rawBlock.Code
	blockSize := rawBlock.BlockSize
	block := rawBlock.Block
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(int32(errCode))
		logger.Errorf("tmq fetch raw block error: %d, %s", errCode, errStr)
		wsTMQErrorMsg(ctx, session, logger, errCode, errStr, TMQFetch, req.ReqID, &req.MessageID)
		return
	}
	resp := &TMQFetchResp{
		Action:    TMQFetch,
		ReqID:     req.ReqID,
		MessageID: req.MessageID,
	}
	if blockSize == 0 {
		resp.Completed = true
		wstool.WSWriteJson(session, resp)
		return
	}
	s = log.GetLogNow(isDebug)
	resp.TableName = wrapper.TMQGetTableName(message.CPointer)
	logger.Debugln("tmq_get_table_name cost:", log.GetLogDuration(isDebug, s))
	resp.Rows = blockSize
	s = log.GetLogNow(isDebug)
	resp.FieldsCount = wrapper.TaosNumFields(message.CPointer)
	logger.Debugln("num_fields cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	rowsHeader, _ := wrapper.ReadColumn(message.CPointer, resp.FieldsCount)
	logger.Debugln("read column cost:", log.GetLogDuration(isDebug, s))
	resp.FieldsNames = rowsHeader.ColNames
	resp.FieldsTypes = rowsHeader.ColTypes
	resp.FieldsLengths = rowsHeader.ColLength
	s = log.GetLogNow(isDebug)
	resp.Precision = wrapper.TaosResultPrecision(message.CPointer)
	logger.Debugln("result_precision cost:", log.GetLogDuration(isDebug, s))
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
	logger.Debugln("handle data cost:", log.GetLogDuration(isDebug, s))
	wstool.WSWriteJson(session, resp)
}

type TMQFetchBlockReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}

func (t *TMQ) fetchBlock(ctx context.Context, session *melody.Session, req *TMQFetchBlockReq) {
	logger := t.logger.WithField("action", TMQFetchBlock).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("fetch block request: %+v\n", req)
	if t.consumer == nil {
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", TMQFetchBlock, req.ReqID, &req.MessageID)
		return
	}
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.tmpMessage.Lock()
	defer t.tmpMessage.Unlock()
	logger.Debugln("get message lock cost:", log.GetLogDuration(isDebug, s))
	if t.tmpMessage.CPointer == nil {
		logger.Errorf("message has been freed")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message has been freed", TMQFetchBlock, req.ReqID, &req.MessageID)
		return
	}
	message := t.tmpMessage
	if message.Index != req.MessageID {
		logger.Errorf("message ID are not equal, req: %d, message: %d", req.MessageID, message.Index)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message is nil", TMQFetchBlock, req.ReqID, &req.MessageID)
		return
	}
	if !canGetData(message.Type) {
		logger.Errorf("message type is not data, type: %d", message.Type)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message type is not data", TMQFetchBlock, req.ReqID, &req.MessageID)
		return
	}
	if message.buffer == nil || len(message.buffer) == 0 {
		logger.Errorf("no fetch data")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "no fetch data", TMQFetchBlock, req.ReqID, &req.MessageID)
		return
	}
	s = log.GetLogNow(isDebug)
	binary.LittleEndian.PutUint64(message.buffer, uint64(wstool.GetDuration(ctx)))
	logger.Debugln("handle data cost:", log.GetLogDuration(isDebug, s))
	session.WriteBinary(message.buffer)
}

type TMQFetchRawReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}

func (t *TMQ) fetchRawBlock(ctx context.Context, session *melody.Session, req *TMQFetchRawReq) {
	logger := t.logger.WithField("action", TMQFetchRaw).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("fetch raw request: %+v\n", req)
	if t.consumer == nil {
		logger.Errorf("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", TMQFetchRaw, req.ReqID, &req.MessageID)
		return
	}
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.tmpMessage.Lock()
	defer t.tmpMessage.Unlock()
	logger.Debugln("get message lock cost:", log.GetLogDuration(isDebug, s))
	if t.tmpMessage.CPointer == nil {
		logger.Errorln("message has been freed")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message has been freed", TMQFetchRaw, req.ReqID, &req.MessageID)
		return
	}
	message := t.tmpMessage
	if message.Index != req.MessageID {
		logger.Errorf("message ID are not equal, req: %d, message: %d", req.MessageID, message.Index)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message ID is not equal", TMQFetchRaw, req.ReqID, &req.MessageID)
		return
	}
	rawData := asynctmq.TaosaInitTMQRaw()
	defer asynctmq.TaosaFreeTMQRaw(rawData)
	errCode, closed := t.wrapperGetRaw(logger, isDebug, message.CPointer, rawData)
	if closed {
		logger.Traceln("server closed")
		return
	}
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		logger.Errorf("tmq get raw error: %d, %s", errCode, errStr)
		wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, TMQFetchRaw, req.ReqID, &req.MessageID)
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
	logger.Debugln("tmq_free_raw cost:", log.GetLogDuration(isDebug, s1))
	logger.Debugln("handle binary data cost:", log.GetLogDuration(isDebug, s))
	session.WriteBinary(message.buffer)
}

func (t *TMQ) fetchRawBlockNew(ctx context.Context, session *melody.Session, req *TMQFetchRawReq) {
	action := TMQFetchRawNew
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("fetch raw request: %+v\n", req)
	if t.consumer == nil {
		logger.Traceln("tmq not init")
		tmqFetchRawBlockErrorMsg(ctx, session, 0xffff, "tmq not init", req.ReqID, req.MessageID)
		return
	}
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.tmpMessage.Lock()
	defer t.tmpMessage.Unlock()
	logger.Debugln("get message lock cost:", log.GetLogDuration(isDebug, s))
	if t.tmpMessage.CPointer == nil {
		logger.Errorln("message has been freed")
		tmqFetchRawBlockErrorMsg(ctx, session, 0xffff, "message has been freed", req.ReqID, req.MessageID)
		return
	}
	message := t.tmpMessage
	if message.Index != req.MessageID {
		logger.Errorf("message ID are not equal, req: %d, message: %d", req.MessageID, message.Index)
		tmqFetchRawBlockErrorMsg(ctx, session, 0xffff, "message ID is not equal", req.ReqID, req.MessageID)
		return
	}
	s = log.GetLogNow(isDebug)
	rawData := asynctmq.TaosaInitTMQRaw()
	defer asynctmq.TaosaFreeTMQRaw(rawData)
	errCode, closed := t.wrapperGetRaw(logger, isDebug, message.CPointer, rawData)
	if closed {
		logger.Traceln("server closed")
		return
	}
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		logger.Errorf("tmq get raw error: %d, %s", errCode, errStr)
		tmqFetchRawBlockErrorMsg(ctx, session, int(errCode), errStr, req.ReqID, req.MessageID)
		return
	}
	s = log.GetLogNow(isDebug)
	length, metaType, data := wrapper.ParseRawMeta(rawData)
	message.buffer = wsFetchRawBlockMessage(ctx, message.buffer, req.ReqID, req.MessageID, metaType, length, data)
	s1 := log.GetLogNow(isDebug)
	wrapper.TMQFreeRaw(rawData)
	logger.Debugln("tmq_free_raw cost:", log.GetLogDuration(isDebug, s1))
	logger.Debugln("handle binary data cost:", log.GetLogDuration(isDebug, s))
	session.WriteBinary(message.buffer)
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
	logger := t.logger.WithField("action", TMQFetchJsonMeta).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("fetch json meta request: %+v\n", req)
	if t.consumer == nil {
		logger.Errorln("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", TMQFetchJsonMeta, req.ReqID, &req.MessageID)
		return
	}
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.tmpMessage.Lock()
	defer t.tmpMessage.Unlock()
	logger.Debugln("get message lock cost:", log.GetLogDuration(isDebug, s))
	if t.tmpMessage.CPointer == nil {
		logger.Errorln("message has been freed")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message has been freed", TMQFetchJsonMeta, req.ReqID, &req.MessageID)
		return
	}
	message := t.tmpMessage
	if message.Index != req.MessageID {
		logger.Errorf("message ID are not equal, req: %d, message: %d", req.MessageID, message.Index)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message ID is not equal", TMQFetchJsonMeta, req.ReqID, &req.MessageID)
		return
	}
	jsonMeta, closed := t.wrapperGetJsonMeta(logger, isDebug, message.CPointer)
	if closed {
		logger.Traceln("server closed")
		return
	}
	resp := TMQFetchJsonMetaResp{
		Action:    TMQFetchJsonMeta,
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
	logger.Debugln("tmq_free_json_meta cost:", log.GetLogDuration(isDebug, s))
	resp.Timing = wstool.GetDuration(ctx)
	wstool.WSWriteJson(session, resp)
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
	logger := t.logger.WithField("action", TMQUnsubscribe).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("unsubscribe request: %+v\n", req)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.Lock()
	logger.Debugln("get global lock cost:", log.GetLogDuration(isDebug, s))
	defer t.Unlock()
	if t.isClosed() {
		logger.Traceln("server closed")
		return
	}
	if t.consumer == nil {
		logger.Errorln("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", TMQUnsubscribe, req.ReqID, nil)
		return
	}
	errCode, closed := t.wrapperUnsubscribe(logger, isDebug)
	if closed {
		logger.Traceln("server closed")
		return
	}
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		logger.WithError(taoserrors.NewError(int(errCode), errStr)).Error("tmq unsubscribe consumer")
		wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, TMQUnsubscribe, req.ReqID, nil)
		return
	}
	logger.Traceln("free all result")
	t.freeMessage(false)
	logger.Traceln("free all result finished")
	t.unsubscribed = true
	wstool.WSWriteJson(session, &TMQUnsubscribeResp{
		Action: TMQUnsubscribe,
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
	logger := t.logger.WithField("action", TMQGetTopicAssignment).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("assignment request: %+v\n", req)
	if t.consumer == nil {
		logger.Errorln("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", TMQGetTopicAssignment, req.ReqID, nil)
		return
	}
	result, closed := t.wrapperGetTopicAssignment(logger, log.IsDebug(), req.Topic)
	if closed {
		logger.Traceln("server closed")
		return
	}
	if result.Code != 0 {
		errStr := wrapper.TMQErr2Str(result.Code)
		logger.WithError(taoserrors.NewError(int(result.Code), errStr)).Error("tmq assignment")
		wsTMQErrorMsg(ctx, session, logger, int(result.Code), errStr, TMQGetTopicAssignment, req.ReqID, nil)
		return
	}
	wstool.WSWriteJson(session, TMQGetTopicAssignmentResp{
		Action:     TMQGetTopicAssignment,
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
	logger := t.logger.WithField("action", TMQSeek).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("offset seek request: %+v\n", req)
	if t.consumer == nil {
		logger.Errorln("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", TMQSeek, req.ReqID, nil)
		return
	}
	isDebug := log.IsDebug()
	errCode, closed := t.wrapperOffsetSeek(logger, isDebug, req.Topic, req.VgroupID, req.Offset)
	if closed {
		logger.Traceln("server closed")
		return
	}
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		logger.WithError(taoserrors.NewError(int(errCode), errStr)).Error("tmq offset seek")
		wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, TMQSeek, req.ReqID, nil)
		return
	}
	wstool.WSWriteJson(session, TMQOffsetSeekResp{
		Action: TMQSeek,
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
		logger.Info("tmq close end, cost:", time.Since(start).String())
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
		logger.Errorln("wait for all goroutines to exit timeout")
	case <-done:
		logger.Debugln("all goroutines exit")
	}
	isDebug := log.IsDebug()

	defer func() {
		s := log.GetLogNow(isDebug)
		t.asyncLocker.Lock()
		asynctmq.DestroyTMQThread(t.thread)
		t.asyncLocker.Unlock()
		logger.Tracef("destroy tmq thread cost: %s", log.GetLogDuration(isDebug, s))
		tmqhandle.GlobalTMQHandlerPoll.Put(t.handler)
	}()

	defer func() {
		if t.consumer != nil {
			if !t.unsubscribed {
				errCode, _ := t.wrapperUnsubscribe(logger, isDebug)
				if errCode != 0 {
					errMsg := wrapper.TMQErr2Str(errCode)
					logger.WithError(taoserrors.NewError(int(errCode), errMsg)).Error("tmq unsubscribe consumer")
				}
			}
			errCode := t.wrapperCloseConsumer(logger, log.IsDebug())
			if errCode != 0 {
				errMsg := wrapper.TMQErr2Str(errCode)
				logger.WithError(taoserrors.NewError(int(errCode), errMsg)).Error("tmq close consumer")
			}
		}
		close(t.exit)
	}()
	logger.Traceln("start to free message")
	s := log.GetLogNow(isDebug)
	t.freeMessage(true)
	logger.Debugln("free message cost:", log.GetLogDuration(isDebug, s))
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
	b, _ := json.Marshal(&WSTMQErrorResp{
		Code:      code & 0xffff,
		Message:   message,
		Action:    action,
		ReqID:     reqID,
		Timing:    wstool.GetDuration(ctx),
		MessageID: messageID,
	})
	logger.Tracef("write json: %s", b)
	session.Write(b)
}

func canGetMeta(messageType int32) bool {
	return messageType == common.TMQ_RES_TABLE_META || messageType == common.TMQ_RES_METADATA
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
	logger := t.logger.WithField("action", TMQCommitted).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("committed request: %+v\n", req)
	if t.consumer == nil {
		logger.Errorf("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", TMQCommitted, req.ReqID, nil)
		return
	}
	isDebug := log.IsDebug()
	offsets := make([]int64, 0, len(req.TopicVgroupIDs))
	for _, tv := range req.TopicVgroupIDs {
		res, closed := t.wrapperCommitted(logger, isDebug, tv.Topic, tv.VgroupID)
		if closed {
			logger.Traceln("server closed")
			return
		}
		if res < 0 && res != OffsetInvalid {
			err := taoserrors.NewError(int(res), wrapper.TMQErr2Str(int32(res)))
			logger.WithError(err).Error("tmq get committed")
			var taosErr *taoserrors.TaosError
			errors.As(err, &taosErr)
			wsTMQErrorMsg(ctx, session, logger, int(taosErr.Code), taosErr.ErrStr, TMQCommitted, req.ReqID, nil)
			return
		}
		offsets = append(offsets, res)
	}
	wstool.WSWriteJson(session, TMQCommittedResp{
		Action:    TMQCommitted,
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
	logger := t.logger.WithField("action", TMQPosition).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("position request: %+v\n", req)
	if t.consumer == nil {
		logger.Errorln("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", TMQPosition, req.ReqID, nil)
		return
	}
	isDebug := log.IsDebug()
	positions := make([]int64, 0, len(req.TopicVgroupIDs))
	for _, tv := range req.TopicVgroupIDs {
		res, closed := t.wrapperPosition(logger, isDebug, tv.Topic, tv.VgroupID)
		if closed {
			logger.Traceln("server closed")
			return
		}
		if res < 0 && res != OffsetInvalid {
			err := taoserrors.NewError(int(res), wrapper.TMQErr2Str(int32(res)))
			logger.WithError(err).Error("tmq get position")
			var taosErr *taoserrors.TaosError
			errors.As(err, &taosErr)
			wsTMQErrorMsg(ctx, session, logger, int(taosErr.Code), taosErr.ErrStr, TMQPosition, req.ReqID, nil)
			return
		}
		positions = append(positions, res)
	}

	wstool.WSWriteJson(session, TMQPositionResp{
		Action:   TMQPosition,
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
	logger := t.logger.WithField("action", TMQListTopics).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("list topics request: %+v\n", req)
	if t.consumer == nil {
		logger.Errorln("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", TMQListTopics, req.ReqID, nil)
		return
	}
	if t.isClosed() {
		logger.Traceln("server closed")
		return
	}
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	logger.Traceln("subscription get thread lock")
	thread.Lock()
	logger.Debugln("subscription get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code, topicsPointer := wrapper.TMQSubscription(t.consumer)
	thread.Unlock()
	logger.Debugln("subscription cost:", log.GetLogDuration(isDebug, s))
	defer wrapper.TMQListDestroy(topicsPointer)
	if code != 0 {
		errStr := wrapper.TMQErr2Str(code)
		logger.WithError(taoserrors.NewError(int(code), errStr)).Error("tmq list topic")
		wsTMQErrorMsg(ctx, session, logger, int(code), errStr, TMQListTopics, req.ReqID, nil)
		return
	}
	s = log.GetLogNow(isDebug)
	topics := wrapper.TMQListToCArray(topicsPointer, int(wrapper.TMQListGetSize(topicsPointer)))
	logger.Debugln("tmq list topic cost:", log.GetLogDuration(log.IsDebug(), s))
	wstool.WSWriteJson(session, TMQListTopicsResp{
		Action: TMQListTopics,
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
	logger := t.logger.WithField("action", TMQCommitOffset).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("commit offset request: %+v\n", req)
	if t.consumer == nil {
		logger.Errorln("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", TMQCommitOffset, req.ReqID, nil)
		return
	}

	code, closed := t.wrapperCommitOffset(logger, log.IsDebug(), req.Topic, req.VgroupID, req.Offset)
	if closed {
		logger.Traceln("server closed")
		return
	}
	if code != 0 {
		errMsg := wrapper.TMQErr2Str(code)
		logger.WithError(taoserrors.NewError(int(code), errMsg)).Error("tmq commit offset")
		wsTMQErrorMsg(ctx, session, logger, int(code), errMsg, TMQCommitOffset, req.ReqID, nil)
		return
	}

	wstool.WSWriteJson(session, TMQCommitOffsetResp{
		Action:   TMQCommitOffset,
		ReqID:    req.ReqID,
		Timing:   wstool.GetDuration(ctx),
		Topic:    req.Topic,
		VgroupID: req.VgroupID,
		Offset:   req.Offset,
	})
}

func (t *TMQ) wrapperCloseConsumer(logger *logrus.Entry, isDebug bool) int32 {
	t.logger.Traceln("get async locker for tmq_consumer_close")
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Debugln("tmq_consumer_close get async locker cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQConsumerCloseA(t.thread, t.consumer, t.handler.Handler)
	code := <-t.handler.Caller.ConsumerCloseResult
	t.asyncLocker.Unlock()
	logger.Debugf("tmq_consumer_close %d cost: %s", code, log.GetLogDuration(isDebug, s))
	return code
}

func (t *TMQ) wrapperSubscribe(logger *logrus.Entry, isDebug bool, topicList unsafe.Pointer) int32 {
	logger.Traceln("call tmq_subscribe")
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Debugln("tmq_subscribe get async lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQSubscribeA(t.thread, t.consumer, topicList, t.handler.Handler)
	errCode := <-t.handler.Caller.SubscribeResult
	t.asyncLocker.Unlock()
	logger.Debugf("tmq_subscribe %d cost: %s", errCode, log.GetLogDuration(isDebug, s))
	return errCode
}

func (t *TMQ) wrapperConsumerNew(logger *logrus.Entry, isDebug bool, tmqConfig unsafe.Pointer) (consumer unsafe.Pointer, err error) {
	logger.Traceln("call tmq_consumer_new")
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Debugln("tmq_consumer_new get async lock cost:", log.GetLogDuration(isDebug, s))
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
	logger.Debugln("tmq_consumer_new cost:", log.GetLogDuration(isDebug, s))
	return result.Consumer, err
}

func (t *TMQ) wrapperCommit(logger *logrus.Entry, isDebug bool) (int32, bool) {
	logger.Traceln("call tmq_commit_sync")
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return 0, true
	}
	s := log.GetLogNow(isDebug)
	logger.Debugln("tmq_commit_sync get async lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQCommitA(t.thread, t.consumer, nil, t.handler.Handler)
	errCode := <-t.handler.Caller.CommitResult
	t.asyncLocker.Unlock()
	logger.Debugf("tmq_commit_sync %d cost: %s", errCode, log.GetLogDuration(isDebug, s))
	return errCode, false
}

func (t *TMQ) wrapperPoll(logger *logrus.Entry, isDebug bool, blockingTime int64) (unsafe.Pointer, bool) {
	logger.Traceln("call tmq_poll")
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return nil, true
	}
	logger.Debugln("tmq_poll get async lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQPollA(t.thread, t.consumer, blockingTime, t.handler.Handler)
	message := <-t.handler.Caller.PollResult
	t.asyncLocker.Unlock()
	logger.Debugln("tmq_poll cost:", log.GetLogDuration(isDebug, s))
	return message, false
}

func (t *TMQ) wrapperFreeResult(logger *logrus.Entry, isDebug bool) bool {
	logger.Traceln("call free result")
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return true
	}
	if t.tmpMessage.CPointer == nil {
		return false
	}
	logger.Debugln("free result get async lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQFreeResultA(t.thread, t.tmpMessage.CPointer, t.handler.Handler)
	<-t.handler.Caller.FreeResult
	t.asyncLocker.Unlock()
	logger.Debugln("free result cost:", log.GetLogDuration(isDebug, s))
	return false
}

func (t *TMQ) wrapperFetchRawBlock(logger *logrus.Entry, isDebug bool, res unsafe.Pointer) (*tmqhandle.FetchRawBlockResult, bool) {
	logger.Traceln("call fetch_raw_block")
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return nil, true
	}
	logger.Debugln("fetch_raw_block get async lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQFetchRawBlockA(t.thread, res, t.handler.Handler)
	rawBlock := <-t.handler.Caller.FetchRawBlockResult
	logger.Debugln("fetch_raw_block cost:", log.GetLogDuration(isDebug, s))
	t.asyncLocker.Unlock()
	return rawBlock, false
}

func (t *TMQ) wrapperGetRaw(logger *logrus.Entry, isDebug bool, res unsafe.Pointer, rawData unsafe.Pointer) (int32, bool) {
	logger.Traceln("call tmq_get_raw")
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return 0, true
	}
	logger.Debugln("tmq_get_raw get async lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQGetRawA(t.thread, res, rawData, t.handler.Handler)
	errCode := <-t.handler.Caller.GetRawResult
	logger.Debugln("tmq_get_raw cost:", log.GetLogDuration(isDebug, s))
	t.asyncLocker.Unlock()
	return errCode, false
}

func (t *TMQ) wrapperGetJsonMeta(logger *logrus.Entry, isDebug bool, res unsafe.Pointer) (unsafe.Pointer, bool) {
	logger.Tracef("call tmq_get_json_meta")
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return nil, true
	}
	logger.Debugln("tmq_get_json_meta get async lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQGetJsonMetaA(t.thread, res, t.handler.Handler)
	jsonMeta := <-t.handler.Caller.GetJsonMetaResult
	t.asyncLocker.Unlock()
	logger.Debugln("tmq_get_json_meta cost:", log.GetLogDuration(isDebug, s))
	return jsonMeta, false
}

func (t *TMQ) wrapperUnsubscribe(logger *logrus.Entry, isDebug bool) (int32, bool) {
	logger.Traceln("call tmq_unsubscribe")
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return 0, true
	}
	logger.Debugln("unsubscribe get async lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQUnsubscribeA(t.thread, t.consumer, t.handler.Handler)
	errCode := <-t.handler.Caller.UnsubscribeResult
	t.asyncLocker.Unlock()
	logger.Debugln("tmq_unsubscribe cost:", log.GetLogDuration(isDebug, s))
	return errCode, false
}

func (t *TMQ) wrapperGetTopicAssignment(logger *logrus.Entry, isDebug bool, topic string) (*tmqhandle.GetTopicAssignmentResult, bool) {
	logger.Tracef("call get_topic_assignment (%s)", topic)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return nil, true
	}
	logger.Debugln("get_topic_assignment get async lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQGetTopicAssignmentA(t.thread, t.consumer, topic, t.handler.Handler)
	result := <-t.handler.Caller.GetTopicAssignmentResult
	t.asyncLocker.Unlock()
	logger.Debugln("get_topic_assignment cost:", log.GetLogDuration(isDebug, s))
	return result, false
}

func (t *TMQ) wrapperOffsetSeek(logger *logrus.Entry, isDebug bool, topic string, vgroupID int32, offset int64) (int32, bool) {
	logger.Tracef("call offset_seek (%s %d %d)", topic, vgroupID, offset)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return 0, true
	}
	logger.Debugln("offset_seek get async lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQOffsetSeekA(t.thread, t.consumer, topic, vgroupID, offset, t.handler.Handler)
	errCode := <-t.handler.Caller.OffsetSeekResult
	t.asyncLocker.Unlock()
	logger.Debugln("offset_seek cost:", log.GetLogDuration(isDebug, s))
	return errCode, false
}

func (t *TMQ) wrapperCommitted(logger *logrus.Entry, isDebug bool, topic string, vgroupID int32) (int64, bool) {
	logger.Tracef("call tmq_committed (%s %d)", topic, vgroupID)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return 0, true
	}
	logger.Debugln("tmq_committed get async lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQCommitted(t.thread, t.consumer, topic, vgroupID, t.handler.Handler)
	offset := <-t.handler.Caller.CommittedResult
	t.asyncLocker.Unlock()
	logger.Debugln("tmq_committed cost:", log.GetLogDuration(isDebug, s))
	return offset, false
}

func (t *TMQ) wrapperPosition(logger *logrus.Entry, isDebug bool, topic string, vgroupID int32) (int64, bool) {
	logger.Tracef("call position (%s %d)", topic, vgroupID)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return 0, true
	}
	logger.Debugln("position get async lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQPosition(t.thread, t.consumer, topic, vgroupID, t.handler.Handler)
	res := <-t.handler.Caller.PositionResult
	t.asyncLocker.Unlock()
	logger.Debugln("position cost:", log.GetLogDuration(isDebug, s))
	return res, false
}

func (t *TMQ) wrapperCommitOffset(logger *logrus.Entry, isDebug bool, topic string, vgroupID int32, offset int64) (int32, bool) {
	logger.Tracef("call commit_offset (%s %d %d)", topic, vgroupID, offset)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return 0, true
	}
	logger.Debugln("commit_offset get async lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQCommitOffset(t.thread, t.consumer, topic, vgroupID, offset, t.handler.Handler)
	code := <-t.handler.Caller.CommitResult
	t.asyncLocker.Unlock()
	logger.Debugln("commit_offset cost:", log.GetLogDuration(isDebug, s))
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

func tmqFetchRawBlockErrorMsg(ctx context.Context, session *melody.Session, code int, message string, reqID uint64, messageID uint64) {
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
	session.WriteBinary(buf)
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

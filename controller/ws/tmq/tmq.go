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
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/asynctmq"
	"github.com/taosdata/taosadapter/v3/db/asynctmq/tmqhandle"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/db/whitelistwrapper"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
	"github.com/taosdata/taosadapter/v3/tools/bytesutil"
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
		logger := log.GetLogger("ws").WithField("wsType", "tmq")
		_ = s.tmqM.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": logger})
	})
}

type TMQ struct {
	Session             *melody.Session
	consumer            unsafe.Pointer
	tmpMessage          *Message
	asyncLocker         sync.Mutex
	thread              unsafe.Pointer
	handler             *tmqhandle.TMQHandler
	isAutoCommit        bool
	unsubscribed        bool
	closed              bool
	closedLock          sync.RWMutex
	autocommitInterval  time.Duration
	nextTime            time.Time
	exit                chan struct{}
	dropUserNotify      chan struct{}
	whitelistChangeChan chan int64
	session             *melody.Session
	ip                  net.IP
	ipStr               string
	wg                  sync.WaitGroup
	conn                unsafe.Pointer
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
	ipAddr := iptool.GetRealIP(session.Request)
	return &TMQ{
		tmpMessage:          &Message{},
		handler:             tmqhandle.GlobalTMQHandlerPoll.Get(),
		thread:              asynctmq.InitTMQThread(),
		isAutoCommit:        true,
		autocommitInterval:  time.Second * 5,
		exit:                make(chan struct{}),
		whitelistChangeChan: make(chan int64, 1),
		dropUserNotify:      make(chan struct{}, 1),
		session:             session,
		ip:                  ipAddr,
		ipStr:               ipAddr.String(),
	}
}

func (t *TMQ) waitSignal() {
	for {
		if t.isClosed() {
			return
		}
		select {
		case <-t.dropUserNotify:
			t.Lock()
			if t.isClosed() {
				t.Unlock()
				return
			}
			logger := wstool.GetLogger(t.session)
			logger.WithField("clientIP", t.ipStr).Info("user dropped! close connection!")
			t.session.Close()
			t.Unlock()
			t.Close(logger)
			return
		case <-t.whitelistChangeChan:
			t.Lock()
			if t.isClosed() {
				t.Unlock()
				return
			}
			whitelist, err := tool.GetWhitelist(t.conn)
			if err != nil {
				logger := wstool.GetLogger(t.session)
				logger.WithField("clientIP", t.ipStr).WithError(err).Errorln("get whitelist error! close connection!")
				t.session.Close()
				t.Unlock()
				t.Close(logger)
				return
			}
			valid := tool.CheckWhitelist(whitelist, t.ip)
			if !valid {
				logger := wstool.GetLogger(t.session)
				logger.WithField("clientIP", t.ipStr).Errorln("ip not in whitelist! close connection!")
				t.session.Close()
				t.Unlock()
				t.Close(logger)
				return
			}
			t.Unlock()
		case <-t.exit:
			return
		}
	}
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
	logger := wstool.GetLogger(session).WithField("action", TMQSubscribe).WithField(config.ReqIDKey, req.ReqID)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	// lock for consumer and unsubscribed
	// used for subscribe,unsubscribe and close
	t.Lock()
	logger.Debugln("get global lock cost:", log.GetLogDuration(isDebug, s))
	defer t.Unlock()
	if t.isClosed() {
		return
	}
	if t.consumer != nil {
		if t.unsubscribed {
			topicList := wrapper.TMQListNew()
			defer func() {
				wrapper.TMQListDestroy(topicList)
			}()
			for _, topic := range req.Topics {
				errCode := wrapper.TMQListAppend(topicList, topic)
				if errCode != 0 {
					s = log.GetLogNow(isDebug)
					t.asyncLocker.Lock()
					logger.Debugln("tmq_consumer_close get thread lock cost:", log.GetLogDuration(isDebug, s))
					s = log.GetLogNow(isDebug)
					asynctmq.TaosaTMQConsumerCloseA(t.thread, t.consumer, t.handler.Handler)
					<-t.handler.Caller.ConsumerCloseResult
					logger.Debugln("tmq_consumer_close cost:", log.GetLogDuration(isDebug, s))
					t.asyncLocker.Unlock()
					errStr := wrapper.TMQErr2Str(errCode)
					wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
					return
				}
			}
			s = log.GetLogNow(isDebug)
			t.asyncLocker.Lock()
			logger.Debugln("tmq_subscribe get thread lock cost:", log.GetLogDuration(isDebug, s))
			s = log.GetLogNow(isDebug)
			asynctmq.TaosaTMQSubscribeA(t.thread, t.consumer, topicList, t.handler.Handler)
			errCode := <-t.handler.Caller.SubscribeResult
			logger.Debugln("tmq_subscribe cost:", log.GetLogDuration(isDebug, s))
			t.asyncLocker.Unlock()
			if errCode != 0 {
				errStr := wrapper.TMQErr2Str(errCode)
				wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
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
			wsTMQErrorMsg(ctx, session, 0xffff, "tmq should have unsubscribed first", TMQSubscribe, req.ReqID, nil)
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
			wsTMQErrorMsg(ctx, session, 0xffff, err.Error(), TMQSubscribe, req.ReqID, nil)
			return
		}
	}
	if len(req.AutoCommitIntervalMS) != 0 {
		autocommitIntervalMS, err := strconv.ParseInt(req.AutoCommitIntervalMS, 10, 64)
		if err != nil {
			wsTMQErrorMsg(ctx, session, 0xffff, err.Error(), TMQSubscribe, req.ReqID, nil)
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
			wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
	}
	s = log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Debugln("tmq_consumer_new get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQNewConsumerA(t.thread, tmqConfig, t.handler.Handler)
	result := <-t.handler.Caller.NewConsumerResult
	var err error
	if len(result.ErrStr) > 0 {
		err = taoserrors.NewError(-1, result.ErrStr)
	}
	if result.Consumer == nil {
		err = taoserrors.NewError(-1, "new consumer return nil")
	}
	logger.Debugln("tmq_consumer_new cost:", log.GetLogDuration(isDebug, s))
	t.asyncLocker.Unlock()
	if err != nil {
		wsTMQErrorMsg(ctx, session, 0xffff, err.Error(), TMQSubscribe, req.ReqID, nil)
		return
	}
	cPointer := result.Consumer
	conn := whitelistwrapper.TMQGetConnect(cPointer)
	whitelist, err := tool.GetWhitelist(conn)
	if err != nil {
		s = log.GetLogNow(isDebug)
		t.asyncLocker.Lock()
		logger.Debugln("tmq_consumer_close get thread lock cost:", log.GetLogDuration(isDebug, s))
		s = log.GetLogNow(isDebug)
		asynctmq.TaosaTMQConsumerCloseA(t.thread, t.consumer, t.handler.Handler)
		<-t.handler.Caller.ConsumerCloseResult
		logger.Debugln("tmq_consumer_close cost:", log.GetLogDuration(isDebug, s))
		t.asyncLocker.Unlock()
		wstool.WSError(ctx, session, err, TMQSubscribe, req.ReqID)
		return
	}
	valid := tool.CheckWhitelist(whitelist, t.ip)
	if !valid {
		s = log.GetLogNow(isDebug)
		t.asyncLocker.Lock()
		logger.Debugln("tmq_consumer_close get thread lock cost:", log.GetLogDuration(isDebug, s))
		s = log.GetLogNow(isDebug)
		asynctmq.TaosaTMQConsumerCloseA(t.thread, t.consumer, t.handler.Handler)
		<-t.handler.Caller.ConsumerCloseResult
		logger.Debugln("tmq_consumer_close cost:", log.GetLogDuration(isDebug, s))
		t.asyncLocker.Unlock()
		wstool.WSErrorMsg(ctx, session, 0xffff, "whitelist prohibits current IP access", TMQSubscribe, req.ReqID)
		return
	}
	err = tool.RegisterChangeWhitelist(conn, t.whitelistChangeChan)
	if err != nil {
		s = log.GetLogNow(isDebug)
		t.asyncLocker.Lock()
		logger.Debugln("tmq_consumer_close get thread lock cost:", log.GetLogDuration(isDebug, s))
		s = log.GetLogNow(isDebug)
		asynctmq.TaosaTMQConsumerCloseA(t.thread, t.consumer, t.handler.Handler)
		<-t.handler.Caller.ConsumerCloseResult
		logger.Debugln("tmq_consumer_close cost:", log.GetLogDuration(isDebug, s))
		t.asyncLocker.Unlock()
		wstool.WSError(ctx, session, err, TMQSubscribe, req.ReqID)
		return
	}
	err = tool.RegisterDropUser(conn, t.dropUserNotify)
	if err != nil {
		s = log.GetLogNow(isDebug)
		t.asyncLocker.Lock()
		logger.Debugln("tmq_consumer_close get thread lock cost:", log.GetLogDuration(isDebug, s))
		s = log.GetLogNow(isDebug)
		asynctmq.TaosaTMQConsumerCloseA(t.thread, t.consumer, t.handler.Handler)
		<-t.handler.Caller.ConsumerCloseResult
		logger.Debugln("tmq_consumer_close cost:", log.GetLogDuration(isDebug, s))
		t.asyncLocker.Unlock()
		wstool.WSError(ctx, session, err, TMQSubscribe, req.ReqID)
		return
	}
	t.conn = conn
	go t.waitSignal()
	topicList := wrapper.TMQListNew()
	defer func() {
		wrapper.TMQListDestroy(topicList)
	}()
	for _, topic := range req.Topics {
		errCode := wrapper.TMQListAppend(topicList, topic)
		if errCode != 0 {
			errStr := wrapper.TMQErr2Str(errCode)
			s = log.GetLogNow(isDebug)
			t.asyncLocker.Lock()
			logger.Debugln("tmq_consumer_close get thread lock cost:", log.GetLogDuration(isDebug, s))
			s = log.GetLogNow(isDebug)
			asynctmq.TaosaTMQConsumerCloseA(t.thread, t.consumer, t.handler.Handler)
			<-t.handler.Caller.ConsumerCloseResult
			logger.Debugln("tmq_consumer_close cost:", log.GetLogDuration(isDebug, s))
			t.asyncLocker.Unlock()
			wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
	}
	s = log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Debugln("tmq_subscribe get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQSubscribeA(t.thread, cPointer, topicList, t.handler.Handler)
	errCode = <-t.handler.Caller.SubscribeResult
	logger.Debugln("tmq_subscribe cost:", log.GetLogDuration(isDebug, s))
	t.asyncLocker.Unlock()
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		s = log.GetLogNow(isDebug)
		t.asyncLocker.Lock()
		logger.Debugln("tmq_consumer_close get thread lock cost:", log.GetLogDuration(isDebug, s))
		s = log.GetLogNow(isDebug)
		asynctmq.TaosaTMQConsumerCloseA(t.thread, cPointer, t.handler.Handler)
		<-t.handler.Caller.ConsumerCloseResult
		logger.Debugln("tmq_consumer_close cost:", log.GetLogDuration(isDebug, s))
		t.asyncLocker.Unlock()
		wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
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
	if t.consumer == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "tmq not init", TMQCommit, req.ReqID, nil)
		return
	}
	// commit all
	logger := wstool.GetLogger(session).WithField("action", TMQCommit).WithField(config.ReqIDKey, req.ReqID)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	// check if closed
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return
	}
	logger.Debugln("get async lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQCommitA(t.thread, t.consumer, nil, t.handler.Handler)
	errCode := <-t.handler.Caller.CommitResult
	t.asyncLocker.Unlock()
	logger.Debugln("tmq_commit_sync cost:", log.GetLogDuration(isDebug, s))
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQCommit, req.ReqID, nil)
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
	if t.consumer == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "tmq not init", TMQPoll, req.ReqID, nil)
		return
	}
	now := time.Now()
	isDebug := log.IsDebug()
	logger := wstool.GetLogger(session).WithField("action", TMQPoll).WithField(config.ReqIDKey, req.ReqID)
	if t.isAutoCommit && now.After(t.nextTime) {
		t.asyncLocker.Lock()
		if t.isClosed() {
			t.asyncLocker.Unlock()
			return
		}
		s := log.GetLogNow(isDebug)
		logger.Debugln("get commit async lock cost:", log.GetLogDuration(isDebug, s))
		s = log.GetLogNow(isDebug)
		asynctmq.TaosaTMQCommitA(t.thread, t.consumer, nil, t.handler.Handler)
		errCode := <-t.handler.Caller.CommitResult
		logger.Debugln("tmq_commit_sync cost:", log.GetLogDuration(isDebug, s))
		t.asyncLocker.Unlock()
		if errCode != 0 {
			errStr := wrapper.TMQErr2Str(errCode)
			logger.Errorln("tmq autocommit error:", taoserrors.NewError(int(errCode), errStr))
		}
		t.nextTime = now.Add(t.autocommitInterval)
	}
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return
	}
	logger.Debugln("get async lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQPollA(t.thread, t.consumer, req.BlockingTime, t.handler.Handler)
	message := <-t.handler.Caller.PollResult
	logger.Debugln("tmq_poll cost:", log.GetLogDuration(isDebug, s))
	t.asyncLocker.Unlock()
	resp := &TMQPollResp{
		Action: TMQPoll,
		ReqID:  req.ReqID,
	}
	if message != nil {
		s = log.GetLogNow(isDebug)
		logger.Debugln("free message cost:", log.GetLogDuration(isDebug, s))
		messageType := wrapper.TMQGetResType(message)
		if messageTypeIsValid(messageType) {
			s = log.GetLogNow(isDebug)
			t.tmpMessage.Lock()
			defer t.tmpMessage.Unlock()
			if t.tmpMessage.CPointer != nil {
				t.asyncLocker.Lock()
				asynctmq.TaosaTMQFreeResultA(t.thread, t.tmpMessage.CPointer, t.handler.Handler)
				<-t.handler.Caller.FreeResult
				t.asyncLocker.Unlock()
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
			logger.Debugln("get message cost:", log.GetLogDuration(isDebug, s))
		} else {
			wsTMQErrorMsg(ctx, session, 0xffff, "unavailable tmq type:"+strconv.Itoa(int(messageType)), TMQPoll, req.ReqID, nil)
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
	if t.consumer == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "tmq not init", TMQFetch, req.ReqID, &req.MessageID)
		return
	}
	logger := wstool.GetLogger(session).WithField("action", TMQFetch).WithField(config.ReqIDKey, req.ReqID)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	logger.Debugln("get list lock cost:", log.GetLogDuration(isDebug, s))
	t.tmpMessage.Lock()
	defer t.tmpMessage.Unlock()
	if t.tmpMessage.CPointer == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "message has been freed", TMQFetch, req.ReqID, &req.MessageID)
		return
	}
	message := t.tmpMessage
	if message.Index != req.MessageID {
		wsTMQErrorMsg(ctx, session, 0xffff, "message ID are not equal", TMQFetch, req.ReqID, &req.MessageID)
		return
	}

	if !canGetData(message.Type) {
		wsTMQErrorMsg(ctx, session, 0xffff, "message type is not data", TMQFetch, req.ReqID, &req.MessageID)
		return
	}
	s = log.GetLogNow(isDebug)
	logger.Debugln("get message lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return
	}
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQFetchRawBlockA(t.thread, message.CPointer, t.handler.Handler)
	rawBlock := <-t.handler.Caller.FetchRawBlockResult
	errCode := rawBlock.Code
	blockSize := rawBlock.BlockSize
	block := rawBlock.Block
	logger.Debugln("fetch_raw_block cost:", log.GetLogDuration(isDebug, s))
	t.asyncLocker.Unlock()
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(int32(errCode))
		wsTMQErrorMsg(ctx, session, errCode, errStr, TMQFetch, req.ReqID, &req.MessageID)
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
	if t.consumer == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "tmq not init", TMQFetchBlock, req.ReqID, &req.MessageID)
		return
	}
	logger := wstool.GetLogger(session).WithField("action", TMQFetchBlock).WithField(config.ReqIDKey, req.ReqID)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	logger.Debugln("get list lock cost:", log.GetLogDuration(isDebug, s))
	t.tmpMessage.Lock()
	defer t.tmpMessage.Unlock()
	if t.tmpMessage.CPointer == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "message has been freed", TMQFetchBlock, req.ReqID, &req.MessageID)
		return
	}
	message := t.tmpMessage
	if message.Index != req.MessageID {
		wsTMQErrorMsg(ctx, session, 0xffff, "message is nil", TMQFetchBlock, req.ReqID, &req.MessageID)
		return
	}
	if !canGetData(message.Type) {
		wsTMQErrorMsg(ctx, session, 0xffff, "message type is not data", TMQFetchBlock, req.ReqID, &req.MessageID)
		return
	}
	if message.buffer == nil || len(message.buffer) == 0 {
		wsTMQErrorMsg(ctx, session, 0xffff, "no fetch data", TMQFetchBlock, req.ReqID, &req.MessageID)
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
	if t.consumer == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "tmq not init", TMQFetchRaw, req.ReqID, &req.MessageID)
		return
	}
	logger := wstool.GetLogger(session).WithField("action", TMQFetchRaw).WithField(config.ReqIDKey, req.ReqID)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.tmpMessage.Lock()
	defer t.tmpMessage.Unlock()
	if t.tmpMessage.CPointer == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "message has been freed", TMQFetchRaw, req.ReqID, &req.MessageID)
		return
	}
	message := t.tmpMessage
	if message.Index != req.MessageID {
		wsTMQErrorMsg(ctx, session, 0xffff, "message ID is not equal", TMQFetchRaw, req.ReqID, &req.MessageID)
		return
	}
	s = log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return
	}
	logger.Debugln("tmq_get_raw get lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	rawData := asynctmq.TaosaInitTMQRaw()
	defer asynctmq.TaosaFreeTMQRaw(rawData)
	asynctmq.TaosaTMQGetRawA(t.thread, message.CPointer, rawData, t.handler.Handler)
	errCode := <-t.handler.Caller.GetRawResult
	logger.Debugln("tmq_get_raw cost:", log.GetLogDuration(isDebug, s))
	t.asyncLocker.Unlock()
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQFetchRaw, req.ReqID, &req.MessageID)
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
	if t.consumer == nil {
		tmqFetchRawBlockErrorMsg(ctx, session, 0xffff, "tmq not init", req.ReqID, req.MessageID)
		return
	}
	logger := wstool.GetLogger(session).WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.tmpMessage.Lock()
	defer t.tmpMessage.Unlock()
	if t.tmpMessage.CPointer == nil {
		tmqFetchRawBlockErrorMsg(ctx, session, 0xffff, "message has been freed", req.ReqID, req.MessageID)
		return
	}
	message := t.tmpMessage
	if message.Index != req.MessageID {
		tmqFetchRawBlockErrorMsg(ctx, session, 0xffff, "message ID is not equal", req.ReqID, req.MessageID)
		return
	}
	s = log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return
	}
	logger.Debugln("tmq_get_raw get lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	rawData := asynctmq.TaosaInitTMQRaw()
	defer asynctmq.TaosaFreeTMQRaw(rawData)
	asynctmq.TaosaTMQGetRawA(t.thread, message.CPointer, rawData, t.handler.Handler)
	errCode := <-t.handler.Caller.GetRawResult
	logger.Debugln("tmq_get_raw cost:", log.GetLogDuration(isDebug, s))
	t.asyncLocker.Unlock()
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
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
	if t.consumer == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "tmq not init", TMQFetchJsonMeta, req.ReqID, &req.MessageID)
		return
	}
	logger := wstool.GetLogger(session).WithField("action", TMQFetchJsonMeta).WithField(config.ReqIDKey, req.ReqID)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.tmpMessage.Lock()
	defer t.tmpMessage.Unlock()
	if t.tmpMessage.CPointer == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "message has been freed", TMQFetchJsonMeta, req.ReqID, &req.MessageID)
		return
	}
	message := t.tmpMessage
	if message.Index != req.MessageID {
		wsTMQErrorMsg(ctx, session, 0xffff, "message ID is not equal", TMQFetchJsonMeta, req.ReqID, &req.MessageID)
		return
	}

	logger.Debugln("get message lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return
	}
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQGetJsonMetaA(t.thread, message.CPointer, t.handler.Handler)
	jsonMeta := <-t.handler.Caller.GetJsonMetaResult
	logger.Debugln("tmq_get_json_meta cost:", log.GetLogDuration(isDebug, s))
	t.asyncLocker.Unlock()
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
	logger := wstool.GetLogger(session).WithField("action", TMQUnsubscribe).WithField(config.ReqIDKey, req.ReqID)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.Lock()
	logger.Debugln("get global lock cost:", log.GetLogDuration(isDebug, s))
	defer t.Unlock()
	if t.isClosed() {
		return
	}
	if t.consumer == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "tmq not init", TMQUnsubscribe, req.ReqID, nil)
		return
	}
	t.asyncLocker.Lock()
	asynctmq.TaosaTMQUnsubscribeA(t.thread, t.consumer, t.handler.Handler)
	errCode := <-t.handler.Caller.UnsubscribeResult
	t.asyncLocker.Unlock()
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		logger.WithError(taoserrors.NewError(int(errCode), errStr)).Error("tmq unsubscribe consumer")
		wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQUnsubscribe, req.ReqID, nil)
		return
	}
	t.freeMessage(false)
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
	if t.consumer == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "tmq not init", TMQGetTopicAssignment, req.ReqID, nil)
		return
	}
	logger := wstool.GetLogger(session).WithField("action", TMQGetTopicAssignment).WithField(config.ReqIDKey, req.ReqID)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return
	}
	logger.Debugln("get_topic_assignment get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQGetTopicAssignmentA(t.thread, t.consumer, req.Topic, t.handler.Handler)
	result := <-t.handler.Caller.GetTopicAssignmentResult
	t.asyncLocker.Unlock()
	logger.Debugln("get_topic_assignment cost:", log.GetLogDuration(isDebug, s))
	if result.Code != 0 {
		errStr := wrapper.TMQErr2Str(result.Code)
		logger.WithError(taoserrors.NewError(int(result.Code), errStr)).Error("tmq assignment")
		wsTMQErrorMsg(ctx, session, int(result.Code), errStr, TMQGetTopicAssignment, req.ReqID, nil)
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
	if t.consumer == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "tmq not init", TMQSeek, req.ReqID, nil)
		return
	}
	logger := wstool.GetLogger(session).WithField("action", TMQSeek).WithField(config.ReqIDKey, req.ReqID)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return
	}
	logger.Debugln("offset_seek get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQOffsetSeekA(t.thread, t.consumer, req.Topic, req.VgroupID, req.Offset, t.handler.Handler)
	errCode := <-t.handler.Caller.OffsetSeekResult
	t.asyncLocker.Unlock()
	logger.Debugln("offset_seek cost:", log.GetLogDuration(isDebug, s))
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		logger.WithError(taoserrors.NewError(int(errCode), errStr)).Error("tmq offset seek")
		wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSeek, req.ReqID, nil)
		return
	}
	wstool.WSWriteJson(session, TMQOffsetSeekResp{
		Action: TMQSeek,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
	})
}

func (t *TMQ) Close(logger logrus.FieldLogger) {
	t.Lock()
	defer t.Unlock()
	if t.isClosed() {
		return
	}
	t.closedLock.Lock()
	t.closed = true
	t.closedLock.Unlock()
	s := time.Now()
	logger.Info("tmq close")
	defer func() {
		logger.Info("tmq close end, cost:", time.Since(s).String())
	}()
	close(t.exit)
	close(t.whitelistChangeChan)
	close(t.dropUserNotify)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	done := make(chan struct{})
	go func() {
		t.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
	case <-done:
	}
	defer func() {
		t.asyncLocker.Lock()
		asynctmq.DestroyTMQThread(t.thread)
		t.asyncLocker.Unlock()
		tmqhandle.GlobalTMQHandlerPoll.Put(t.handler)
	}()

	defer func() {
		if t.consumer != nil {
			if !t.unsubscribed {
				t.asyncLocker.Lock()
				asynctmq.TaosaTMQUnsubscribeA(t.thread, t.consumer, t.handler.Handler)
				errCode := <-t.handler.Caller.UnsubscribeResult
				t.asyncLocker.Unlock()
				if errCode != 0 {
					errMsg := wrapper.TMQErr2Str(errCode)
					logger.WithError(taoserrors.NewError(int(errCode), errMsg)).Error("tmq unsubscribe consumer")
				}
			}
			t.asyncLocker.Lock()
			asynctmq.TaosaTMQConsumerCloseA(t.thread, t.consumer, t.handler.Handler)
			errCode := <-t.handler.Caller.ConsumerCloseResult
			t.asyncLocker.Unlock()
			if errCode != 0 {
				errMsg := wrapper.TMQErr2Str(errCode)
				logger.WithError(taoserrors.NewError(int(errCode), errMsg)).Error("tmq close consumer")
			}
		}
	}()
	t.freeMessage(true)
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
	if t.consumer == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "tmq not init", TMQCommitted, req.ReqID, nil)
		return
	}
	logger := wstool.GetLogger(session).WithField("action", TMQCommitted).WithField(config.ReqIDKey, req.ReqID)
	s := log.GetLogNow(log.IsDebug())
	logger.Debugln("tmq committed get thread lock cost:", log.GetLogDuration(log.IsDebug(), s))

	offsets := make([]int64, 0, len(req.TopicVgroupIDs))
	for _, tv := range req.TopicVgroupIDs {
		t.asyncLocker.Lock()
		if t.isClosed() {
			t.asyncLocker.Unlock()
			return
		}
		asynctmq.TaosaTMQCommitted(t.thread, t.consumer, tv.Topic, int32(tv.VgroupID), t.handler.Handler)
		res := <-t.handler.Caller.CommittedResult
		t.asyncLocker.Unlock()
		if res < 0 && res != OffsetInvalid {
			err := taoserrors.NewError(int(res), wrapper.TMQErr2Str(int32(res)))
			logger.WithError(err).Error("tmq get committed")
			var taosErr *taoserrors.TaosError
			errors.As(err, &taosErr)
			wsTMQErrorMsg(ctx, session, int(taosErr.Code), taosErr.ErrStr, TMQCommitted, req.ReqID, nil)
			return
		}
		offsets = append(offsets, res)
	}

	logger.Debugln("tmq get committed cost:", log.GetLogDuration(log.IsDebug(), s))
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
	if t.consumer == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "tmq not init", TMQPosition, req.ReqID, nil)
		return
	}
	logger := wstool.GetLogger(session).WithField("action", TMQPosition).WithField(config.ReqIDKey, req.ReqID)
	s := log.GetLogNow(log.IsDebug())

	logger.Debugln("tmq get position get thread lock cost:", log.GetLogDuration(log.IsDebug(), s))

	positions := make([]int64, 0, len(req.TopicVgroupIDs))
	for _, tv := range req.TopicVgroupIDs {
		t.asyncLocker.Lock()
		if t.isClosed() {
			t.asyncLocker.Unlock()
			return
		}
		asynctmq.TaosaTMQPosition(t.thread, t.consumer, tv.Topic, int32(tv.VgroupID), t.handler.Handler)
		res := <-t.handler.Caller.PositionResult
		t.asyncLocker.Unlock()

		if res < 0 && res != OffsetInvalid {
			err := taoserrors.NewError(int(res), wrapper.TMQErr2Str(int32(res)))
			logger.WithError(err).Error("tmq get position")
			var taosErr *taoserrors.TaosError
			errors.As(err, &taosErr)
			wsTMQErrorMsg(ctx, session, int(taosErr.Code), taosErr.ErrStr, TMQPosition, req.ReqID, nil)
			return
		}
		positions = append(positions, res)
	}
	logger.Debugln("tmq get position cost:", log.GetLogDuration(log.IsDebug(), s))

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
	if t.consumer == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "tmq not init", TMQListTopics, req.ReqID, nil)
		return
	}
	logger := wstool.GetLogger(session).WithField("action", TMQListTopics).WithField(config.ReqIDKey, req.ReqID)
	s := log.GetLogNow(log.IsDebug())
	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return
	}
	code, topicsPointer := wrapper.TMQSubscription(t.consumer)
	t.asyncLocker.Unlock()
	defer wrapper.TMQListDestroy(topicsPointer)
	if code != 0 {
		errStr := wrapper.TMQErr2Str(code)
		logger.WithError(taoserrors.NewError(int(code), errStr)).Error("tmq list topic")
		wsTMQErrorMsg(ctx, session, int(code), errStr, TMQListTopics, req.ReqID, nil)
		return
	}
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
	if t.consumer == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "tmq not init", TMQCommitOffset, req.ReqID, nil)
		return
	}
	logger := wstool.GetLogger(session).WithField("action", TMQCommitOffset).WithField(config.ReqIDKey, req.ReqID)
	logger.Debugln("tmq commit offset get thread lock cost:",
		log.GetLogDuration(log.IsDebug(), log.GetLogNow(log.IsDebug())))

	t.asyncLocker.Lock()
	if t.isClosed() {
		t.asyncLocker.Unlock()
		return
	}
	asynctmq.TaosaTMQCommitOffset(t.thread, t.consumer, req.Topic, req.VgroupID, req.Offset, t.handler.Handler)
	code := <-t.handler.Caller.CommitResult
	t.asyncLocker.Unlock()
	if code != 0 {
		errMsg := wrapper.TMQErr2Str(code)
		logger.WithError(taoserrors.NewError(int(code), errMsg)).Error("tmq commit offset")
		wsTMQErrorMsg(ctx, session, int(code), errMsg, TMQCommitOffset, req.ReqID, nil)
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

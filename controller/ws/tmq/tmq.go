package tmq

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
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
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/jsontype"
)

type TMQController struct {
	tmqM *melody.Melody
}

func NewTMQController() *TMQController {
	tmqM := melody.New()
	tmqM.Config.MaxMessageSize = 0

	tmqM.HandleConnect(func(session *melody.Session) {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws connect")
		session.Set(TaosTMQKey, NewTaosTMQ())
	})

	tmqM.HandleMessage(func(session *melody.Session, data []byte) {
		if tmqM.IsClosed() {
			return
		}
		go func() {
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
				t := session.MustGet(TaosTMQKey)
				t.(*TMQ).subscribe(ctx, session, &req)
			case TMQPoll:
				var req TMQPollReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal pool args")
					return
				}
				t := session.MustGet(TaosTMQKey)
				t.(*TMQ).poll(ctx, session, &req)
			case TMQFetch:
				var req TMQFetchReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal fetch args")
					return
				}
				t := session.MustGet(TaosTMQKey)
				t.(*TMQ).fetch(ctx, session, &req)
			case TMQFetchBlock:
				var req TMQFetchBlockReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal fetch block args")
					return
				}
				t := session.MustGet(TaosTMQKey)
				t.(*TMQ).fetchBlock(ctx, session, &req)
			case TMQCommit:
				var req TMQCommitReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal commit args")
					return
				}
				t := session.MustGet(TaosTMQKey)
				t.(*TMQ).commit(ctx, session, &req)
			case TMQFetchJsonMeta:
				var req TMQFetchJsonMetaReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal fetch json meta args")
					return
				}
				t := session.MustGet(TaosTMQKey)
				t.(*TMQ).fetchJsonMeta(ctx, session, &req)
			case TMQFetchRaw:
				var req TMQFetchRawMetaReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal fetch raw meta args")
					return
				}
				t := session.MustGet(TaosTMQKey)
				t.(*TMQ).fetchRawMeta(ctx, session, &req)
			case TMQUnsubscribe:
				var req TMQUnsubscribeReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal unsubscribe args")
					return
				}
				t := session.MustGet(TaosTMQKey)
				t.(*TMQ).unsubscribe(ctx, session, &req)
			case TMQGetTopicAssignment:
				var req TMQGetTopicAssignmentReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal unsubscribe args")
					return
				}
				t := session.MustGet(TaosTMQKey)
				t.(*TMQ).assignment(ctx, session, &req)
			case TMQSeek:
				var req TMQOffsetSeekReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal unsubscribe args")
					return
				}
				t := session.MustGet(TaosTMQKey)
				t.(*TMQ).offsetSeek(ctx, session, &req)
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
		logger := session.MustGet("logger").(*logrus.Entry)
		_, is := err.(*websocket.CloseError)
		if is {
			logger.WithError(err).Debugln("ws close in error")
		} else {
			logger.WithError(err).Errorln("ws error")
		}
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
	Session      *melody.Session
	consumer     unsafe.Pointer
	messages     *TopicVGroup
	asyncLocker  sync.Mutex
	thread       unsafe.Pointer
	handler      *tmqhandle.TMQHandler
	isAutoCommit bool
	unsubscribed bool
	closed       bool
	nextTime     time.Time
	ticker       *time.Timer
	sync.Mutex
}

func NewTaosTMQ() *TMQ {
	return &TMQ{
		messages:     NewTopicVGroup(WithAutoClean(), WithTimeout(5*int64(config.Conf.TMQ.ReleaseIntervalMultiplierForAutocommit))),
		handler:      tmqhandle.GlobalTMQHandlerPoll.Get(),
		thread:       asynctmq.InitTMQThread(),
		isAutoCommit: true,
	}
}

func (t *TMQ) cleanupMessage(m *Message) {
	t.messages.CleanByOffset(m.Topic, m.VGroupID, m.Offset)
}

func (t *TMQ) cleanupMessageByOffset(topic string, vgID int32, offset int64) {
	t.messages.CleanByOffset(topic, vgID, offset)
}

func (t *TMQ) addMessage(message *Message) {
	t.messages.AddMessage(message)
}

func (t *TMQ) getMessageByOffset(topic string, vgID int32, offset int64) (*Message, error) {
	return t.messages.GetByOffset(topic, vgID, offset)
}

func (t *TMQ) getMessageByMessageID(messageID uint64) (*Message, error) {
	return t.messages.GetByMessageID(messageID)
}

type TMQSubscribeReq struct {
	ReqID                uint64   `json:"req_id"`
	User                 string   `json:"user"`
	Password             string   `json:"password"`
	DB                   string   `json:"db"`
	GroupID              string   `json:"group_id"`
	ClientID             string   `json:"client_id"`
	OffsetRest           string   `json:"offset_rest"`
	Topics               []string `json:"topics"`
	AutoCommit           string   `json:"auto_commit"`
	AutoCommitIntervalMS string   `json:"auto_commit_interval_ms"`
	SnapshotEnable       string   `json:"snapshot_enable"`
	WithTableName        string   `json:"with_table_name"`
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
	t.Lock()
	logger.Debugln("get global lock cost:", log.GetLogDuration(isDebug, s))
	defer t.Unlock()
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
	if len(req.GroupID) != 0 {
		errCode := wrapper.TMQConfSet(tmqConfig, "group.id", req.GroupID)
		if errCode != httperror.SUCCESS {
			errStr := wrapper.TMQErr2Str(errCode)
			wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
	}
	if len(req.ClientID) != 0 {
		errCode := wrapper.TMQConfSet(tmqConfig, "client.id", req.ClientID)
		if errCode != httperror.SUCCESS {
			errStr := wrapper.TMQErr2Str(errCode)
			wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
	}
	if len(req.DB) != 0 {
		errCode := wrapper.TMQConfSet(tmqConfig, "td.connect.db", req.DB)
		if errCode != httperror.SUCCESS {
			errStr := wrapper.TMQErr2Str(errCode)
			wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
	}

	if len(req.OffsetRest) != 0 {
		errCode := wrapper.TMQConfSet(tmqConfig, "auto.offset.reset", req.OffsetRest)
		if errCode != httperror.SUCCESS {
			errStr := wrapper.TMQErr2Str(errCode)
			wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
	}

	errCode := wrapper.TMQConfSet(tmqConfig, "td.connect.user", req.User)
	if errCode != httperror.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
		return
	}
	errCode = wrapper.TMQConfSet(tmqConfig, "td.connect.pass", req.Password)
	if errCode != httperror.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
		return
	}
	if len(req.WithTableName) != 0 {
		errCode = wrapper.TMQConfSet(tmqConfig, "msg.with.table.name", req.WithTableName)
		if errCode != httperror.SUCCESS {
			errStr := wrapper.TMQErr2Str(errCode)
			wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
	}
	if len(req.AutoCommit) != 0 {
		errCode = wrapper.TMQConfSet(tmqConfig, "enable.auto.commit", req.AutoCommit)
		if errCode != httperror.SUCCESS {
			errStr := wrapper.TMQErr2Str(errCode)
			wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
		var err error
		t.isAutoCommit, err = strconv.ParseBool(req.AutoCommit)
		if err != nil {
			wsTMQErrorMsg(ctx, session, 0xffff, err.Error(), TMQSubscribe, req.ReqID, nil)
			return
		}
		if !t.isAutoCommit {
			t.messages.StopAutoClean()
		}
	}
	if len(req.AutoCommitIntervalMS) != 0 {
		errCode = wrapper.TMQConfSet(tmqConfig, "auto.commit.interval.ms", req.AutoCommitIntervalMS)
		if errCode != httperror.SUCCESS {
			errStr := wrapper.TMQErr2Str(errCode)
			wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
		autocommitIntervalMS, err := strconv.ParseInt(req.AutoCommitIntervalMS, 10, 64)
		if err != nil {
			wsTMQErrorMsg(ctx, session, 0xffff, err.Error(), TMQSubscribe, req.ReqID, nil)
			return
		}
		t.messages.SetMessageTimeout((autocommitIntervalMS / 1000) * int64(config.Conf.TMQ.ReleaseIntervalMultiplierForAutocommit))
	}
	if len(req.SnapshotEnable) != 0 {
		errCode = wrapper.TMQConfSet(tmqConfig, "experimental.snapshot.enable", req.SnapshotEnable)
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
	asynctmq.TaosaTMQSubscribeA(t.thread, cPointer, topicList, t.handler.Handler)
	errCode = <-t.handler.Caller.SubscribeResult
	logger.Debugln("tmq_subscribe cost:", log.GetLogDuration(isDebug, s))
	t.asyncLocker.Unlock()
	if errCode != 0 {
		s = log.GetLogNow(isDebug)
		t.asyncLocker.Lock()
		logger.Debugln("tmq_consumer_close get thread lock cost:", log.GetLogDuration(isDebug, s))
		s = log.GetLogNow(isDebug)
		asynctmq.TaosaTMQConsumerCloseA(t.thread, cPointer, t.handler.Handler)
		<-t.handler.Caller.ConsumerCloseResult
		logger.Debugln("tmq_consumer_close cost:", log.GetLogDuration(isDebug, s))
		t.asyncLocker.Unlock()
		errStr := wrapper.TMQErr2Str(errCode)
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
	MessageID uint64 `json:"message_id"`
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
	logger := wstool.GetLogger(session).WithField("action", TMQCommit).WithField(config.ReqIDKey, req.ReqID)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	logger.Debugln("get list lock cost:", log.GetLogDuration(isDebug, s))
	resp := &TMQCommitResp{
		Action:    TMQCommit,
		ReqID:     req.ReqID,
		MessageID: req.MessageID,
	}
	message, err := t.getMessageByMessageID(req.MessageID)
	if err != nil {
		resp.Timing = wstool.GetDuration(ctx)
		wsTMQErrorMsg(ctx, session, int(0xfff), err.Error(), TMQCommit, req.ReqID, nil)
		return
	}
	s = log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Debugln("get async lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQCommitA(t.thread, t.consumer, message.CPointer, t.handler.Handler)
	errCode := <-t.handler.Caller.CommitResult
	t.asyncLocker.Unlock()
	logger.Debugln("tmq_commit_sync cost:", log.GetLogDuration(isDebug, s))
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQCommit, req.ReqID, nil)
		return
	}
	t.cleanupMessage(message)
	resp.Timing = wstool.GetDuration(ctx)
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
	t.asyncLocker.Lock()
	asynctmq.TaosaTMQPollA(t.thread, t.consumer, req.BlockingTime, t.handler.Handler)
	message := <-t.handler.Caller.PollResult
	t.asyncLocker.Unlock()
	resp := &TMQPollResp{
		Action: TMQPoll,
		ReqID:  req.ReqID,
	}
	if message != nil {
		messageType := wrapper.TMQGetResType(message)
		if messageTypeIsValid(messageType) {
			m := t.messages.CreateMessage(
				wrapper.TMQGetTopicName(message),
				wrapper.TMQGetVgroupID(message),
				wrapper.TMQGetVgroupOffset(message),
				messageType, message)
			t.addMessage(m)
			resp.HaveMessage = true
			resp.Topic = m.Topic
			resp.Database = wrapper.TMQGetDBName(message)
			resp.VgroupID = m.VGroupID
			resp.MessageID = m.MessageID()
			resp.MessageType = messageType
			resp.Offset = m.Offset
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

	message, err := t.getMessageByMessageID(req.MessageID)
	if err != nil && errors.Is(err, NotFountError) {
		wsTMQErrorMsg(ctx, session, 0xffff, "message is nil", TMQFetch, req.ReqID, &req.MessageID)
		return
	}
	if err != nil {
		wsTMQErrorMsg(ctx, session, 0xffff, err.Error(), TMQFetch, req.ReqID, &req.MessageID)
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
	if message.buffer == nil {
		message.buffer = new(bytes.Buffer)
	} else {
		message.buffer.Reset()
	}
	blockLength := int(parser.RawBlockGetLength(block))
	message.buffer.Grow(blockLength + 24)
	wstool.WriteUint64(message.buffer, 0)
	wstool.WriteUint64(message.buffer, req.ReqID)
	wstool.WriteUint64(message.buffer, req.MessageID)
	for offset := 0; offset < blockLength; offset++ {
		message.buffer.WriteByte(*((*byte)(unsafe.Pointer(uintptr(block) + uintptr(offset)))))
	}
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

	message, err := t.getMessageByMessageID(req.MessageID)
	if err != nil && errors.Is(err, NotFountError) {
		wsTMQErrorMsg(ctx, session, 0xffff, "message is nil", TMQFetch, req.ReqID, &req.MessageID)
		return
	}
	if err != nil {
		wsTMQErrorMsg(ctx, session, 0xffff, err.Error(), TMQFetch, req.ReqID, &req.MessageID)
		return
	}

	if !canGetData(message.Type) {
		wsTMQErrorMsg(ctx, session, 0xffff, "message type is not data", TMQFetchBlock, req.ReqID, &req.MessageID)
		return
	}
	if message.buffer == nil || message.buffer.Len() == 0 {
		wsTMQErrorMsg(ctx, session, 0xffff, "no fetch data", TMQFetchBlock, req.ReqID, &req.MessageID)
		return
	}
	s = log.GetLogNow(isDebug)
	b := message.buffer.Bytes()
	binary.LittleEndian.PutUint64(b, uint64(wstool.GetDuration(ctx)))
	logger.Debugln("handle data cost:", log.GetLogDuration(isDebug, s))
	session.WriteBinary(b)
}

type TMQFetchRawMetaReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}

func (t *TMQ) fetchRawMeta(ctx context.Context, session *melody.Session, req *TMQFetchRawMetaReq) {
	if t.consumer == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "tmq not init", TMQFetchRaw, req.ReqID, &req.MessageID)
		return
	}
	logger := wstool.GetLogger(session).WithField("action", TMQFetchRaw).WithField(config.ReqIDKey, req.ReqID)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)

	message, err := t.getMessageByMessageID(req.MessageID)
	if err != nil && errors.Is(err, NotFountError) {
		wsTMQErrorMsg(ctx, session, 0xffff, "message is nil", TMQFetchRaw, req.ReqID, &req.MessageID)
		return
	}
	if err != nil {
		wsTMQErrorMsg(ctx, session, 0xffff, err.Error(), TMQFetchRaw, req.ReqID, &req.MessageID)
		return
	}

	s = log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Debugln("tmq_get_raw get lock cost:", log.GetLogDuration(isDebug, s))
	s = time.Now()
	rawMeta := asynctmq.TaosaInitTMQRaw()
	defer asynctmq.TaosaFreeTMQRaw(rawMeta)
	asynctmq.TaosaTMQGetRawA(t.thread, message.CPointer, rawMeta, t.handler.Handler)
	errCode := <-t.handler.Caller.GetRawResult
	logger.Debugln("tmq_get_raw cost:", log.GetLogDuration(isDebug, s))
	t.asyncLocker.Unlock()
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQFetchRaw, req.ReqID, &req.MessageID)
		return
	}
	s = time.Now()
	length, metaType, data := wrapper.ParseRawMeta(rawMeta)
	if message.buffer == nil {
		message.buffer = new(bytes.Buffer)
	} else {
		message.buffer.Reset()
	}
	message.buffer.Grow(int(length) + 38)
	wstool.WriteUint64(message.buffer, uint64(wstool.GetDuration(ctx)))
	wstool.WriteUint64(message.buffer, req.ReqID)
	wstool.WriteUint64(message.buffer, req.MessageID)
	wstool.WriteUint64(message.buffer, TMQRawMessage)
	wstool.WriteUint32(message.buffer, length)
	wstool.WriteUint16(message.buffer, metaType)
	for offset := 0; offset < int(length); offset++ {
		message.buffer.WriteByte(*((*byte)(unsafe.Pointer(uintptr(data) + uintptr(offset)))))
	}
	s1 := time.Now()
	wrapper.TMQFreeRaw(rawMeta)
	logger.Debugln("tmq_free_raw cost:", log.GetLogDuration(isDebug, s1))
	logger.Debugln("handle binary data cost:", log.GetLogDuration(isDebug, s))
	session.WriteBinary(message.buffer.Bytes())
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

	message, err := t.getMessageByMessageID(req.MessageID)
	if err != nil && errors.Is(err, NotFountError) {
		wsTMQErrorMsg(ctx, session, 0xffff, "message is nil", TMQFetchJsonMeta, req.ReqID, &req.MessageID)
		return
	}
	if err != nil {
		wsTMQErrorMsg(ctx, session, 0xffff, err.Error(), TMQFetchJsonMeta, req.ReqID, &req.MessageID)
		return
	}

	logger.Debugln("get message lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
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
		wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
		return
	}
	t.cleanupMessages()
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
	if t.closed {
		return
	}
	defer func() {
		t.asyncLocker.Lock()
		asynctmq.DestroyTMQThread(t.thread)
		t.asyncLocker.Unlock()
		tmqhandle.GlobalTMQHandlerPoll.Put(t.handler)
	}()
	t.closed = true
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
	t.stopRecordMessages()
}

func (t *TMQ) cleanupMessages() {
	t.messages.CleanAll()
}

func (t *TMQ) stopRecordMessages() {
	t.messages.StopAutoClean()
	t.messages.CleanAll()
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

func init() {
	c := NewTMQController()
	controller.AddController(c)
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
		Action:    TMQPosition,
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

	code, topicsPointer := wrapper.TMQSubscription(t.consumer)
	defer wrapper.TMQListDestroy(topicsPointer)
	topics := wrapper.TMQListToCArray(topicsPointer, int(wrapper.TMQListGetSize(topicsPointer)))
	logger.Debugln("tmq list topic cost:", log.GetLogDuration(log.IsDebug(), s))
	if code != 0 {
		errStr := wrapper.TMQErr2Str(code)
		logger.WithError(taoserrors.NewError(int(code), errStr)).Error("tmq list topic")
		wsTMQErrorMsg(ctx, session, int(code), errStr, TMQListTopics, req.ReqID, nil)
		return
	}
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
	asynctmq.TaosaTMQCommitOffset(t.thread, t.consumer, req.Topic, req.VgroupID, req.Offset, t.handler.Handler)
	code := <-t.handler.Caller.CommitResult
	t.asyncLocker.Unlock()
	if code != 0 {
		errMsg := wrapper.TMQErr2Str(code)
		logger.WithError(taoserrors.NewError(int(code), errMsg)).Error("tmq commit offset")
		wsTMQErrorMsg(ctx, session, int(code), errMsg, TMQCommitOffset, req.ReqID, nil)
		return
	}
	t.cleanupMessageByOffset(req.Topic, req.VgroupID, req.Offset)

	wstool.WSWriteJson(session, TMQCommitOffsetResp{
		Action:   TMQCommitOffset,
		ReqID:    req.ReqID,
		Timing:   wstool.GetDuration(ctx),
		Topic:    req.Topic,
		VgroupID: req.VgroupID,
		Offset:   req.Offset,
	})
}

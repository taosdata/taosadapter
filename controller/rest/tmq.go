package rest

import (
	"bytes"
	"container/list"
	"encoding/json"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/huskar-t/melody"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/thread"
	"github.com/taosdata/taosadapter/tools/jsontype"
	"github.com/taosdata/taosadapter/tools/web"
)

const (
	TMQInit        = "init"
	TMQSubscribe   = "subscribe"
	TMQPoll        = "poll"
	TMQFetch       = "fetch"
	TMQFetchBlock  = "fetch_block"
	TMQCommit      = "commit"
	TMQUnSubscribe = "unsubscribe"
	TMQClose       = "close"
)
const TaosTMQKey = "taos_tmq"

type TMQ struct {
	Session       *melody.Session
	listLocker    sync.RWMutex
	consumers     *list.List
	consumerIndex uint64
	closed        bool
	sync.Mutex
}

func NewTaosTMQ() *TMQ {
	return &TMQ{consumers: list.New()}
}

type Consumer struct {
	index    uint64
	cPointer unsafe.Pointer
	message  unsafe.Pointer
	buffer   *bytes.Buffer
	sync.Mutex
}

func (c *Consumer) setMessage(message unsafe.Pointer) {
	c.Lock()
	defer c.Unlock()
	if c.message != nil {
		thread.Lock()
		wrapper.TaosFreeResult(c.message)
		thread.Unlock()
	}
	c.message = message
}

func (c *Consumer) cleanUp() {
	c.Lock()
	defer c.Unlock()
	if c.message != nil {
		thread.Lock()
		wrapper.TaosFreeResult(c.message)
		thread.Unlock()
	}
	c.message = nil
	if c.cPointer != nil {
		thread.Lock()
		wrapper.TMQConsumerClose(c.cPointer)
		thread.Unlock()
	}
	c.cPointer = nil
	c.buffer = nil
}

func (t *TMQ) addConsumer(consumer *Consumer) {
	index := atomic.AddUint64(&t.consumerIndex, 1)
	consumer.index = index
	t.listLocker.Lock()
	t.consumers.PushBack(consumer)
	t.listLocker.Unlock()
}
func (t *TMQ) getConsumer(index uint64) *list.Element {
	t.listLocker.RLock()
	defer t.listLocker.RUnlock()
	root := t.consumers.Front()
	if root == nil {
		return nil
	}
	rootIndex := root.Value.(*Consumer).index
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
func (t *TMQ) removeConsumer(item *list.Element) {
	t.listLocker.Lock()
	t.consumers.Remove(item)
	t.listLocker.Unlock()
}

type TMQInitReq struct {
	ReqID      uint64 `json:"req_id"`
	User       string `json:"user"`
	Password   string `json:"password"`
	DB         string `json:"db"`
	GroupID    string `json:"group_id"`
	ClientID   string `json:"client_id"`
	OffsetRest string `json:"offset_rest"`
	// 如果允许输入 ip 和 port 会不会造成 SSRF ?
}

type TMQInitResp struct {
	Code       int    `json:"code"`
	Message    string `json:"message"`
	Action     string `json:"action"`
	ReqID      uint64 `json:"req_id"`
	ConsumerID uint64 `json:"consumer_id"`
}

func (t *TMQ) init(session *melody.Session, req *TMQInitReq) {
	t.Lock()
	defer t.Unlock()
	config := wrapper.TMQConfNew()
	defer func() {
		wrapper.TMQConfDestroy(config)
	}()
	if len(req.GroupID) != 0 {
		errCode := wrapper.TMQConfSet(config, "group.id", req.GroupID)
		if errCode != errors.SUCCESS {
			errStr := wrapper.TMQErr2Str(errCode)
			wsErrorMsg(session, int(errCode), errStr, TMQInit, req.ReqID)
			return
		}
	}
	if len(req.ClientID) != 0 {
		errCode := wrapper.TMQConfSet(config, "client.id", req.ClientID)
		if errCode != errors.SUCCESS {
			errStr := wrapper.TMQErr2Str(errCode)
			wsErrorMsg(session, int(errCode), errStr, TMQInit, req.ReqID)
			return
		}
	}
	if len(req.DB) != 0 {
		errCode := wrapper.TMQConfSet(config, "td.connect.db", req.DB)
		if errCode != errors.SUCCESS {
			errStr := wrapper.TMQErr2Str(errCode)
			wsErrorMsg(session, int(errCode), errStr, TMQInit, req.ReqID)
			return
		}
	}

	if len(req.OffsetRest) != 0 {
		errCode := wrapper.TMQConfSet(config, "auto.offset.reset", req.DB)
		if errCode != errors.SUCCESS {
			errStr := wrapper.TMQErr2Str(errCode)
			wsErrorMsg(session, int(errCode), errStr, TMQInit, req.ReqID)
			return
		}
	}

	errCode := wrapper.TMQConfSet(config, "td.connect.user", req.User)
	if errCode != errors.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		wsErrorMsg(session, int(errCode), errStr, TMQInit, req.ReqID)
		return
	}
	errCode = wrapper.TMQConfSet(config, "td.connect.pass", req.Password)
	if errCode != errors.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		wsErrorMsg(session, int(errCode), errStr, TMQInit, req.ReqID)
		return
	}
	errCode = wrapper.TMQConfSet(config, "msg.with.table.name", "true")
	if errCode != errors.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		wsErrorMsg(session, int(errCode), errStr, TMQInit, req.ReqID)
		return
	}
	errCode = wrapper.TMQConfSet(config, "enable.auto.commit", "false")
	if errCode != errors.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		wsErrorMsg(session, int(errCode), errStr, TMQInit, req.ReqID)
		return
	}
	thread.Lock()
	cPointer, err := wrapper.TMQConsumerNew(config)
	thread.Unlock()
	if err != nil {
		wsErrorMsg(session, 0xffff, err.Error(), TMQInit, req.ReqID)
		return
	}
	consumer := &Consumer{
		cPointer: cPointer,
	}
	t.addConsumer(consumer)
	wsWriteJson(session, &TMQInitResp{
		Action:     TMQInit,
		ReqID:      req.ReqID,
		ConsumerID: consumer.index,
	})
}

type TMQSubscribeReq struct {
	ReqID      uint64   `json:"req_id"`
	ConsumerID uint64   `json:"consumer_id"`
	Topics     []string `json:"topics"`
}
type TMQSubscribeResp struct {
	Code       int    `json:"code"`
	Message    string `json:"message"`
	Action     string `json:"action"`
	ReqID      uint64 `json:"req_id"`
	ConsumerID uint64 `json:"consumer_id"`
}

func (t *TMQ) subscribe(session *melody.Session, req *TMQSubscribeReq) {
	consumerItem := t.getConsumer(req.ConsumerID)
	if consumerItem == nil {
		wsErrorMsg(session, 0xffff, "consumer is nil", TMQSubscribe, req.ReqID)
		return
	}
	consumer := consumerItem.Value.(*Consumer)
	topicList := wrapper.TMQListNew()
	defer func() {
		wrapper.TMQListDestroy(topicList)
	}()
	for _, topic := range req.Topics {
		errCode := wrapper.TMQListAppend(topicList, topic)
		if errCode != 0 {
			errStr := wrapper.TMQErr2Str(errCode)
			wsErrorMsg(session, int(errCode), errStr, TMQSubscribe, req.ReqID)
			return
		}
	}
	thread.Lock()
	errCode := wrapper.TMQSubscribe(consumer.cPointer, topicList)
	thread.Unlock()
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		wsErrorMsg(session, int(errCode), errStr, TMQSubscribe, req.ReqID)
		return
	}
	wsWriteJson(session, &TMQSubscribeResp{
		Action:     TMQSubscribe,
		ReqID:      req.ReqID,
		ConsumerID: req.ConsumerID,
	})
}

type TMQUnsubscribeReq struct {
	ReqID      uint64 `json:"req_id"`
	ConsumerID uint64 `json:"consumer_id"`
}
type TMQUnsubscribeResp struct {
	Code       int    `json:"code"`
	Message    string `json:"message"`
	Action     string `json:"action"`
	ReqID      uint64 `json:"req_id"`
	ConsumerID uint64 `json:"consumer_id"`
}

func (t *TMQ) unsubscribe(session *melody.Session, req *TMQUnsubscribeReq) {
	consumerItem := t.getConsumer(req.ConsumerID)
	if consumerItem == nil {
		wsErrorMsg(session, 0xffff, "consumer is nil", TMQUnSubscribe, req.ReqID)
		return
	}
	consumer := consumerItem.Value.(*Consumer)
	thread.Lock()
	errCode := wrapper.TMQUnsubscribe(consumer.cPointer)
	thread.Unlock()
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		wsErrorMsg(session, int(errCode), errStr, TMQUnSubscribe, req.ReqID)
		return
	}
	wsWriteJson(session, &TMQUnsubscribeResp{
		Action:     TMQUnSubscribe,
		ReqID:      req.ReqID,
		ConsumerID: req.ConsumerID,
	})
}

type TMQCommitReq struct {
	ReqID      uint64 `json:"req_id"`
	ConsumerID uint64 `json:"consumer_id"`
}
type TMQCommitResp struct {
	Code       int    `json:"code"`
	Message    string `json:"message"`
	Action     string `json:"action"`
	ReqID      uint64 `json:"req_id"`
	ConsumerID uint64 `json:"consumer_id"`
}

func (t *TMQ) commit(session *melody.Session, req *TMQCommitReq) {
	consumerItem := t.getConsumer(req.ConsumerID)
	if consumerItem == nil {
		wsErrorMsg(session, 0xffff, "consumer is nil", TMQCommit, req.ReqID)
		return
	}
	consumer := consumerItem.Value.(*Consumer)
	thread.Lock()
	errCode := wrapper.TMQCommitSync(consumer.cPointer, nil)
	thread.Unlock()
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		wsErrorMsg(session, int(errCode), errStr, TMQCommit, req.ReqID)
		return
	}
	wsWriteJson(session, &TMQCommitResp{
		Action:     TMQCommit,
		ReqID:      req.ReqID,
		ConsumerID: req.ConsumerID,
	})
}

type TMQPollReq struct {
	ReqID        uint64 `json:"req_id"`
	ConsumerID   uint64 `json:"consumer_id"`
	BlockingTime int64  `json:"blocking_time"`
}

type TMQPollResp struct {
	Code        int    `json:"code"`
	Message     string `json:"message"`
	Action      string `json:"action"`
	ReqID       uint64 `json:"req_id"`
	ConsumerID  uint64 `json:"consumer_id"`
	HaveMessage bool   `json:"have_message"`
	Topic       string `json:"topic"`
	Database    string `json:"database"`
	VgroupID    int32  `json:"vgroup_id"`
}

func (t *TMQ) poll(session *melody.Session, req *TMQPollReq) {
	consumerItem := t.getConsumer(req.ConsumerID)
	if consumerItem == nil {
		wsErrorMsg(session, 0xffff, "consumer is nil", TMQPoll, req.ReqID)
		return
	}
	consumer := consumerItem.Value.(*Consumer)
	if req.BlockingTime > 1000 {
		req.BlockingTime = 1000
	}
	thread.Lock()
	message := wrapper.TMQConsumerPoll(consumer.cPointer, req.BlockingTime)
	thread.Unlock()
	resp := &TMQPollResp{
		Action:     TMQPoll,
		ReqID:      req.ReqID,
		ConsumerID: req.ConsumerID,
	}
	if message != nil {
		consumer.setMessage(message)
		resp.HaveMessage = true
		thread.Lock()
		resp.Topic = wrapper.TMQGetTopicName(message)
		resp.Database = wrapper.TMQGetDBName(message)
		resp.VgroupID = wrapper.TMQGetVgroupID(message)
		thread.Unlock()
	}
	wsWriteJson(session, resp)
}

type TMQFetchReq struct {
	ReqID      uint64 `json:"req_id"`
	ConsumerID uint64 `json:"consumer_id"`
}
type TMQFetchResp struct {
	Code          int                `json:"code"`
	Message       string             `json:"message"`
	Action        string             `json:"action"`
	ReqID         uint64             `json:"req_id"`
	ConsumerID    uint64             `json:"consumer_id"`
	Completed     bool               `json:"completed"`
	TableName     string             `json:"table_name"`
	Rows          int                `json:"rows"`
	FieldsCount   int                `json:"fields_count"`
	FieldsNames   []string           `json:"fields_names"`
	FieldsTypes   jsontype.JsonUint8 `json:"fields_types"`
	FieldsLengths []int64            `json:"fields_lengths"`
	Precision     int                `json:"precision"`
}

func (t *TMQ) fetch(session *melody.Session, req *TMQFetchReq) {
	consumerItem := t.getConsumer(req.ConsumerID)
	if consumerItem == nil {
		wsErrorMsg(session, 0xffff, "consumer is nil", TMQFetch, req.ReqID)
		return
	}
	consumer := consumerItem.Value.(*Consumer)
	consumer.Lock()
	if consumer.message == nil {
		consumer.Unlock()
		wsErrorMsg(session, 0xffff, "consumer message is nil", TMQFetch, req.ReqID)
		return
	}
	thread.Lock()
	blockSize, errCode, block := wrapper.TaosFetchRawBlock(consumer.message)
	thread.Unlock()
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(int32(errCode))
		wsErrorMsg(session, errCode, errStr, TMQFetch, req.ReqID)
		return
	}
	resp := &TMQFetchResp{
		Action:     TMQFetch,
		ReqID:      req.ReqID,
		ConsumerID: req.ConsumerID,
	}
	if blockSize == 0 {
		thread.Lock()
		wrapper.TaosFreeResult(consumer.message)
		thread.Unlock()
		consumer.message = nil
		consumer.Unlock()
		resp.Completed = true
		wsWriteJson(session, resp)
		return
	}
	resp.TableName = wrapper.TMQGetTableName(consumer.message)
	resp.Rows = blockSize
	resp.FieldsCount = wrapper.TaosNumFields(consumer.message)
	rowsHeader, _ := wrapper.ReadColumn(consumer.message, resp.FieldsCount)
	resp.FieldsNames = rowsHeader.ColNames
	resp.FieldsTypes = rowsHeader.ColTypes
	resp.FieldsLengths = rowsHeader.ColLength
	resp.Precision = wrapper.TaosResultPrecision(consumer.message)
	if consumer.buffer == nil {
		consumer.buffer = new(bytes.Buffer)
	} else {
		consumer.buffer.Reset()
	}
	blockLength := int(*(*int32)(block))
	consumer.buffer.Grow(blockLength + 16)
	writeUint64(consumer.buffer, req.ReqID)
	writeUint64(consumer.buffer, req.ConsumerID)
	for offset := 0; offset < blockLength; offset++ {
		consumer.buffer.WriteByte(*((*byte)(unsafe.Pointer(uintptr(block) + uintptr(offset)))))
	}
	consumer.Unlock()
	wsWriteJson(session, resp)
}

type TMQFetchBlockReq struct {
	ReqID      uint64 `json:"req_id"`
	ConsumerID uint64 `json:"consumer_id"`
}

func (t *TMQ) fetchBlock(session *melody.Session, req *TMQFetchBlockReq) {
	consumerItem := t.getConsumer(req.ConsumerID)
	if consumerItem == nil {
		wsErrorMsg(session, 0xffff, "consumer is nil", TMQFetchBlock, req.ReqID)
		return
	}
	consumer := consumerItem.Value.(*Consumer)
	consumer.Lock()
	if consumer.buffer == nil || consumer.buffer.Len() == 0 {
		wsErrorMsg(session, 0xffff, "no fetch data", TMQFetchBlock, req.ReqID)
		return
	}
	b := consumer.buffer.Bytes()
	consumer.Unlock()
	session.WriteBinary(b)
}

type TMQCloseReq struct {
	ReqID      uint64 `json:"req_id"`
	ConsumerID uint64 `json:"consumer_id"`
}

func (t *TMQ) close(session *melody.Session, req *TMQCloseReq) {
	consumerItem := t.getConsumer(req.ConsumerID)
	if consumerItem == nil {
		wsErrorMsg(session, 0xffff, "consumer is nil", TMQClose, req.ReqID)
		return
	}
	consumer := consumerItem.Value.(*Consumer)
	consumer.cleanUp()
}

func (t *TMQ) Close() {
	t.Lock()
	defer t.Unlock()
	if t.closed {
		return
	}
	t.closed = true
	t.listLocker.Lock()
	defer t.listLocker.Unlock()
	root := t.consumers.Front()
	if root == nil {
		return
	}
	root.Value.(*Consumer).cleanUp()
	item := root.Next()
	for {
		if item == nil || item == root {
			return
		}
		item.Value.(*Consumer).cleanUp()
		item = item.Next()
	}
}

func (ctl *Restful) InitTMQ() {
	ctl.tmqM = melody.New()

	ctl.tmqM.HandleConnect(func(session *melody.Session) {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws connect")
		session.Set(TaosTMQKey, NewTaosTMQ())
	})

	ctl.tmqM.HandleMessage(func(session *melody.Session, data []byte) {
		if ctl.tmqM.IsClosed() {
			return
		}
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("get ws message data:", string(data))
		var action WSAction
		err := json.Unmarshal(data, &action)
		if err != nil {
			logger.WithError(err).Errorln("unmarshal ws request")
			return
		}
		switch action.Action {
		case TMQInit:
			var req TMQInitReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal connect request args")
				return
			}
			t := session.MustGet(TaosTMQKey)
			t.(*TMQ).init(session, &req)
		case TMQSubscribe:
			var req TMQSubscribeReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal query args")
				return
			}
			t := session.MustGet(TaosTMQKey)
			t.(*TMQ).subscribe(session, &req)
		case TMQPoll:
			var req TMQPollReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal fetch args")
				return
			}
			t := session.MustGet(TaosTMQKey)
			t.(*TMQ).poll(session, &req)
		case TMQFetch:
			var req TMQFetchReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal fetch args")
				return
			}
			t := session.MustGet(TaosTMQKey)
			t.(*TMQ).fetch(session, &req)
		case TMQFetchBlock:
			var req TMQFetchBlockReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal fetch args")
				return
			}
			t := session.MustGet(TaosTMQKey)
			t.(*TMQ).fetchBlock(session, &req)
		case TMQCommit:
			var req TMQCommitReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal fetch args")
				return
			}
			t := session.MustGet(TaosTMQKey)
			t.(*TMQ).commit(session, &req)
		case TMQUnSubscribe:
			var req TMQUnsubscribeReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal fetch args")
				return
			}
			t := session.MustGet(TaosTMQKey)
			t.(*TMQ).unsubscribe(session, &req)
		case TMQClose:
			var req TMQCloseReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal fetch args")
				return
			}
			t := session.MustGet(TaosTMQKey)
			t.(*TMQ).close(session, &req)
		}
	})
	ctl.tmqM.HandleClose(func(session *melody.Session, i int, s string) error {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws close", i, s)
		t, exist := session.Get(TaosTMQKey)
		if exist && t != nil {
			t.(*TMQ).Close()
		}
		return nil
	})

	ctl.tmqM.HandleError(func(session *melody.Session, err error) {
		logger := session.MustGet("logger").(*logrus.Entry)
		_, is := err.(*websocket.CloseError)
		if is {
			logger.WithError(err).Debugln("ws close in error")
		} else {
			logger.WithError(err).Errorln("ws error")
		}
		t, exist := session.Get(TaosTMQKey)
		if exist && t != nil {
			t.(*TMQ).Close()
		}
	})

	ctl.tmqM.HandleDisconnect(func(session *melody.Session) {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws disconnect")
		t, exist := session.Get(TaosTMQKey)
		if exist && t != nil {
			t.(*TMQ).Close()
		}
	})
}

func (ctl *Restful) tmq(c *gin.Context) {
	id := web.GetRequestID(c)
	loggerWithID := logger.WithField("sessionID", id)
	_ = ctl.tmqM.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": loggerWithID})
}

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
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/httperror"
	"github.com/taosdata/taosadapter/thread"
	"github.com/taosdata/taosadapter/tools/jsontype"
	"github.com/taosdata/taosadapter/tools/web"
)

const (
	TMQSubscribe  = "subscribe"
	TMQPoll       = "poll"
	TMQFetch      = "fetch"
	TMQFetchBlock = "fetch_block"
	TMQCommit     = "commit"
)
const TaosTMQKey = "taos_tmq"

type TMQ struct {
	Session    *melody.Session
	listLocker sync.RWMutex
	consumer   unsafe.Pointer
	messages   *list.List
	//consumers     *list.List
	messageIndex uint64
	closed       bool
	sync.Mutex
}

func NewTaosTMQ() *TMQ {
	return &TMQ{messages: list.New()}
}

type TMQMessage struct {
	index    uint64
	cPointer unsafe.Pointer
	buffer   *bytes.Buffer
	sync.Mutex
}

func (c *TMQMessage) cleanUp() {
	c.Lock()
	defer c.Unlock()
	if c.cPointer != nil {
		thread.Lock()
		wrapper.TaosFreeResult(c.cPointer)
		thread.Unlock()
	}
	c.cPointer = nil
	c.buffer = nil
}

func (t *TMQ) addMessage(message *TMQMessage) {
	index := atomic.AddUint64(&t.messageIndex, 1)
	message.index = index
	t.listLocker.Lock()
	t.messages.PushBack(message)
	t.listLocker.Unlock()
}
func (t *TMQ) getMessage(index uint64) *list.Element {

	root := t.messages.Front()
	if root == nil {
		return nil
	}
	rootIndex := root.Value.(*TMQMessage).index
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

type TMQSubscribeReq struct {
	ReqID      uint64 `json:"req_id"`
	User       string `json:"user"`
	Password   string `json:"password"`
	DB         string `json:"db"`
	GroupID    string `json:"group_id"`
	ClientID   string `json:"client_id"`
	OffsetRest string `json:"offset_rest"`
	// 如果允许输入 ip 和 port 会不会造成 SSRF ?
	Topics []string `json:"topics"`
}

type TMQSubscribeResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
}

func (t *TMQ) subscribe(session *melody.Session, req *TMQSubscribeReq) {
	t.Lock()
	defer t.Unlock()
	if t.consumer != nil {
		wsTMQErrorMsg(session, 0xffff, "tmq duplicate init", TMQSubscribe, req.ReqID, nil)
		return
	}
	config := wrapper.TMQConfNew()
	defer func() {
		wrapper.TMQConfDestroy(config)
	}()
	if len(req.GroupID) != 0 {
		errCode := wrapper.TMQConfSet(config, "group.id", req.GroupID)
		if errCode != httperror.SUCCESS {
			errStr := wrapper.TMQErr2Str(errCode)
			wsTMQErrorMsg(session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
	}
	if len(req.ClientID) != 0 {
		errCode := wrapper.TMQConfSet(config, "client.id", req.ClientID)
		if errCode != httperror.SUCCESS {
			errStr := wrapper.TMQErr2Str(errCode)
			wsTMQErrorMsg(session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
	}
	if len(req.DB) != 0 {
		errCode := wrapper.TMQConfSet(config, "td.connect.db", req.DB)
		if errCode != httperror.SUCCESS {
			errStr := wrapper.TMQErr2Str(errCode)
			wsTMQErrorMsg(session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
	}

	if len(req.OffsetRest) != 0 {
		errCode := wrapper.TMQConfSet(config, "auto.offset.reset", req.DB)
		if errCode != httperror.SUCCESS {
			errStr := wrapper.TMQErr2Str(errCode)
			wsTMQErrorMsg(session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
	}

	errCode := wrapper.TMQConfSet(config, "td.connect.user", req.User)
	if errCode != httperror.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		wsTMQErrorMsg(session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
		return
	}
	errCode = wrapper.TMQConfSet(config, "td.connect.pass", req.Password)
	if errCode != httperror.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		wsTMQErrorMsg(session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
		return
	}
	errCode = wrapper.TMQConfSet(config, "msg.with.table.name", "true")
	if errCode != httperror.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		wsTMQErrorMsg(session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
		return
	}
	errCode = wrapper.TMQConfSet(config, "enable.auto.commit", "false")
	if errCode != httperror.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		wsTMQErrorMsg(session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
		return
	}
	thread.Lock()
	cPointer, err := wrapper.TMQConsumerNew(config)
	thread.Unlock()
	if err != nil {
		wsTMQErrorMsg(session, 0xffff, err.Error(), TMQSubscribe, req.ReqID, nil)
		return
	}

	{
		topicList := wrapper.TMQListNew()
		defer func() {
			wrapper.TMQListDestroy(topicList)
		}()
		for _, topic := range req.Topics {
			errCode := wrapper.TMQListAppend(topicList, topic)
			if errCode != 0 {
				thread.Lock()
				_ = wrapper.TMQConsumerClose(cPointer)
				thread.Unlock()
				errStr := wrapper.TMQErr2Str(errCode)
				wsTMQErrorMsg(session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
				return
			}
		}
		thread.Lock()
		errCode = wrapper.TMQSubscribe(cPointer, topicList)
		thread.Unlock()
		if errCode != 0 {
			thread.Lock()
			_ = wrapper.TMQConsumerClose(cPointer)
			thread.Unlock()
			errStr := wrapper.TMQErr2Str(errCode)
			wsTMQErrorMsg(session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
	}
	t.consumer = cPointer
	wsWriteJson(session, &TMQSubscribeResp{
		Action: TMQSubscribe,
		ReqID:  req.ReqID,
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
	MessageID uint64 `json:"message_id"`
}

func (t *TMQ) commit(session *melody.Session, req *TMQCommitReq) {
	if t.consumer == nil {
		wsTMQErrorMsg(session, 0xffff, "tmq not init", TMQCommit, req.ReqID, nil)
		return
	}
	t.listLocker.Lock()
	resp := &TMQCommitResp{
		Action:    TMQCommit,
		ReqID:     req.ReqID,
		MessageID: req.MessageID,
	}
	messageItem := t.getMessage(req.MessageID)
	if messageItem == nil {
		t.listLocker.Unlock()
		wsWriteJson(session, resp)
		return
	}
	message := messageItem.Value.(*TMQMessage)
	thread.Lock()
	errCode := wrapper.TMQCommitSync(t.consumer, message.cPointer)
	thread.Unlock()
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		wsTMQErrorMsg(session, int(errCode), errStr, TMQCommit, req.ReqID, nil)
		return
	}
	item := t.messages.Front()
	next := item.Next()
	for {
		item.Value.(*TMQMessage).cleanUp()
		t.messages.Remove(messageItem)
		if item == messageItem {
			break
		}
		item = next
		next = item.Next()
	}
	t.listLocker.Unlock()
	wsWriteJson(session, resp)
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
	HaveMessage bool   `json:"have_message"`
	Topic       string `json:"topic"`
	Database    string `json:"database"`
	VgroupID    int32  `json:"vgroup_id"`
	MessageID   uint64 `json:"message_id"`
}

func (t *TMQ) poll(session *melody.Session, req *TMQPollReq) {
	if t.consumer == nil {
		wsTMQErrorMsg(session, 0xffff, "tmq not init", TMQPoll, req.ReqID, nil)
		return
	}
	if req.BlockingTime > 1000 {
		req.BlockingTime = 1000
	}
	thread.Lock()
	message := wrapper.TMQConsumerPoll(t.consumer, req.BlockingTime)
	thread.Unlock()
	resp := &TMQPollResp{
		Action: TMQPoll,
		ReqID:  req.ReqID,
	}
	if message != nil {
		m := &TMQMessage{
			cPointer: message,
			buffer:   new(bytes.Buffer),
		}
		t.addMessage(m)
		resp.HaveMessage = true
		thread.Lock()
		resp.Topic = wrapper.TMQGetTopicName(message)
		resp.Database = wrapper.TMQGetDBName(message)
		resp.VgroupID = wrapper.TMQGetVgroupID(message)
		thread.Unlock()
		resp.MessageID = m.index
	}
	wsWriteJson(session, resp)
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

func (t *TMQ) fetch(session *melody.Session, req *TMQFetchReq) {
	if t.consumer == nil {
		wsTMQErrorMsg(session, 0xffff, "tmq not init", TMQFetch, req.ReqID, &req.MessageID)
		return
	}
	t.listLocker.RLock()
	messageItem := t.getMessage(req.MessageID)
	t.listLocker.RUnlock()
	if messageItem == nil {
		wsTMQErrorMsg(session, 0xffff, "message is nil", TMQFetch, req.ReqID, &req.MessageID)
		return
	}
	message := messageItem.Value.(*TMQMessage)
	message.Lock()
	thread.Lock()
	blockSize, errCode, block := wrapper.TaosFetchRawBlock(message.cPointer)
	thread.Unlock()
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(int32(errCode))
		message.Unlock()
		wsTMQErrorMsg(session, errCode, errStr, TMQFetch, req.ReqID, &req.MessageID)
		return
	}
	resp := &TMQFetchResp{
		Action:    TMQFetch,
		ReqID:     req.ReqID,
		MessageID: req.MessageID,
	}
	if blockSize == 0 {
		message.Unlock()
		resp.Completed = true
		wsWriteJson(session, resp)
		return
	}
	resp.TableName = wrapper.TMQGetTableName(message.cPointer)
	resp.Rows = blockSize
	resp.FieldsCount = wrapper.TaosNumFields(message.cPointer)
	rowsHeader, _ := wrapper.ReadColumn(message.cPointer, resp.FieldsCount)
	resp.FieldsNames = rowsHeader.ColNames
	resp.FieldsTypes = rowsHeader.ColTypes
	resp.FieldsLengths = rowsHeader.ColLength
	resp.Precision = wrapper.TaosResultPrecision(message.cPointer)
	if message.buffer == nil {
		message.buffer = new(bytes.Buffer)
	} else {
		message.buffer.Reset()
	}
	blockLength := int(*(*int32)(block))
	message.buffer.Grow(blockLength + 16)
	writeUint64(message.buffer, req.ReqID)
	writeUint64(message.buffer, req.MessageID)
	for offset := 0; offset < blockLength; offset++ {
		message.buffer.WriteByte(*((*byte)(unsafe.Pointer(uintptr(block) + uintptr(offset)))))
	}
	message.Unlock()
	wsWriteJson(session, resp)
}

type TMQFetchBlockReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}

func (t *TMQ) fetchBlock(session *melody.Session, req *TMQFetchBlockReq) {
	if t.consumer == nil {
		wsTMQErrorMsg(session, 0xffff, "tmq not init", TMQFetchBlock, req.ReqID, &req.MessageID)
		return
	}
	t.listLocker.RLock()
	messageItem := t.getMessage(req.MessageID)
	t.listLocker.RUnlock()
	if messageItem == nil {
		wsTMQErrorMsg(session, 0xffff, "message is nil", TMQFetchBlock, req.ReqID, &req.MessageID)
		return
	}
	message := messageItem.Value.(*TMQMessage)
	message.Lock()
	if message.buffer == nil || message.buffer.Len() == 0 {
		wsTMQErrorMsg(session, 0xffff, "no fetch data", TMQFetchBlock, req.ReqID, &req.MessageID)
		return
	}
	b := message.buffer.Bytes()
	message.Unlock()
	session.WriteBinary(b)
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
	item := t.messages.Front()
	if item == nil {
		return
	}
	next := item.Next()
	for {
		item.Value.(*TMQMessage).cleanUp()
		item = next
		if item == nil {
			return
		}
		next = item.Next()
	}
}

func (ctl *Restful) InitTMQ() {
	ctl.tmqM = melody.New()
	ctl.tmqM.Config.MaxMessageSize = 4 * 1024 * 1024

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
		case ClientVersion:
			session.Write(ctl.wsVersionResp)
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

type WSTMQErrorResp struct {
	Code      int     `json:"code"`
	Message   string  `json:"message"`
	Action    string  `json:"action"`
	ReqID     uint64  `json:"req_id"`
	MessageID *uint64 `json:"message_id,omitempty"`
}

func wsTMQErrorMsg(session *melody.Session, code int, message string, action string, reqID uint64, messageID *uint64) {
	b, _ := json.Marshal(&WSTMQErrorResp{
		Code:      code & 0xffff,
		Message:   message,
		Action:    action,
		ReqID:     reqID,
		MessageID: messageID,
	})
	session.Write(b)
}

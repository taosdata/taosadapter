package rest

import (
	"bytes"
	"container/list"
	"context"
	"encoding/binary"
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/huskar-t/melody"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/parser"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/thread"
	"github.com/taosdata/taosadapter/v3/tools/jsontype"
	"github.com/taosdata/taosadapter/v3/tools/web"
)

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
	index       uint64
	cPointer    unsafe.Pointer
	buffer      *bytes.Buffer
	messageType int32
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
		if item.Value.(*TMQMessage).index == index {
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
	Timing  int64  `json:"timing"`
}

func (t *TMQ) subscribe(ctx context.Context, session *melody.Session, req *TMQSubscribeReq) {
	logger := getLogger(session).WithField("action", TMQSubscribe)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.Lock()
	logger.Debugln("get global lock cost:", log.GetLogDuration(isDebug, s))
	defer t.Unlock()
	if t.consumer != nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "tmq duplicate init", TMQSubscribe, req.ReqID, nil)
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
			wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
	}
	if len(req.ClientID) != 0 {
		errCode := wrapper.TMQConfSet(config, "client.id", req.ClientID)
		if errCode != httperror.SUCCESS {
			errStr := wrapper.TMQErr2Str(errCode)
			wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
	}
	if len(req.DB) != 0 {
		errCode := wrapper.TMQConfSet(config, "td.connect.db", req.DB)
		if errCode != httperror.SUCCESS {
			errStr := wrapper.TMQErr2Str(errCode)
			wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
	}

	if len(req.OffsetRest) != 0 {
		errCode := wrapper.TMQConfSet(config, "auto.offset.reset", req.OffsetRest)
		if errCode != httperror.SUCCESS {
			errStr := wrapper.TMQErr2Str(errCode)
			wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
	}

	errCode := wrapper.TMQConfSet(config, "td.connect.user", req.User)
	if errCode != httperror.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
		return
	}
	errCode = wrapper.TMQConfSet(config, "td.connect.pass", req.Password)
	if errCode != httperror.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
		return
	}
	errCode = wrapper.TMQConfSet(config, "msg.with.table.name", "true")
	if errCode != httperror.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
		return
	}
	errCode = wrapper.TMQConfSet(config, "enable.auto.commit", "false")
	if errCode != httperror.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
		return
	}
	errCode = wrapper.TMQConfSet(config, "enable.heartbeat.background", "true")
	if errCode != httperror.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
		return
	}
	errCode = wrapper.TMQConfSet(config, "experimental.snapshot.enable", "true")
	if errCode != httperror.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
		return
	}
	s = log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("tmq_consumer_new get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	cPointer, err := wrapper.TMQConsumerNew(config)
	logger.Debugln("tmq_consumer_new cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if err != nil {
		wsTMQErrorMsg(ctx, session, 0xffff, err.Error(), TMQSubscribe, req.ReqID, nil)
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
				s = log.GetLogNow(isDebug)
				thread.Lock()
				logger.Debugln("tmq_consumer_close get thread lock cost:", log.GetLogDuration(isDebug, s))
				s = log.GetLogNow(isDebug)
				_ = wrapper.TMQConsumerClose(cPointer)
				logger.Debugln("tmq_consumer_close cost:", log.GetLogDuration(isDebug, s))
				thread.Unlock()
				errStr := wrapper.TMQErr2Str(errCode)
				wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
				return
			}
		}
		s = log.GetLogNow(isDebug)
		thread.Lock()
		logger.Debugln("tmq_subscribe get thread lock cost:", log.GetLogDuration(isDebug, s))
		s = log.GetLogNow(isDebug)
		errCode = wrapper.TMQSubscribe(cPointer, topicList)
		logger.Debugln("tmq_subscribe cost:", log.GetLogDuration(isDebug, s))
		thread.Unlock()
		if errCode != 0 {
			s = log.GetLogNow(isDebug)
			thread.Lock()
			logger.Debugln("tmq_consumer_close get thread lock cost:", log.GetLogDuration(isDebug, s))
			s = log.GetLogNow(isDebug)
			_ = wrapper.TMQConsumerClose(cPointer)
			logger.Debugln("tmq_consumer_close cost:", log.GetLogDuration(isDebug, s))
			thread.Unlock()
			errStr := wrapper.TMQErr2Str(errCode)
			wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQSubscribe, req.ReqID, nil)
			return
		}
	}
	t.consumer = cPointer
	wsWriteJson(session, &TMQSubscribeResp{
		Action: TMQSubscribe,
		ReqID:  req.ReqID,
		Timing: getDuration(ctx),
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

// todo
func (t *TMQ) commit(ctx context.Context, session *melody.Session, req *TMQCommitReq) {
	if t.consumer == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "tmq not init", TMQCommit, req.ReqID, nil)
		return
	}
	logger := getLogger(session).WithField("action", TMQCommit)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.listLocker.Lock()
	logger.Debugln("get list lock cost:", log.GetLogDuration(isDebug, s))
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
	s = log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	errCode := wrapper.TMQCommitSync(t.consumer, message.cPointer)
	logger.Debugln("tmq_commit_sync cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		wsTMQErrorMsg(ctx, session, int(errCode), errStr, TMQCommit, req.ReqID, nil)
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
	resp.Timing = getDuration(ctx)
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
	Timing      int64  `json:"timing"`
	HaveMessage bool   `json:"have_message"`
	Topic       string `json:"topic"`
	Database    string `json:"database"`
	VgroupID    int32  `json:"vgroup_id"`
	MessageType int32  `json:"message_type"`
	MessageID   uint64 `json:"message_id"`
}

func (t *TMQ) poll(ctx context.Context, session *melody.Session, req *TMQPollReq) {
	if t.consumer == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "tmq not init", TMQPoll, req.ReqID, nil)
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
		messageType := wrapper.TMQGetResType(message)
		if messageTypeIsValid(messageType) {
			m := &TMQMessage{
				cPointer:    message,
				buffer:      new(bytes.Buffer),
				messageType: messageType,
			}
			t.addMessage(m)
			resp.HaveMessage = true
			resp.Topic = wrapper.TMQGetTopicName(message)
			resp.Database = wrapper.TMQGetDBName(message)
			resp.VgroupID = wrapper.TMQGetVgroupID(message)
			resp.MessageID = m.index
			resp.MessageType = messageType
		} else {
			wsTMQErrorMsg(ctx, session, 0xffff, "unavailable tmq type:"+strconv.Itoa(int(messageType)), TMQPoll, req.ReqID, nil)
			return
		}
	}
	resp.Timing = getDuration(ctx)
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
	logger := getLogger(session).WithField("action", WSFetch)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.listLocker.RLock()
	logger.Debugln("get list lock cost:", log.GetLogDuration(isDebug, s))
	messageItem := t.getMessage(req.MessageID)
	t.listLocker.RUnlock()
	if messageItem == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "message is nil", TMQFetch, req.ReqID, &req.MessageID)
		return
	}
	message := messageItem.Value.(*TMQMessage)
	if !canGetData(message.messageType) {
		wsTMQErrorMsg(ctx, session, 0xffff, "message type is not data", TMQFetch, req.ReqID, &req.MessageID)
		return
	}
	s = log.GetLogNow(isDebug)
	message.Lock()
	logger.Debugln("get message lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	blockSize, errCode, block := wrapper.TaosFetchRawBlock(message.cPointer)
	logger.Debugln("taos_fetch_raw_block cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(int32(errCode))
		message.Unlock()
		wsTMQErrorMsg(ctx, session, errCode, errStr, TMQFetch, req.ReqID, &req.MessageID)
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
	s = log.GetLogNow(isDebug)
	resp.TableName = wrapper.TMQGetTableName(message.cPointer)
	logger.Debugln("tmq_get_table_name cost:", log.GetLogDuration(isDebug, s))
	resp.Rows = blockSize
	s = log.GetLogNow(isDebug)
	resp.FieldsCount = wrapper.TaosNumFields(message.cPointer)
	logger.Debugln("taos_num_fields cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	rowsHeader, _ := wrapper.ReadColumn(message.cPointer, resp.FieldsCount)
	logger.Debugln("read column cost:", log.GetLogDuration(isDebug, s))
	resp.FieldsNames = rowsHeader.ColNames
	resp.FieldsTypes = rowsHeader.ColTypes
	resp.FieldsLengths = rowsHeader.ColLength
	s = log.GetLogNow(isDebug)
	resp.Precision = wrapper.TaosResultPrecision(message.cPointer)
	logger.Debugln("taos_result_precision cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	if message.buffer == nil {
		message.buffer = new(bytes.Buffer)
	} else {
		message.buffer.Reset()
	}
	blockLength := int(parser.RawBlockGetLength(block))
	message.buffer.Grow(blockLength + 24)
	writeUint64(message.buffer, 0)
	writeUint64(message.buffer, req.ReqID)
	writeUint64(message.buffer, req.MessageID)
	for offset := 0; offset < blockLength; offset++ {
		message.buffer.WriteByte(*((*byte)(unsafe.Pointer(uintptr(block) + uintptr(offset)))))
	}
	message.Unlock()
	resp.Timing = getDuration(ctx)
	logger.Debugln("handle data cost:", log.GetLogDuration(isDebug, s))
	wsWriteJson(session, resp)
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
	logger := getLogger(session).WithField("action", TMQFetchBlock)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.listLocker.RLock()
	logger.Debugln("get list lock cost:", log.GetLogDuration(isDebug, s))
	messageItem := t.getMessage(req.MessageID)
	t.listLocker.RUnlock()
	if messageItem == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "message is nil", TMQFetchBlock, req.ReqID, &req.MessageID)
		return
	}
	message := messageItem.Value.(*TMQMessage)
	if !canGetData(message.messageType) {
		wsTMQErrorMsg(ctx, session, 0xffff, "message type is not data", TMQFetchBlock, req.ReqID, &req.MessageID)
		return
	}
	message.Lock()
	if message.buffer == nil || message.buffer.Len() == 0 {
		message.Unlock()
		wsTMQErrorMsg(ctx, session, 0xffff, "no fetch data", TMQFetchBlock, req.ReqID, &req.MessageID)
		return
	}
	s = log.GetLogNow(isDebug)
	b := message.buffer.Bytes()
	message.Unlock()
	binary.LittleEndian.PutUint64(b, uint64(getDuration(ctx)))
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
	logger := getLogger(session).WithField("action", TMQFetchRaw)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.listLocker.RLock()
	logger.Debugln("get list lock cost:", log.GetLogDuration(isDebug, s))
	messageItem := t.getMessage(req.MessageID)
	t.listLocker.RUnlock()
	if messageItem == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "message is nil", TMQFetchRaw, req.ReqID, &req.MessageID)
		return
	}
	message := messageItem.Value.(*TMQMessage)
	message.Lock()
	s = log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("tmq_get_raw get lock cost:", log.GetLogDuration(isDebug, s))
	s = time.Now()
	errCode, rawMeta := wrapper.TMQGetRaw(message.cPointer)
	logger.Debugln("tmq_get_raw cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		message.Unlock()
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
	writeUint64(message.buffer, uint64(getDuration(ctx)))
	writeUint64(message.buffer, req.ReqID)
	writeUint64(message.buffer, req.MessageID)
	writeUint64(message.buffer, TMQRawMessage)
	writeUint32(message.buffer, length)
	writeUint16(message.buffer, metaType)
	for offset := 0; offset < int(length); offset++ {
		message.buffer.WriteByte(*((*byte)(unsafe.Pointer(uintptr(data) + uintptr(offset)))))
	}
	s1 := time.Now()
	wrapper.TMQFreeRaw(rawMeta)
	logger.Debugln("tmq_free_raw cost:", log.GetLogDuration(isDebug, s1))
	message.Unlock()
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
	logger := getLogger(session).WithField("action", TMQFetchJsonMeta)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.listLocker.RLock()
	logger.Debugln("get list lock cost:", log.GetLogDuration(isDebug, s))
	messageItem := t.getMessage(req.MessageID)
	t.listLocker.RUnlock()
	if messageItem == nil {
		wsTMQErrorMsg(ctx, session, 0xffff, "message is nil", TMQFetchJsonMeta, req.ReqID, &req.MessageID)
		return
	}
	message := messageItem.Value.(*TMQMessage)
	if !canGetMeta(message.messageType) {
		wsTMQErrorMsg(ctx, session, 0xffff, "message type is not meta", TMQFetchJsonMeta, req.ReqID, &req.MessageID)
		return
	}
	s = log.GetLogNow(isDebug)
	message.Lock()
	logger.Debugln("get message lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	jsonMeta := wrapper.TMQGetJsonMeta(message.cPointer)
	logger.Debugln("tmq_get_json_meta cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
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
	message.Unlock()
	resp.Timing = getDuration(ctx)
	wsWriteJson(session, resp)
}

func (t *TMQ) Close(logger logrus.FieldLogger) {
	t.Lock()
	defer t.Unlock()
	if t.closed {
		return
	}
	t.closed = true
	defer func() {
		if t.consumer != nil {
			thread.Lock()
			errCode := wrapper.TMQConsumerClose(t.consumer)
			thread.Unlock()
			if errCode != 0 {
				errMsg := wrapper.TMQErr2Str(errCode)
				logger.WithError(errors.NewError(int(errCode), errMsg)).Error("tmq close consumer")
			}
		}
	}()
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
		ctx := context.WithValue(context.Background(), StartTimeKey, time.Now().UnixNano())
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
				logger.WithError(err).Errorln("unmarshal subscribe args")
				return
			}
			t := session.MustGet(TaosTMQKey)
			t.(*TMQ).subscribe(ctx, session, &req)
		case TMQPoll:
			var req TMQPollReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal pool args")
				return
			}
			t := session.MustGet(TaosTMQKey)
			t.(*TMQ).poll(ctx, session, &req)
		case TMQFetch:
			var req TMQFetchReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal fetch args")
				return
			}
			t := session.MustGet(TaosTMQKey)
			t.(*TMQ).fetch(ctx, session, &req)
		case TMQFetchBlock:
			var req TMQFetchBlockReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal fetch block args")
				return
			}
			t := session.MustGet(TaosTMQKey)
			t.(*TMQ).fetchBlock(ctx, session, &req)
		case TMQCommit:
			var req TMQCommitReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal commit args")
				return
			}
			t := session.MustGet(TaosTMQKey)
			t.(*TMQ).commit(ctx, session, &req)
		case TMQFetchJsonMeta:
			var req TMQFetchJsonMetaReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal fetch json meta args")
				return
			}
			t := session.MustGet(TaosTMQKey)
			t.(*TMQ).fetchJsonMeta(ctx, session, &req)
		case TMQFetchRaw:
			var req TMQFetchRawMetaReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal fetch raw meta args")
				return
			}
			t := session.MustGet(TaosTMQKey)
			t.(*TMQ).fetchRawMeta(ctx, session, &req)
		default:
			logger.WithError(err).Errorln("unknown action: " + action.Action)
			return
		}
	})
	ctl.tmqM.HandleClose(func(session *melody.Session, i int, s string) error {
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
			t.(*TMQ).Close(logger)
		}
	})

	ctl.tmqM.HandleDisconnect(func(session *melody.Session) {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws disconnect")
		t, exist := session.Get(TaosTMQKey)
		if exist && t != nil {
			t.(*TMQ).Close(logger)
		}
	})
}

func (ctl *Restful) tmq(c *gin.Context) {
	id := web.GetRequestID(c)
	loggerWithID := logger.WithField("sessionID", id).WithField("wsType", "tmq")
	_ = ctl.tmqM.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": loggerWithID})
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
		Timing:    getDuration(ctx),
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

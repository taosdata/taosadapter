package tmq

import (
	"bytes"
	"container/list"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/taosdata/driver-go/v3/wrapper"
)

type TopicVGroup struct {
	messages          *topicVGroupMessages
	idx               *topicVGroupIdx
	autoClean         bool
	autoCleanInterval time.Duration
	messageTimeout    time.Duration
	autoCleanStarted  bool
	sync.RWMutex
}

type topicVGroup struct {
	topic    string
	vGroupID uint32
}

type Message struct {
	Index    uint64
	Topic    string
	VGroupID uint32
	Offset   uint64
	Type     int32
	CPointer unsafe.Pointer // message pointer from taosc
	buffer   *bytes.Buffer
	timeout  time.Time // message timeout
}

func (m *Message) setMessageID(messageID uint64) {
	m.Index = messageID
}

func (m *Message) MessageID() uint64 {
	return m.Index
}

type TopicVGroupOpt func(group *TopicVGroup)

func WithAutoClean() TopicVGroupOpt {
	return func(group *TopicVGroup) {
		group.autoClean = true
	}
}

func WithCleanInterval(interval int64) TopicVGroupOpt {
	return func(group *TopicVGroup) {
		group.autoCleanInterval = time.Duration(interval) * time.Second
	}
}

func WithTimeout(timeout int64) TopicVGroupOpt {
	return func(group *TopicVGroup) {
		group.messageTimeout = time.Duration(timeout) * time.Second
	}
}

func NewTopicVGroup(opts ...TopicVGroupOpt) *TopicVGroup {
	tg := TopicVGroup{
		messages:          newTopicVGroupMessages(),
		idx:               newTopicVGroupIdx(),
		autoCleanInterval: time.Second,
		messageTimeout:    time.Second,
	}
	for _, opt := range opts {
		opt(&tg)
	}
	if tg.autoClean {
		tg.startAutoClean()
	}
	return &tg
}

func (tg *TopicVGroup) CreateMessage(topic string, vGroupID uint32, offset uint64, t int32, message unsafe.Pointer) *Message {
	return &Message{Index: tg.getIndex(topic, vGroupID), Topic: topic, VGroupID: vGroupID, Offset: offset, Type: t, CPointer: message}
}

// StopAutoClean can only stop once
func (tg *TopicVGroup) StopAutoClean() {
	tg.Lock()
	defer tg.Unlock()

	if !tg.autoCleanStarted {
		return
	}

	tg.autoCleanStarted = false
}

func (tg *TopicVGroup) SetMessageTimeout(timeout int64) {
	tg.Lock()
	defer tg.Unlock()
	tg.messageTimeout = time.Duration(timeout) * time.Second
}

func (tg *TopicVGroup) AddMessage(message *Message) {
	topic, vGroupID := message.Topic, message.VGroupID
	messages, ok := tg.getMessages(topic, vGroupID)
	if !ok {
		messages = tg.initMessages(topic, vGroupID)
	}
	message.setMessageID(tg.getIndex(topic, vGroupID))
	if tg.autoClean {
		message.timeout = time.Now().Add(tg.messageTimeout)
	}

	messages.append(message)
}

func (tg *TopicVGroup) getMessages(topic string, vGroupID uint32) (*messageList, bool) {
	return tg.messages.getMessageList(topic, vGroupID)
}

func (tg *TopicVGroup) initMessages(topic string, vGroupID uint32) *messageList {
	return tg.messages.initMessageList(topic, vGroupID)
}

var UnKnownMessageID = errors.New("unknown message id")

func (tg *TopicVGroup) GetByMessageID(messageID uint64) (message *Message, err error) {
	topic, vGroupID, ok := tg.getTopicAndVGroup(messageID)
	if !ok {
		return nil, UnKnownMessageID
	}
	messages, _ := tg.getMessages(topic, vGroupID)
	return messages.getByIdx(messageID)
}

func (tg *TopicVGroup) GetByOffset(topic string, vGroupID uint32, offset uint64) (*Message, error) {
	messages, ok := tg.getMessages(topic, vGroupID)
	if !ok {
		return nil, NotFountError
	}

	return messages.getByOffset(offset)
}

func (tg *TopicVGroup) CleanByOffset(topic string, vGroupID uint32, offset uint64) {
	messages, ok := tg.getMessages(topic, vGroupID)
	if !ok {
		return
	}
	messages.cleanByOffset(offset)
}

func (tg *TopicVGroup) CleanAll() {
	all := tg.getAllMessageList()
	for _, messages := range all {
		messages.cleanAll()
	}
}

func (tg *TopicVGroup) startAutoClean() {
	tg.Lock()
	defer tg.Unlock()

	if tg.autoCleanStarted {
		return
	}

	tg.autoCleanStarted = true
	go func() {
		timer := time.NewTimer(tg.autoCleanInterval)
		defer func() {
			timer.Stop()
		}()

		for range timer.C {
			all := tg.getAllMessageList()
			for _, messages := range all {
				messages.cleanTimeoutMessages()
			}
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(tg.autoCleanInterval)

			if !tg.autoCleanStarted {
				break
			}
		}
	}()
}

// getIndex to get the message id. message id is 64bit(uint64). top 10 bit refer to topic/vGroupID, other is atomic int
func (tg *TopicVGroup) getIndex(topic string, vGroupID uint32) uint64 {
	return tg.idx.messageId(topic, vGroupID)
}

func (tg *TopicVGroup) getAllMessageList() []*messageList {
	return tg.messages.getAllMessageList()
}

func (tg *TopicVGroup) getTopicAndVGroup(messageID uint64) (topic string, vGroupID uint32, ok bool) {
	return tg.idx.getTopicAndVGroup(messageID)
}

type topicVGroupMessages struct {
	messages map[topicVGroup]*messageList // key is topic and vGroupID, value is messageList
	sync.RWMutex
}

func newTopicVGroupMessages() *topicVGroupMessages {
	return &topicVGroupMessages{messages: make(map[topicVGroup]*messageList, 64)}
}

func (tm *topicVGroupMessages) initMessageList(topic string, vGroupID uint32) *messageList {
	tm.Lock()
	defer tm.Unlock()

	tg := topicVGroup{topic: topic, vGroupID: vGroupID}
	messages, ok := tm.messages[tg]
	if ok {
		return messages
	}

	messages = newMessageList()
	tm.messages[tg] = messages

	return messages
}

func (tm *topicVGroupMessages) getMessageList(topic string, vGroupID uint32) (*messageList, bool) {
	tm.RLock()
	defer tm.RUnlock()
	messages, ok := tm.messages[topicVGroup{topic: topic, vGroupID: vGroupID}]
	return messages, ok
}

func (tm *topicVGroupMessages) getAllMessageList() (messages []*messageList) {
	tm.RLock()
	defer tm.RUnlock()

	messages = make([]*messageList, 0, len(tm.messages))
	for _, message := range tm.messages {
		messages = append(messages, message)
	}
	return
}

type messageList struct {
	messages *list.List // double link list, value is OffsetNode, order by OffsetNode.Offset
	sync.RWMutex
}

func newMessageList() *messageList {
	return &messageList{messages: list.New()}
}

var NotFountError = errors.New("not Found")

func (n *messageList) getByOffset(offset uint64) (*Message, error) {
	n.RLock()
	defer n.RUnlock()

	node := n.messages.Front()
	for {
		if node == nil {
			break
		}
		message := node.Value.(*Message)
		if message.Offset == offset {
			return message, nil
		}
		node = node.Next()
	}

	return nil, NotFountError
}

func (n *messageList) getByIdx(idx uint64) (*Message, error) {
	n.RLock()
	defer n.RUnlock()

	node := n.messages.Front()
	for {
		if node == nil {
			break
		}
		message := node.Value.(*Message)
		if message.Index == idx {
			return message, nil
		}
		node = node.Next()
	}

	return nil, NotFountError
}

func (n *messageList) append(message *Message) {
	n.Lock()
	defer n.Unlock()

	n.messages.PushBack(message)
}

func (n *messageList) cleanByOffset(offset uint64) {
	n.Lock()
	defer n.Unlock()

	// list from head and clean the node of offset lte than offset
	node := n.messages.Front()
	for {
		if node == nil {
			break
		}
		next := node.Next()
		offsetNode := node.Value.(*Message)

		if offsetNode.Offset > offset {
			break
		}

		// clean C pointer
		n.freeCPointer(offsetNode.CPointer)
		// remove node
		n.messages.Remove(node)

		node = next
	}
	return
}

func (n *messageList) cleanTimeoutMessages() {
	n.Lock()
	defer n.Unlock()

	if n.messages.Len() == 0 {
		return
	}

	node := n.messages.Front()
	for {
		if node == nil {
			break
		}

		next := node.Next()
		offsetNode := node.Value.(*Message)

		if time.Now().Before(offsetNode.timeout) {
			break
		}

		// clean C pointer
		n.freeCPointer(offsetNode.CPointer)
		// remove node
		n.messages.Remove(node)

		node = next
	}
	return
}

func (n *messageList) cleanAll() {
	n.Lock()
	defer n.Unlock()

	node := n.messages.Front()
	for {
		if node == nil {
			break
		}
		next := node.Next()
		messageNode := node.Value.(*Message)
		n.freeCPointer(messageNode.CPointer)
		n.messages.Remove(node)

		node = next
	}

	return
}

func (n *messageList) getAll() (messages []*Message) {
	n.RLock()
	defer n.RUnlock()

	if n.messages.Len() == 0 {
		return nil
	}

	node := n.messages.Front()
	for {
		if node == nil {
			break
		}

		messages = append(messages, node.Value.(*Message))
		node = node.Next()
	}

	return
}

func (n *messageList) freeCPointer(pointer unsafe.Pointer) {
	if pointer == nil {
		return
	}
	wrapper.TaosFreeResult(pointer)
	pointer = nil
}

// topicVGroupIdx key is message id top 16 bit, value is topicVGroup
type topicVGroupIdx struct {
	topicVGroupIdx   map[topicVGroup]uint32
	idxTopicVGroups  map[uint32]topicVGroup
	messageIndex     uint64
	topicVGroupIndex uint32
	sync.RWMutex
}

func newTopicVGroupIdx() *topicVGroupIdx {
	return &topicVGroupIdx{
		topicVGroupIdx:  make(map[topicVGroup]uint32, 64),
		idxTopicVGroups: make(map[uint32]topicVGroup, 64),
	}
}

const maxMessageIndex = math.MaxUint64 >> 16 // top 16 is 0 and other is 1

func (tv *topicVGroupIdx) addTopicAndVGroup(topic string, vGroupID uint32) uint64 {
	tv.Lock()
	defer tv.Unlock()

	tg := topicVGroup{topic: topic, vGroupID: vGroupID}
	if idx, ok := tv.topicVGroupIdx[tg]; ok {
		return uint64(idx)
	}

	idx := atomic.AddUint32(&tv.topicVGroupIndex, 1)
	tv.topicVGroupIdx[tg] = idx
	tv.idxTopicVGroups[idx] = tg
	return uint64(idx)
}

func (tv *topicVGroupIdx) getTopicAndVGroup(id uint64) (topic string, vGroupID uint32, exists bool) {
	tv.RLock()
	defer tv.RUnlock()

	tg, ok := tv.idxTopicVGroups[tv.getIdxByMessageID(id)]
	return tg.topic, tg.vGroupID, ok
}

func (tv *topicVGroupIdx) getIdxByMessageID(id uint64) uint32 {
	return uint32(id >> 48) // only 16 bit, so uint32 is ok
}

func (tv *topicVGroupIdx) getTopicVGroupIdx(topic string, vGroupID uint32) (uint64, bool) {
	tg := topicVGroup{topic: topic, vGroupID: vGroupID}
	tv.RLock()
	defer tv.RUnlock()
	id, ok := tv.topicVGroupIdx[tg]
	return uint64(id), ok
}

func (tv *topicVGroupIdx) messageId(topic string, vGroupID uint32) uint64 {
	tgIdx, ok := tv.getTopicVGroupIdx(topic, vGroupID)
	if !ok {
		tgIdx = tv.addTopicAndVGroup(topic, vGroupID)
	}

	if tv.messageIndex+1 >= maxMessageIndex {
		tv.Lock()
		if tv.messageIndex+1 >= maxMessageIndex {
			tv.messageIndex = 1
		}
		tv.Unlock()
	}
	messageIdx := atomic.AddUint64(&tv.messageIndex, 1)
	return (tgIdx << 48) | messageIdx
}

package tmq

import (
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestTopicVGroup_AddMessage(t *testing.T) {
	tg := NewTopicVGroup()

	msg1 := tg.CreateMessage("topic_1", 1, 1, 0, nil)
	msg2 := tg.CreateMessage("topic_1", 1, 3, 0, nil)
	msg3 := tg.CreateMessage("topic_1", 2, 4, 0, nil)
	msg4 := tg.CreateMessage("topic_1", 2, 6, 0, nil)
	msg5 := tg.CreateMessage("topic_2", 1, 2, 0, nil)
	msg6 := tg.CreateMessage("topic_2", 2, 4, 0, nil)

	tg.AddMessage(msg1)
	tg.AddMessage(msg2)
	tg.AddMessage(msg3)
	tg.AddMessage(msg4)
	tg.AddMessage(msg5)
	tg.AddMessage(msg6)

	Messages1, ok := tg.getMessages("topic_1", 1)
	assert.True(t, ok)
	assert.Equal(t, 2, Messages1.messages.Len())
	message1All := Messages1.getAll()
	assert.Equal(t, unsafe.Pointer(message1All[0]), unsafe.Pointer(msg1))
	assert.Equal(t, unsafe.Pointer(message1All[1]), unsafe.Pointer(msg2))

	message2, ok := tg.getMessages("topic_1", 1)
	assert.True(t, ok)
	assert.Equal(t, 2, message2.messages.Len())
	message2All := message2.getAll()
	assert.Equal(t, unsafe.Pointer(message2All[0]), unsafe.Pointer(msg1))
	assert.Equal(t, unsafe.Pointer(message2All[1]), unsafe.Pointer(msg2))
}

func TestTopicVGroup_CleanByOffset(t *testing.T) {
	tg := NewTopicVGroup()
	msg1 := tg.CreateMessage("topic_1", 1, 1, 0, nil)
	msg2 := tg.CreateMessage("topic_1", 1, 3, 0, nil)
	msg3 := tg.CreateMessage("topic_1", 1, 4, 0, nil)
	msg4 := tg.CreateMessage("topic_1", 1, 6, 0, nil)
	msg5 := tg.CreateMessage("topic_1", 1, 8, 0, nil)
	msg6 := tg.CreateMessage("topic_1", 1, 11, 0, nil)

	tg.AddMessage(msg1)
	tg.AddMessage(msg2)
	tg.AddMessage(msg3)
	tg.AddMessage(msg4)
	tg.AddMessage(msg5)
	tg.AddMessage(msg6)

	tg.CleanByOffset("topic_1", 1, 6)
	messages, ok := tg.getMessages("topic_1", 1)
	allMessages := messages.getAll()

	assert.True(t, ok)
	assert.Equal(t, 2, messages.messages.Len())
	assert.Equal(t, uint64(8), allMessages[0].Offset)
	assert.Equal(t, uint64(11), allMessages[1].Offset)
}

func TestTopicVGroup_WrongMessageID(t *testing.T) {
	tg := NewTopicVGroup()
	msg := tg.CreateMessage("topic_1", 1, 1, 0, unsafe.Pointer(&struct{}{}))
	tg.AddMessage(msg)

	_, err := tg.GetByMessageID(msg.MessageID() + 123)
	assert.Error(t, err)

}

func TestTopicVGroup_AutoClean(t *testing.T) {
	tg := NewTopicVGroup(WithAutoClean(), WithCleanInterval(1), WithTimeout(1))

	msg1 := tg.CreateMessage("topic_1", 1, 1, 0, nil)
	msg2 := tg.CreateMessage("topic_1", 1, 3, 0, nil)
	msg3 := tg.CreateMessage("topic_1", 1, 4, 0, nil)
	msg4 := tg.CreateMessage("topic_1", 1, 6, 0, nil)
	msg5 := tg.CreateMessage("topic_2", 1, 8, 0, nil)
	msg6 := tg.CreateMessage("topic_2", 1, 11, 0, nil)

	tg.AddMessage(msg1)
	tg.AddMessage(msg2)
	tg.AddMessage(msg3)
	tg.AddMessage(msg4)
	tg.AddMessage(msg5)
	tg.AddMessage(msg6)

	time.Sleep(2 * time.Second)

	messages, ok := tg.getMessages("topic_1", 1)
	allMessages := messages.getAll()
	assert.True(t, ok)
	assert.Equal(t, 0, len(allMessages))
}

func TestTopicVGroup_InGroutine(t *testing.T) {
	tg := NewTopicVGroup()

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()

		msg1 := tg.CreateMessage("topic_1", 1, 1, 0, nil)
		msg2 := tg.CreateMessage("topic_1", 1, 3, 0, nil)
		tg.AddMessage(msg1)
		tg.AddMessage(msg2)

		tg.CleanByOffset(msg2.Topic, msg2.VGroupID, msg2.Offset)
	}()

	go func() {
		defer wg.Done()

		msg1 := tg.CreateMessage("topic_1", 1, 4, 0, nil)
		msg2 := tg.CreateMessage("topic_1", 1, 6, 0, nil)
		tg.AddMessage(msg1)
		tg.AddMessage(msg2)

		tg.CleanByOffset(msg2.Topic, msg2.VGroupID, msg2.Offset)
	}()

	go func() {
		defer wg.Done()

		msg1 := tg.CreateMessage("topic_1", 1, 5, 0, nil)
		msg2 := tg.CreateMessage("topic_1", 1, 7, 0, nil)
		tg.AddMessage(msg1)
		tg.AddMessage(msg2)

		tg.CleanByOffset(msg2.Topic, msg2.VGroupID, msg2.Offset)
	}()

	wg.Wait()

	messages, ok := tg.getMessages("topic_1", 1)
	allMessages := messages.getAll()
	assert.True(t, ok)
	assert.Equal(t, 0, len(allMessages))
}

func TestMessage_MessageID(t *testing.T) {
	idx := newTopicVGroupIdx()
	idx.addTopicAndVGroup("topic_1", 1)
	idx.addTopicAndVGroup("topic_2", 2)
	idx.addTopicAndVGroup("topic_2", 2)

	id := idx.messageId("topic_1", 1)
	assert.Equal(t, uint32(1), idx.getIdxByMessageID(id))
	topic, vgID, ok := idx.getTopicAndVGroup(id)
	assert.Equal(t, "topic_1", topic)
	assert.Equal(t, uint32(1), vgID)
	assert.True(t, ok)
	id = idx.messageId("topic_2", 2)
	assert.Equal(t, uint32(2), idx.getIdxByMessageID(id))
}

func BenchmarkTopicVGroup_AddMessage(b *testing.B) {
	b.ResetTimer()

	tg := NewTopicVGroup()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			msg := tg.CreateMessage("topic_1", uint32(i), uint64(j), 0, nil)
			tg.AddMessage(msg)
		}
		tg.CleanByOffset("topic_1", uint32(i), 100000)
	}
}

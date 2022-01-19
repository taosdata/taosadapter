package jsonbuilder

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ConfigDefault = &JsonConfig{indentionStep: 0}

func Test_writeByte_should_grow_buffer(t *testing.T) {
	should := require.New(t)
	stream := NewStream(ConfigDefault, nil, 1)
	stream.writeByte('1')
	should.Equal("1", string(stream.Buffer()))
	should.Equal(1, len(stream.buf))
	stream.writeByte('2')
	should.Equal("12", string(stream.Buffer()))
	should.Equal(2, len(stream.buf))
	stream.writeThreeBytes('3', '4', '5')
	should.Equal("12345", string(stream.Buffer()))
}

func Test_writeBytes_should_grow_buffer(t *testing.T) {
	should := require.New(t)
	stream := NewStream(ConfigDefault, nil, 1)
	stream.Write([]byte{'1', '2'})
	should.Equal("12", string(stream.Buffer()))
	should.Equal(2, len(stream.buf))
	stream.Write([]byte{'3', '4', '5', '6', '7'})
	should.Equal("1234567", string(stream.Buffer()))
	should.Equal(7, len(stream.buf))
}

func Test_writeRaw_should_grow_buffer(t *testing.T) {
	should := require.New(t)
	stream := NewStream(ConfigDefault, nil, 1)
	stream.WriteRaw("123")
	should.Nil(stream.Error)
	should.Equal("123", string(stream.Buffer()))
}

func Test_writeString_should_grow_buffer(t *testing.T) {
	should := require.New(t)
	stream := NewStream(ConfigDefault, nil, 0)
	stream.WriteString("123")
	should.Nil(stream.Error)
	should.Equal(`"123"`, string(stream.Buffer()))
}

type NopWriter struct {
	bufferSize int
}

func (w *NopWriter) Write(p []byte) (n int, err error) {
	w.bufferSize = cap(p)
	return len(p), nil
}

func Test_flush_buffer_should_stop_grow_buffer(t *testing.T) {
	// Stream an array of a zillion zeros.
	writer := new(NopWriter)
	stream := NewStream(ConfigDefault, writer, 512)
	stream.WriteArrayStart()
	for i := 0; i < 10000000; i++ {
		stream.WriteInt(0)
		stream.WriteMore()
		stream.Flush()
	}
	stream.WriteInt(0)
	stream.WriteArrayEnd()

	// Confirm that the buffer didn't have to grow.
	should := require.New(t)

	// 512 is the internal buffer size set in NewEncoder
	//
	// Flush is called after each array element, so only the first 8 bytes of it
	// is ever used, and it is never extended. Capacity remains 512.
	should.Equal(512, writer.bufferSize)
}

func TestStream_Common(t *testing.T) {
	writer := new(NopWriter)
	writer2 := new(NopWriter)
	stream := NewStream(ConfigDefault, writer, 512)
	assert.Equal(t, 512, stream.Available())
	assert.Equal(t, 0, stream.Buffered())
	stream.SetBuffer(make([]byte, 0, 512))
	stream.Reset(writer2)
	stream.Write([]byte{1})
	stream.WriteArrayStart()
	stream.WriteByte(1)
	stream.Flush()
	stream.WriteNil()
	stream.WriteTrue()
	stream.WriteFalse()
	stream.WriteBool(true)
	stream.WriteBool(false)
	stream.WriteObjectStart()
	stream.WriteObjectField("a")
	stream.WriteObjectEnd()
	stream.WriteEmptyObject()
	stream.WriteEmptyArray()
	stream.WriteFloat32(1)
	stream.WriteFloat32(float32(math.Inf(1)))
	stream.WriteFloat32(float32(math.NaN()))
	stream.WriteFloat32(math.MaxFloat32)
	stream.WriteFloat32Lossy(1)
	stream.WriteFloat32Lossy(float32(math.Inf(1)))
	stream.WriteFloat32Lossy(float32(math.NaN()))
	stream.WriteFloat32Lossy(math.MaxFloat32)
	stream.WriteFloat64(1)
	stream.WriteFloat64(math.Inf(1))
	stream.WriteFloat64(math.MaxFloat64)
	stream.WriteFloat64Lossy(1)
	stream.WriteFloat64Lossy(math.Inf(1))
	stream.WriteFloat64Lossy(math.MaxFloat64)
	stream.WriteInt8(1)
	stream.WriteInt16(1)
	stream.WriteInt32(1)
	stream.WriteInt64(1)
	stream.WriteInt(1)
	stream.WriteInt8(-1)
	stream.WriteInt16(-1)
	stream.WriteInt32(-1)
	stream.WriteInt64(-1)
	stream.WriteInt(-1)
	stream.WriteUint8(1)
	stream.WriteUint16(1)
	stream.WriteUint32(1)
	stream.WriteUint64(1)
	stream.WriteUint(1)
	stream.WriteUint8(math.MaxUint8)
	stream.WriteUint16(math.MaxUint16)
	stream.WriteUint32(math.MaxUint32)
	stream.WriteUint64(math.MaxUint64)
	stream.WriteUint(math.MaxUint64)
	stream.WriteStringByte('"')
	stream.WriteStringByte('/')
	stream.WriteStringByte('a')
	stream.WriteStringByte('\n')
	stream.WriteStringByte('\r')
	stream.WriteStringByte('\t')
	stream.WriteString("\r\n\t/")
}

func TestStr(t *testing.T) {
	b := &strings.Builder{}
	stream := BorrowStream(b)
	stream.WriteString("a\nb")
	stream.Flush()
	assert.Equal(t, "\"a\\nb\"", b.String())
}

func TestStrByte(t *testing.T) {
	b := &strings.Builder{}
	stream := BorrowStream(b)
	stream.WriteStringByte('a')
	stream.WriteStringByte('\n')
	stream.WriteStringByte('b')
	stream.Flush()
	assert.Equal(t, "a\\nb", b.String())
}

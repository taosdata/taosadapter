// Copyright (c) 2022 TAOS Data, Inc.
//
// SPDX-License-Identifier: MIT
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package melody

import (
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

const (
	StatusNormal = uint32(1)
	StatusStop   = uint32(2)
)

var (
	ErrSessionClosed = errors.New("session is closed")
	ErrWriteClosed   = errors.New("tried to write to a closed session")
)

// Session wrapper around websocket connections.
type Session struct {
	Request      *http.Request
	Keys         sync.Map
	conn         *websocket.Conn
	output       chan *envelope
	melody       *Melody
	status       uint32
	closeOnce    sync.Once
	lastReadTime time.Time
}

func (s *Session) writeMessage(message *envelope) {
	if s.closed() {
		s.melody.errorHandler(s, ErrWriteClosed)
		return
	}
	defer func() {
		if recover() != nil {
			s.melody.errorHandler(s, ErrWriteClosed)
		}
	}()
	s.output <- message
}

func (s *Session) writeRaw(message *envelope) error {
	if s.closed() {
		return ErrWriteClosed
	}

	// no error returned from SetWriteDeadline
	_ = s.conn.SetWriteDeadline(time.Now().Add(s.melody.Config.WriteWait))

	err := s.conn.WriteMessage(message.t, message.msg)

	if err != nil {
		return err
	}

	return nil
}

func (s *Session) closed() bool {
	return atomic.LoadUint32(&s.status) == StatusStop
}

func (s *Session) close() {
	s.closeOnce.Do(func() {
		atomic.StoreUint32(&s.status, StatusStop)
		_ = s.conn.Close()
		close(s.output)
	})
}

func (s *Session) writePump() {
	ticker := time.NewTicker(s.melody.Config.PingPeriod)
	defer ticker.Stop()
	for {
		select {
		case msg, ok := <-s.output:
			if !ok {
				// The channel has been closed, this means the session is closed, return to stop the writePump
				return
			}

			err := s.writeRaw(msg)

			if err != nil {
				s.melody.errorHandler(s, err)
				return
			}

			if msg.t == websocket.CloseMessage {
				return
			}
		case <-ticker.C:
			_ = s.writeRaw(&envelope{t: websocket.PingMessage, msg: []byte{}})
		}
	}
}

func (s *Session) readPump() {
	s.conn.SetReadLimit(s.melody.Config.MaxMessageSize)
	s.setReadDeadline()

	s.conn.SetPongHandler(func(string) error {
		s.setReadDeadline()
		s.melody.pongHandler(s)
		return nil
	})

	if s.melody.closeHandler != nil {
		s.conn.SetCloseHandler(func(code int, text string) error {
			return s.melody.closeHandler(s, code, text)
		})
	}

	for {
		t, message, err := s.conn.ReadMessage()

		if err != nil {
			s.melody.errorHandler(s, err)
			break
		}
		s.setReadDeadline()
		if t == websocket.TextMessage {
			s.melody.messageHandler(s, message)
		}

		if t == websocket.BinaryMessage {
			s.melody.messageHandlerBinary(s, message)
		}
	}
}

func (s *Session) setReadDeadline() {
	now := time.Now()
	if now.Sub(s.lastReadTime) >= time.Second {
		s.lastReadTime = now
		err := s.conn.SetReadDeadline(s.lastReadTime.Add(s.melody.Config.PongWait + s.melody.Config.PingPeriod))
		if err != nil {
			if logger, exists := s.Get("logger"); exists {
				logger.(*logrus.Entry).Errorf("setReadDeadline error: %v", err)
			}
		}
	}
}

// Write writes message to session.
func (s *Session) Write(msg []byte) error {
	if s.closed() {
		return ErrSessionClosed
	}

	s.writeMessage(&envelope{t: websocket.TextMessage, msg: msg})

	return nil
}

// WriteBinary writes a binary message to session.
func (s *Session) WriteBinary(msg []byte) error {
	if s.closed() {
		return ErrSessionClosed
	}

	s.writeMessage(&envelope{t: websocket.BinaryMessage, msg: msg})

	return nil
}

// Close closes session.
func (s *Session) Close() error {
	if s.closed() {
		return ErrSessionClosed
	}

	s.writeMessage(&envelope{t: websocket.CloseMessage, msg: []byte{}})

	return nil
}

// Set is used to store a new key/value pair exclusivelly for this session.
// It also lazy initializes s.Keys if it was not used previously.
func (s *Session) Set(key string, value interface{}) {
	s.Keys.Store(key, value)
}

// Get returns the value for the given key, ie: (value, true).
// If the value does not exists it returns (nil, false)
func (s *Session) Get(key string) (value interface{}, exists bool) {
	return s.Keys.Load(key)
}

// MustGet returns the value for the given key if it exists, otherwise it panics.
func (s *Session) MustGet(key string) interface{} {
	if value, exists := s.Get(key); exists {
		return value
	}

	panic("Key \"" + key + "\" does not exist")
}

// UnSet will delete the key and has no return value
func (s *Session) UnSet(key string) {
	s.Keys.Delete(key)
}

// IsClosed returns the status of the connection.
func (s *Session) IsClosed() bool {
	return s.closed()
}

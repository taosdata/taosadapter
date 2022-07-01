package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/tools/parseblock"
)

func TestTMQ(t *testing.T) {
	ts1 := time.Now()
	ts2 := ts1.Add(time.Second)
	ts3 := ts2.Add(time.Second)
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists test_ws_tmq")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct0 (ts timestamp, c1 int)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct1 (ts timestamp, c1 int, c2 float)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct2 (ts timestamp, c1 int, c2 float, c3 binary(10))")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create topic if not exists test_tmq_ws_topic as DATABASE test_ws_tmq")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct0 values('%s',1)`, ts1.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct1 values('%s',1,2)`, ts2.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct2 values('%s',1,2,'3')`, ts3.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer ws.Close()
	const (
		AfterTMQSubscribe = iota + 1
		AfterTMQPoll
		AfterTMQFetch
		AfterTMQFetchBlock
		AfterTMQCommit
		AfterVersion
	)
	messageID := uint64(0)
	status := 0
	finish := make(chan struct{})
	var tmpFetchResp *TMQFetchResp
	pollCount := 0
	testMessageHandler := func(messageType int, message []byte) error {
		if messageType == websocket.BinaryMessage {
			t.Log(messageType, message)
		} else {
			t.Log(messageType, string(message))
		}
		switch status {
		case AfterTMQSubscribe:
			var d TMQSubscribeResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", TMQSubscribe, d.Code, d.Message)
			}
			status = AfterTMQPoll
			b, _ := json.Marshal(&TMQPollReq{
				ReqID:        3,
				BlockingTime: 500,
			})
			action, _ := json.Marshal(&WSAction{
				Action: TMQPoll,
				Args:   b,
			})
			t.Log(string(action))
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterTMQPoll:
			if pollCount == 5 {
				status = AfterVersion
				action, _ := json.Marshal(&WSAction{
					Action: ClientVersion,
					Args:   nil,
				})
				err = ws.WriteMessage(
					websocket.TextMessage,
					action,
				)
				return nil
			}
			pollCount += 1
			var d TMQPollResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", TMQPoll, d.Code, d.Message)
			}
			if d.HaveMessage {
				messageID = d.MessageID
				status = AfterTMQFetch
				b, _ := json.Marshal(&TMQFetchReq{
					ReqID:     4,
					MessageID: messageID,
				})
				action, _ := json.Marshal(&WSAction{
					Action: TMQFetch,
					Args:   b,
				})
				t.Log(string(action))
				err = ws.WriteMessage(
					websocket.TextMessage,
					action,
				)
				if err != nil {
					return err
				}
			} else {
				status = AfterTMQPoll
				//fetch
				b, _ := json.Marshal(&TMQPollReq{
					ReqID:        3,
					BlockingTime: 500,
				})
				action, _ := json.Marshal(&WSAction{
					Action: TMQPoll,
					Args:   b,
				})
				t.Log(string(action))
				err = ws.WriteMessage(
					websocket.TextMessage,
					action,
				)
				if err != nil {
					return err
				}
			}
		case AfterTMQFetch:
			var d TMQFetchResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", TMQFetch, d.Code, d.Message)
			}

			if d.Completed {
				status = AfterTMQCommit
				b, _ := json.Marshal(&TMQCommitReq{
					ReqID:     3,
					MessageID: messageID,
				})
				action, _ := json.Marshal(&WSAction{
					Action: TMQCommit,
					Args:   b,
				})
				t.Log(string(action))
				err = ws.WriteMessage(
					websocket.TextMessage,
					action,
				)
				if err != nil {
					return err
				}
			} else {
				tmpFetchResp = &d
				status = AfterTMQFetchBlock
				b, _ := json.Marshal(&TMQFetchBlockReq{
					ReqID:     0,
					MessageID: messageID,
				})
				action, _ := json.Marshal(&WSAction{
					Action: TMQFetchBlock,
					Args:   b,
				})
				t.Log(string(action))
				err = ws.WriteMessage(
					websocket.TextMessage,
					action,
				)
				if err != nil {
					return err
				}
			}
		case AfterTMQFetchBlock:
			_, _, value := parseblock.ParseTmqBlock(message, tmpFetchResp.FieldsTypes, tmpFetchResp.Rows, tmpFetchResp.Precision)
			switch tmpFetchResp.TableName {
			case "ct0":
				assert.Equal(t, 1, len(value))
				assert.Equal(t, ts1.UnixNano()/1e6, value[0][0].(time.Time).UnixNano()/1e6)
				assert.Equal(t, int32(1), value[0][1])
			case "ct1":
				assert.Equal(t, 1, len(value))
				assert.Equal(t, ts2.UnixNano()/1e6, value[0][0].(time.Time).UnixNano()/1e6)
				assert.Equal(t, int32(1), value[0][1])
				assert.Equal(t, float32(2), value[0][2])
			case "ct2":
				assert.Equal(t, 1, len(value))
				assert.Equal(t, ts3.UnixNano()/1e6, value[0][0].(time.Time).UnixNano()/1e6)
				assert.Equal(t, int32(1), value[0][1])
				assert.Equal(t, float32(2), value[0][2])
				assert.Equal(t, "3", value[0][3])
			}
			_ = value
			status = AfterTMQFetch
			b, _ := json.Marshal(&TMQFetchReq{
				ReqID:     4,
				MessageID: messageID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: TMQFetch,
				Args:   b,
			})
			t.Log(string(action))
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterTMQCommit:
			var d TMQFetchResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", TMQCommit, d.Code, d.Message)
			}
			status = AfterTMQPoll
			b, _ := json.Marshal(&TMQPollReq{
				ReqID:        3,
				BlockingTime: 500,
			})
			action, _ := json.Marshal(&WSAction{
				Action: TMQPoll,
				Args:   b,
			})
			t.Log(string(action))
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterVersion:
			var d WSVersionResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSFetch, d.Code, d.Message)
			}
			assert.NotEmpty(t, d.Version)
			t.Log("client version", d.Version)
			finish <- struct{}{}
			return nil
		}
		return nil
	}
	go func() {
		for {
			mt, message, err := ws.ReadMessage()
			if err != nil {
				if strings.Contains(err.Error(), "use of closed network connection") {
					finish <- struct{}{}
					return
				}
				t.Error(err)
				finish <- struct{}{}
				return
			}
			err = testMessageHandler(mt, message)
			if err != nil {
				if strings.Contains(err.Error(), "use of closed network connection") {
					finish <- struct{}{}
					return
				}
				if mt == websocket.BinaryMessage {
					t.Error(err, message)
				} else {
					t.Error(err, string(message))
				}
				finish <- struct{}{}
				return
			}
		}
	}()

	init := &TMQSubscribeReq{
		ReqID:    0,
		User:     "root",
		Password: "taosdata",
		GroupID:  "test",
		Topics:   []string{"test_tmq_ws_topic"},
	}

	b, _ := json.Marshal(init)
	action, _ := json.Marshal(&WSAction{
		Action: TMQSubscribe,
		Args:   b,
	})
	t.Log(string(action))
	status = AfterTMQSubscribe
	err = ws.WriteMessage(
		websocket.TextMessage,
		action,
	)
	if err != nil {
		t.Error(err)
		return
	}
	<-finish
	ws.Close()
	time.Sleep(time.Second * 3)
}

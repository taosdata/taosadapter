package tmq

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common/parser"
	"github.com/taosdata/driver-go/v3/common/tmq"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	_ "github.com/taosdata/taosadapter/v3/controller/rest"
	"github.com/taosdata/taosadapter/v3/controller/ws/query"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/parseblock"
)

var router *gin.Engine

func TestMain(m *testing.M) {
	viper.Set("pool.maxConnect", 10000)
	viper.Set("pool.maxIdle", 10000)
	viper.Set("logLevel", "trace")
	viper.Set("uploadKeeper.enable", false)
	config.Init()
	db.PrepareConnection()
	log.ConfigLog()
	gin.SetMode(gin.ReleaseMode)
	router = gin.New()
	controllers := controller.GetControllers()
	for _, webController := range controllers {
		webController.Init(router)
	}
	m.Run()
}

func TestTMQ(t *testing.T) {
	ts1 := time.Now()
	ts2 := ts1.Add(time.Second)
	ts3 := ts2.Add(time.Second)
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists test_ws_tmq WAL_RETENTION_PERIOD 86400")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	defer func() {
		w = httptest.NewRecorder()
		body = strings.NewReader("drop database if exists test_ws_tmq")
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.RemoteAddr = "127.0.0.1:33333"
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}()

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct0 (ts timestamp, c1 int)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct1 (ts timestamp, c1 int, c2 float)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct2 (ts timestamp, c1 int, c2 float, c3 binary(10))")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create topic if not exists test_tmq_ws_topic as DATABASE test_ws_tmq")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct0 values('%s',1)`, ts1.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct1 values('%s',1,2)`, ts2.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct2 values('%s',1,2,'3')`, ts3.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.RemoteAddr = "127.0.0.1:33333"
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
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
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
	var tmqFetchResp *TMQFetchResp
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
			action, _ := json.Marshal(&wstool.WSAction{
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
				action, _ := json.Marshal(&wstool.WSAction{
					Action: wstool.ClientVersion,
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
				action, _ := json.Marshal(&wstool.WSAction{
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
				action, _ := json.Marshal(&wstool.WSAction{
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
				action, _ := json.Marshal(&wstool.WSAction{
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
				tmqFetchResp = &d
				status = AfterTMQFetchBlock
				b, _ := json.Marshal(&TMQFetchBlockReq{
					ReqID:     0,
					MessageID: messageID,
				})
				action, _ := json.Marshal(&wstool.WSAction{
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
			_, _, value := parseblock.ParseTmqBlock(message[8:], tmqFetchResp.FieldsTypes, tmqFetchResp.Rows, tmqFetchResp.Precision)
			switch tmqFetchResp.TableName {
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
			action, _ := json.Marshal(&wstool.WSAction{
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
			action, _ := json.Marshal(&wstool.WSAction{
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
			var d wstool.WSVersionResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", TMQFetch, d.Code, d.Message)
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
				var closeErr *websocket.CloseError
				if errors.As(err, &closeErr) && closeErr.Code == websocket.CloseAbnormalClosure {
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
		ReqID:                0,
		User:                 "root",
		Password:             "taosdata",
		GroupID:              "test",
		Topics:               []string{"test_tmq_ws_topic"},
		AutoCommit:           "true",
		AutoCommitIntervalMS: "5000",
		SnapshotEnable:       "true",
		WithTableName:        "true",
		OffsetReset:          "earliest",
	}

	b, _ := json.Marshal(init)
	action, _ := json.Marshal(&wstool.WSAction{
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
	err = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.NoError(t, err)
	time.Sleep(time.Second * 5)

	w = httptest.NewRecorder()
	body = strings.NewReader("drop topic if exists test_tmq_ws_topic")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("drop database if exists test_ws_tmq")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

type MultiMeta struct {
	TmqMetaVersion string     `json:"tmq_meta_version"`
	Metas          []tmq.Meta `json:"metas"`
}

func TestMeta(t *testing.T) {
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists test_ws_tmq_meta WAL_RETENTION_PERIOD 86400")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create topic if not exists test_tmq_meta_ws_topic with meta as DATABASE test_ws_tmq_meta")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_meta", body)
	req.RemoteAddr = "127.0.0.1:33333"
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
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	const (
		AfterTMQSubscribe = iota + 1
		AfterTMQPoll
		AfterFetchRawMeta
		AfterFetchJsonMeta
		AfterTMQCommit
		AfterUnsubscribe
		AfterVersion
	)
	messageID := uint64(0)
	status := 0
	finish := make(chan struct{})
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
			w = httptest.NewRecorder()
			body = strings.NewReader("create table stb (ts timestamp," +
				"c1 bool," +
				"c2 tinyint," +
				"c3 smallint," +
				"c4 int," +
				"c5 bigint," +
				"c6 tinyint unsigned," +
				"c7 smallint unsigned," +
				"c8 int unsigned," +
				"c9 bigint unsigned," +
				"c10 float," +
				"c11 double," +
				"c12 binary(20)," +
				"c13 nchar(20)" +
				")" +
				"tags(tts timestamp," +
				"tc1 bool," +
				"tc2 tinyint," +
				"tc3 smallint," +
				"tc4 int," +
				"tc5 bigint," +
				"tc6 tinyint unsigned," +
				"tc7 smallint unsigned," +
				"tc8 int unsigned," +
				"tc9 bigint unsigned," +
				"tc10 float," +
				"tc11 double," +
				"tc12 binary(20)," +
				"tc13 nchar(20)" +
				")")
			req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_meta", body)
			req.RemoteAddr = "127.0.0.1:33333"
			req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
			router.ServeHTTP(w, req)
			assert.Equal(t, 200, w.Code)
			status = AfterTMQPoll
			b, _ := json.Marshal(&TMQPollReq{
				ReqID:        3,
				BlockingTime: 500,
			})
			action, _ := json.Marshal(&wstool.WSAction{
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
				status = AfterUnsubscribe
				b, _ := json.Marshal(&TMQUnsubscribeReq{
					ReqID: 6,
				})
				action, _ := json.Marshal(&wstool.WSAction{
					Action: TMQUnsubscribe,
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
				status = AfterFetchJsonMeta
				b, _ := json.Marshal(&TMQFetchJsonMetaReq{
					ReqID:     4,
					MessageID: messageID,
				})
				action, _ := json.Marshal(&wstool.WSAction{
					Action: TMQFetchJsonMeta,
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
				action, _ := json.Marshal(&wstool.WSAction{
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
		case AfterFetchJsonMeta:
			var d TMQFetchJsonMetaResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", TMQFetch, d.Code, d.Message)
			}
			t.Log(string(d.Data))
			valid := jsoniter.Valid(d.Data)
			assert.True(t, valid)
			var meta tmq.Meta
			err = jsoniter.Unmarshal(d.Data, &meta)
			if err != nil {
				var multiMeta MultiMeta
				err = jsoniter.Unmarshal(d.Data, &multiMeta)
				assert.NoError(t, err)
			}
			//t.Log(meta)
			status = AfterFetchRawMeta
			b, _ := json.Marshal(&TMQFetchRawReq{
				ReqID:     3,
				MessageID: messageID,
			})
			action, _ := json.Marshal(&wstool.WSAction{
				Action: TMQFetchRaw,
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
		case AfterFetchRawMeta:
			if messageType != websocket.BinaryMessage {
				t.Fatal(string(message))
			}
			writeRaw(t, message)
			w = httptest.NewRecorder()
			body = strings.NewReader("describe stb")
			req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_meta_target", body)
			req.RemoteAddr = "127.0.0.1:33333"
			req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
			router.ServeHTTP(w, req)
			assert.Equal(t, 200, w.Code)
			var resp wstool.TDEngineRestfulResp
			err = jsoniter.Unmarshal(w.Body.Bytes(), &resp)
			assert.NoError(t, err)
			expect := [][]driver.Value{
				{"ts", "TIMESTAMP", float64(8), ""},
				{"c1", "BOOL", float64(1), ""},
				{"c2", "TINYINT", float64(1), ""},
				{"c3", "SMALLINT", float64(2), ""},
				{"c4", "INT", float64(4), ""},
				{"c5", "BIGINT", float64(8), ""},
				{"c6", "TINYINT UNSIGNED", float64(1), ""},
				{"c7", "SMALLINT UNSIGNED", float64(2), ""},
				{"c8", "INT UNSIGNED", float64(4), ""},
				{"c9", "BIGINT UNSIGNED", float64(8), ""},
				{"c10", "FLOAT", float64(4), ""},
				{"c11", "DOUBLE", float64(8), ""},
				{"c12", "VARCHAR", float64(20), ""},
				{"c13", "NCHAR", float64(20), ""},
				{"tts", "TIMESTAMP", float64(8), "TAG"},
				{"tc1", "BOOL", float64(1), "TAG"},
				{"tc2", "TINYINT", float64(1), "TAG"},
				{"tc3", "SMALLINT", float64(2), "TAG"},
				{"tc4", "INT", float64(4), "TAG"},
				{"tc5", "BIGINT", float64(8), "TAG"},
				{"tc6", "TINYINT UNSIGNED", float64(1), "TAG"},
				{"tc7", "SMALLINT UNSIGNED", float64(2), "TAG"},
				{"tc8", "INT UNSIGNED", float64(4), "TAG"},
				{"tc9", "BIGINT UNSIGNED", float64(8), "TAG"},
				{"tc10", "FLOAT", float64(4), "TAG"},
				{"tc11", "DOUBLE", float64(8), "TAG"},
				{"tc12", "VARCHAR", float64(20), "TAG"},
				{"tc13", "NCHAR", float64(20), "TAG"},
			}
			for index, values := range expect {
				for i := 0; i < 4; i++ {
					assert.Equal(t, values[i], resp.Data[index][i])
				}
			}
			status = AfterTMQCommit
			b, _ := json.Marshal(&TMQCommitReq{
				ReqID:     3,
				MessageID: messageID,
			})
			action, _ := json.Marshal(&wstool.WSAction{
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
			action, _ := json.Marshal(&wstool.WSAction{
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
		case AfterUnsubscribe:
			var d TMQUnsubscribeResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", TMQUnsubscribe, d.Code, d.Message)
			}
			status = AfterVersion
			action, _ := json.Marshal(&wstool.WSAction{
				Action: wstool.ClientVersion,
				Args:   nil,
			})
			t.Log(string(action))
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
		case AfterVersion:
			var d wstool.WSVersionResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", TMQFetch, d.Code, d.Message)
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
				var closeErr *websocket.CloseError
				if errors.As(err, &closeErr) && closeErr.Code == websocket.CloseAbnormalClosure {
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
		ReqID:                0,
		User:                 "root",
		Password:             "taosdata",
		GroupID:              "test",
		Topics:               []string{"test_tmq_meta_ws_topic"},
		AutoCommit:           "true",
		AutoCommitIntervalMS: "5000",
		SnapshotEnable:       "true",
		WithTableName:        "true",
		OffsetReset:          "earliest",
		EnableBatchMeta:      "true",
		SessionTimeoutMS:     "12000",
		MaxPollIntervalMS:    "300000",
	}

	b, _ := json.Marshal(init)
	action, _ := json.Marshal(&wstool.WSAction{
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
	err = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.NoError(t, err)
	time.Sleep(time.Second * 5)
	w = httptest.NewRecorder()
	body = strings.NewReader("describe stb")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_meta_target", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	var resp wstool.TDEngineRestfulResp
	err = jsoniter.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	expect := [][]driver.Value{
		{"ts", "TIMESTAMP", float64(8), ""},
		{"c1", "BOOL", float64(1), ""},
		{"c2", "TINYINT", float64(1), ""},
		{"c3", "SMALLINT", float64(2), ""},
		{"c4", "INT", float64(4), ""},
		{"c5", "BIGINT", float64(8), ""},
		{"c6", "TINYINT UNSIGNED", float64(1), ""},
		{"c7", "SMALLINT UNSIGNED", float64(2), ""},
		{"c8", "INT UNSIGNED", float64(4), ""},
		{"c9", "BIGINT UNSIGNED", float64(8), ""},
		{"c10", "FLOAT", float64(4), ""},
		{"c11", "DOUBLE", float64(8), ""},
		{"c12", "VARCHAR", float64(20), ""},
		{"c13", "NCHAR", float64(20), ""},
		{"tts", "TIMESTAMP", float64(8), "TAG"},
		{"tc1", "BOOL", float64(1), "TAG"},
		{"tc2", "TINYINT", float64(1), "TAG"},
		{"tc3", "SMALLINT", float64(2), "TAG"},
		{"tc4", "INT", float64(4), "TAG"},
		{"tc5", "BIGINT", float64(8), "TAG"},
		{"tc6", "TINYINT UNSIGNED", float64(1), "TAG"},
		{"tc7", "SMALLINT UNSIGNED", float64(2), "TAG"},
		{"tc8", "INT UNSIGNED", float64(4), "TAG"},
		{"tc9", "BIGINT UNSIGNED", float64(8), "TAG"},
		{"tc10", "FLOAT", float64(4), "TAG"},
		{"tc11", "DOUBLE", float64(8), "TAG"},
		{"tc12", "VARCHAR", float64(20), "TAG"},
		{"tc13", "NCHAR", float64(20), "TAG"},
	}
	for index, values := range expect {
		for i := 0; i < 4; i++ {
			assert.Equal(t, values[i], resp.Data[index][i])
		}
	}
	time.Sleep(time.Second * 3)
	w = httptest.NewRecorder()
	body = strings.NewReader("drop topic if exists test_tmq_meta_ws_topic")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("drop database if exists test_ws_tmq_meta_target")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("drop database if exists test_ws_tmq_meta")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

func writeRaw(t *testing.T, rawData []byte) {
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists test_ws_tmq_meta_target WAL_RETENTION_PERIOD 86400")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	const (
		AfterConnect  = 1
		AfterWriteRaw = 2
	)

	status := 0
	//total := 0
	finish := make(chan struct{})
	//var jsonResult [][]interface{}
	testMessageHandler := func(_ int, message []byte) error {
		//json
		switch status {
		case AfterConnect:
			var d query.WSConnectResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", query.WSConnect, d.Code, d.Message)
			}
			//query
			status = AfterWriteRaw
			err = ws.WriteMessage(websocket.BinaryMessage, rawData[8:])
			if err != nil {
				return err
			}
		case AfterWriteRaw:
			var d query.WSWriteMetaResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", query.WSQuery, d.Code, d.Message)
			}
			finish <- struct{}{}
		}
		return nil
	}
	go func() {
		for {
			mt, message, err := ws.ReadMessage()
			if err != nil {
				if strings.Contains(err.Error(), "use of closed network connection") {
					return
				}
				t.Error(err)
				finish <- struct{}{}
				return
			}
			err = testMessageHandler(mt, message)
			if err != nil {
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

	connect := &query.WSConnectReq{
		ReqID:    0,
		User:     "root",
		Password: "taosdata",
		DB:       "test_ws_tmq_meta_target",
	}

	b, _ := json.Marshal(connect)
	action, _ := json.Marshal(&wstool.WSAction{
		Action: query.WSConnect,
		Args:   b,
	})
	status = AfterConnect
	err = ws.WriteMessage(
		websocket.TextMessage,
		action,
	)
	if err != nil {
		t.Error(err)
		return
	}
	<-finish
	err = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.NoError(t, err)
}

func TestTMQAutoCommit(t *testing.T) {
	ts1 := time.Now()
	ts2 := ts1.Add(time.Second)
	ts3 := ts2.Add(time.Second)
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists test_ws_tmq_auto_commit WAL_RETENTION_PERIOD 86400")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct0 (ts timestamp, c1 int)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_auto_commit", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct1 (ts timestamp, c1 int, c2 float)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_auto_commit", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct2 (ts timestamp, c1 int, c2 float, c3 binary(10))")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_auto_commit", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create topic if not exists test_tmq_ws_auto_commit_topic as DATABASE test_ws_tmq_auto_commit")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_auto_commit", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct0 values('%s',1)`, ts1.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_auto_commit", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct1 values('%s',1,2)`, ts2.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_auto_commit", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct2 values('%s',1,2,'3')`, ts3.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_auto_commit", body)
	req.RemoteAddr = "127.0.0.1:33333"
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
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	const (
		AfterTMQSubscribe = iota + 1
		AfterTMQPoll
		AfterTMQFetch
		AfterTMQFetchBlock
		AfterVersion
	)
	messageID := uint64(0)
	status := 0
	finish := make(chan struct{})
	var tmqFetchResp *TMQFetchResp
	pollCount := 0
	expectError := false
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
			action, _ := json.Marshal(&wstool.WSAction{
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
				action, _ := json.Marshal(&wstool.WSAction{
					Action: wstool.ClientVersion,
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
				action, _ := json.Marshal(&wstool.WSAction{
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
				action, _ := json.Marshal(&wstool.WSAction{
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
				if expectError {
					assert.Equal(t, d.Message, "message is nil")
					status = AfterVersion
					action, _ := json.Marshal(&wstool.WSAction{
						Action: wstool.ClientVersion,
						Args:   nil,
					})
					err = ws.WriteMessage(
						websocket.TextMessage,
						action,
					)
					return nil
				}
				return fmt.Errorf("%s %d,%s", TMQFetch, d.Code, d.Message)
			}

			if d.Completed {
				status = AfterTMQPoll
				b, _ := json.Marshal(&TMQPollReq{
					ReqID:        3,
					BlockingTime: 500,
				})
				action, _ := json.Marshal(&wstool.WSAction{
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
			} else {
				tmqFetchResp = &d
				status = AfterTMQFetchBlock
				b, _ := json.Marshal(&TMQFetchBlockReq{
					ReqID:     0,
					MessageID: messageID,
				})
				action, _ := json.Marshal(&wstool.WSAction{
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
			_, _, value := parseblock.ParseTmqBlock(message[8:], tmqFetchResp.FieldsTypes, tmqFetchResp.Rows, tmqFetchResp.Precision)
			switch tmqFetchResp.TableName {
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
			action, _ := json.Marshal(&wstool.WSAction{
				Action: TMQFetch,
				Args:   b,
			})
			t.Log(string(action))
			time.Sleep(3 * 500 * time.Millisecond)
			expectError = true
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterVersion:
			var d wstool.WSVersionResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", TMQFetch, d.Code, d.Message)
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
				var closeErr *websocket.CloseError
				if errors.As(err, &closeErr) && closeErr.Code == websocket.CloseAbnormalClosure {
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
		ReqID:                0,
		User:                 "root",
		Password:             "taosdata",
		GroupID:              "test",
		Topics:               []string{"test_tmq_ws_auto_commit_topic"},
		AutoCommit:           "true",
		OffsetReset:          "earliest",
		AutoCommitIntervalMS: "500",
		SnapshotEnable:       "true",
		WithTableName:        "true",
	}

	b, _ := json.Marshal(init)
	action, _ := json.Marshal(&wstool.WSAction{
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
	err = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.NoError(t, err)
	assert.Equal(t, true, expectError)
	time.Sleep(time.Second * 5)
	w = httptest.NewRecorder()
	body = strings.NewReader("drop topic if exists test_tmq_ws_auto_commit_topic")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("drop database if exists test_ws_tmq_auto_commit")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

func TestTMQUnsubscribeAndSubscribe(t *testing.T) {
	ts1 := time.Now()
	ts2 := ts1.Add(time.Second)
	ts3 := ts2.Add(time.Second)
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists test_ws_tmq_unsubscribe WAL_RETENTION_PERIOD 86400")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	defer func() {
		w = httptest.NewRecorder()
		body = strings.NewReader("drop database if exists test_ws_tmq_unsubscribe")
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.RemoteAddr = "127.0.0.1:33333"
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}()

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct0 (ts timestamp, c1 int)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_unsubscribe", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct1 (ts timestamp, c1 int, c2 float)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_unsubscribe", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct2 (ts timestamp, c1 int, c2 float, c3 binary(10))")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_unsubscribe", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create topic if not exists test_tmq_ws_unsubscribe_topic as DATABASE test_ws_tmq_unsubscribe")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_unsubscribe", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct0 values('%s',1)`, ts1.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_unsubscribe", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create topic if not exists test_tmq_ws_unsubscribe2_topic as select * from ct0")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_unsubscribe", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct1 values('%s',1,2)`, ts2.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_unsubscribe", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct2 values('%s',1,2,'3')`, ts3.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_unsubscribe", body)
	req.RemoteAddr = "127.0.0.1:33333"
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
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	const (
		AfterTMQSubscribe = iota + 1
		AfterTMQPoll
		AfterTMQFetch
		AfterTMQFetchBlock
		AfterVersion
		AfterUnsubscribe
		AfterTMQSubscribe2
		AfterTMQPoll2
		AfterTMQFetch2
		AfterTMQFetchBlock2
	)
	messageID := uint64(0)
	status := 0
	finish := make(chan struct{})
	var tmqFetchResp *TMQFetchResp
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
			action, _ := json.Marshal(&wstool.WSAction{
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
				status = AfterUnsubscribe
				b, _ := json.Marshal(&TMQUnsubscribeReq{
					ReqID: 6,
				})
				action, _ := json.Marshal(&wstool.WSAction{
					Action: TMQUnsubscribe,
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
				action, _ := json.Marshal(&wstool.WSAction{
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
				action, _ := json.Marshal(&wstool.WSAction{
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
				status = AfterTMQPoll
				b, _ := json.Marshal(&TMQPollReq{
					ReqID:        3,
					BlockingTime: 500,
				})
				action, _ := json.Marshal(&wstool.WSAction{
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
			} else {
				tmqFetchResp = &d
				status = AfterTMQFetchBlock
				b, _ := json.Marshal(&TMQFetchBlockReq{
					ReqID:     0,
					MessageID: messageID,
				})
				action, _ := json.Marshal(&wstool.WSAction{
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
			_, _, value := parseblock.ParseTmqBlock(message[8:], tmqFetchResp.FieldsTypes, tmqFetchResp.Rows, tmqFetchResp.Precision)
			switch tmqFetchResp.TableName {
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
			action, _ := json.Marshal(&wstool.WSAction{
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
		case AfterUnsubscribe:
			pollCount = 0
			var d TMQUnsubscribeResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", TMQUnsubscribe, d.Code, d.Message)
			}
			status = AfterTMQSubscribe2
			b, _ := json.Marshal(&TMQSubscribeReq{
				ReqID:       0,
				OffsetReset: "earliest",
				Topics:      []string{"test_tmq_ws_unsubscribe2_topic"},
			})
			action, _ := json.Marshal(&wstool.WSAction{
				Action: TMQSubscribe,
				Args:   b,
			})
			t.Log(string(action))
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
		case AfterTMQSubscribe2:
			var d TMQSubscribeResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", TMQSubscribe, d.Code, d.Message)
			}
			status = AfterTMQPoll2
			b, _ := json.Marshal(&TMQPollReq{
				ReqID:        3,
				BlockingTime: 500,
			})
			action, _ := json.Marshal(&wstool.WSAction{
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
		case AfterTMQPoll2:
			if pollCount == 5 {
				status = AfterVersion
				action, _ := json.Marshal(&wstool.WSAction{
					Action: wstool.ClientVersion,
					Args:   nil,
				})
				t.Log(string(action))
				err = ws.WriteMessage(
					websocket.TextMessage,
					action,
				)
				err = ws.WriteMessage(
					websocket.TextMessage,
					action,
				)
				if err != nil {
					return err
				}
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
				status = AfterTMQFetch2
				b, _ := json.Marshal(&TMQFetchReq{
					ReqID:     4,
					MessageID: messageID,
				})
				action, _ := json.Marshal(&wstool.WSAction{
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
				status = AfterTMQPoll2
				//fetch
				b, _ := json.Marshal(&TMQPollReq{
					ReqID:        3,
					BlockingTime: 500,
				})
				action, _ := json.Marshal(&wstool.WSAction{
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
		case AfterTMQFetch2:
			var d TMQFetchResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", TMQFetch, d.Code, d.Message)
			}

			if d.Completed {
				status = AfterTMQPoll2
				b, _ := json.Marshal(&TMQPollReq{
					ReqID:        3,
					BlockingTime: 500,
				})
				action, _ := json.Marshal(&wstool.WSAction{
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
			} else {
				tmqFetchResp = &d
				status = AfterTMQFetchBlock2
				b, _ := json.Marshal(&TMQFetchBlockReq{
					ReqID:     0,
					MessageID: messageID,
				})
				action, _ := json.Marshal(&wstool.WSAction{
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
		case AfterTMQFetchBlock2:
			_, _, value := parseblock.ParseTmqBlock(message[8:], tmqFetchResp.FieldsTypes, tmqFetchResp.Rows, tmqFetchResp.Precision)
			assert.Equal(t, 1, len(value))
			assert.Equal(t, ts1.UnixNano()/1e6, value[0][0].(time.Time).UnixNano()/1e6)
			assert.Equal(t, int32(1), value[0][1])
			status = AfterTMQFetch2
			b, _ := json.Marshal(&TMQFetchReq{
				ReqID:     4,
				MessageID: messageID,
			})
			action, _ := json.Marshal(&wstool.WSAction{
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
		case AfterVersion:
			var d wstool.WSVersionResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", TMQFetch, d.Code, d.Message)
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
				var closeErr *websocket.CloseError
				if errors.As(err, &closeErr) && closeErr.Code == websocket.CloseAbnormalClosure {
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
		ReqID:                0,
		User:                 "root",
		Password:             "taosdata",
		GroupID:              "test",
		OffsetReset:          "earliest",
		Topics:               []string{"test_tmq_ws_unsubscribe_topic"},
		AutoCommit:           "true",
		AutoCommitIntervalMS: "500",
		SnapshotEnable:       "true",
		WithTableName:        "true",
	}

	b, _ := json.Marshal(init)
	action, _ := json.Marshal(&wstool.WSAction{
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
	err = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.NoError(t, err)
	time.Sleep(time.Second * 5)
	w = httptest.NewRecorder()
	body = strings.NewReader("drop topic if exists test_tmq_ws_unsubscribe_topic")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("drop topic if exists test_tmq_ws_unsubscribe2_topic")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("drop database if exists test_ws_tmq_unsubscribe")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

func TestTMQSeek(t *testing.T) {
	vgroups := 2
	ts1 := time.Now()
	ts2 := ts1.Add(time.Second)
	ts3 := ts2.Add(time.Second)
	insertSql := []string{
		fmt.Sprintf(`insert into ct0 values('%s',1)`, ts1.Format(time.RFC3339Nano)),
		fmt.Sprintf(`insert into ct1 values('%s',1,2)`, ts2.Format(time.RFC3339Nano)),
		fmt.Sprintf(`insert into ct2 values('%s',1,2,'3')`, ts3.Format(time.RFC3339Nano)),
	}
	insertCount := len(insertSql)
	tryPollCount := 3 * insertCount
	topic := "test_tmq_ws_seek_topic"
	dbName := "test_ws_tmq_seek"
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists " + dbName + " vgroups " + strconv.Itoa(vgroups) + " WAL_RETENTION_PERIOD 86400")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct0 (ts timestamp, c1 int)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/"+dbName, body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct1 (ts timestamp, c1 int, c2 float)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/"+dbName, body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct2 (ts timestamp, c1 int, c2 float, c3 binary(10))")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/"+dbName, body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	for i := 0; i < insertCount; i++ {
		w = httptest.NewRecorder()
		body = strings.NewReader(insertSql[i])
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql/"+dbName, body)
		req.RemoteAddr = "127.0.0.1:33333"
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	w = httptest.NewRecorder()
	body = strings.NewReader("create topic if not exists " + topic + " as database " + dbName)
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/"+dbName, body)
	req.RemoteAddr = "127.0.0.1:33333"
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
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	//sub
	{
		req := &TMQSubscribeReq{
			ReqID:         0,
			User:          "root",
			Password:      "taosdata",
			GroupID:       "test",
			Topics:        []string{topic},
			OffsetReset:   "earliest",
			AutoCommit:    "false",
			WithTableName: "true",
		}
		b, _ := json.Marshal(req)
		action, _ := json.Marshal(&wstool.WSAction{
			Action: TMQSubscribe,
			Args:   b,
		})
		err = ws.WriteMessage(
			websocket.TextMessage,
			action,
		)
		assert.NoError(t, err)
		mt, message, err := ws.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, websocket.TextMessage, mt)
		var resp TMQSubscribeResp
		err = json.Unmarshal(message, &resp)
		assert.NoError(t, err)
		assert.Equal(t, 0, resp.Code)
	}
	//assignment 1
	vgID := make([]int32, vgroups)
	{
		req := TMQGetTopicAssignmentReq{
			ReqID: 1,
			Topic: topic,
		}
		b, _ := json.Marshal(req)
		action, _ := json.Marshal(&wstool.WSAction{
			Action: TMQGetTopicAssignment,
			Args:   b,
		})
		err = ws.WriteMessage(
			websocket.TextMessage,
			action,
		)
		assert.NoError(t, err)
		mt, message, err := ws.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, websocket.TextMessage, mt)
		var resp TMQGetTopicAssignmentResp
		err = json.Unmarshal(message, &resp)
		assert.NoError(t, err)
		assert.Equal(t, 0, resp.Code)
		assert.Equal(t, vgroups, len(resp.Assignment))
		for i := 0; i < vgroups; i++ {
			assert.Equal(t, int64(0), resp.Assignment[i].Offset)
			assert.Equal(t, int64(0), resp.Assignment[i].Begin)
			vgID[i] = resp.Assignment[i].VGroupID
		}
	}
	//poll 1
	{
		rowCount := 0
		for i := 0; i < tryPollCount; i++ {
			if rowCount >= insertCount {
				break
			}
			req := TMQPollReq{
				ReqID:        1,
				BlockingTime: 500,
			}
			b, _ := json.Marshal(req)
			action, _ := json.Marshal(&wstool.WSAction{
				Action: TMQPoll,
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			assert.NoError(t, err)
			mt, message, err := ws.ReadMessage()
			assert.NoError(t, err)
			assert.Equal(t, websocket.TextMessage, mt)
			var resp TMQPollResp
			err = json.Unmarshal(message, &resp)
			assert.NoError(t, err)
			assert.Equal(t, 0, resp.Code)
			if resp.HaveMessage {
				for {
					req := TMQFetchReq{
						ReqID:     1,
						MessageID: resp.MessageID,
					}
					b, _ := json.Marshal(req)
					action, _ := json.Marshal(&wstool.WSAction{
						Action: TMQFetch,
						Args:   b,
					})
					err = ws.WriteMessage(
						websocket.TextMessage,
						action,
					)
					assert.NoError(t, err)
					mt, message, err := ws.ReadMessage()
					assert.NoError(t, err)
					assert.Equal(t, websocket.TextMessage, mt)
					var tmqFetchResp TMQFetchResp
					err = json.Unmarshal(message, &tmqFetchResp)
					assert.NoError(t, err)
					assert.Equal(t, 0, tmqFetchResp.Code)
					if tmqFetchResp.Completed {
						break
					}
					fetchBlockReq := TMQFetchBlockReq{
						ReqID:     1,
						MessageID: tmqFetchResp.MessageID,
					}
					b, _ = json.Marshal(fetchBlockReq)
					action, _ = json.Marshal(&wstool.WSAction{
						Action: TMQFetchBlock,
						Args:   b,
					})
					err = ws.WriteMessage(
						websocket.TextMessage,
						action,
					)
					assert.NoError(t, err)
					mt, message, err = ws.ReadMessage()
					assert.NoError(t, err)
					assert.Equal(t, websocket.BinaryMessage, mt)
					_, _, value := parseblock.ParseTmqBlock(message[8:], tmqFetchResp.FieldsTypes, tmqFetchResp.Rows, tmqFetchResp.Precision)
					t.Log(value)
					rowCount += 1

				}
				{
					req := TMQCommitReq{
						ReqID:     1,
						MessageID: resp.MessageID,
					}
					b, _ := json.Marshal(req)
					action, _ := json.Marshal(&wstool.WSAction{
						Action: TMQCommit,
						Args:   b,
					})
					err = ws.WriteMessage(
						websocket.TextMessage,
						action,
					)
					assert.NoError(t, err)
					mt, message, err := ws.ReadMessage()
					assert.NoError(t, err)
					assert.Equal(t, websocket.TextMessage, mt)
					var resp TMQPollResp
					err = json.Unmarshal(message, &resp)
					assert.NoError(t, err)
					assert.Equal(t, 0, resp.Code)
				}
			}
		}
		assert.Equal(t, insertCount, rowCount)
	}
	//
	code, message := doHttpSql(fmt.Sprintf("insert into %s.ct0 values(now,2)", dbName))
	assert.Equal(t, 0, code, message)
	insertCount += 1

	// poll2
	for i := 0; i < tryPollCount; i++ {
		b, _ := json.Marshal(TMQPollReq{ReqID: 0, BlockingTime: 500})
		msg, err := doWebSocket(ws, TMQPoll, b)
		assert.NoError(t, err)
		var resp TMQPollResp
		err = json.Unmarshal(msg, &resp)
		assert.NoError(t, err)
		assert.Equal(t, 0, resp.Code)
		if resp.HaveMessage {
			break
		}
	}

	//assignment after poll
	{
		req := TMQGetTopicAssignmentReq{
			ReqID: 1,
			Topic: topic,
		}
		b, _ := json.Marshal(req)
		action, _ := json.Marshal(&wstool.WSAction{
			Action: TMQGetTopicAssignment,
			Args:   b,
		})
		err = ws.WriteMessage(
			websocket.TextMessage,
			action,
		)
		assert.NoError(t, err)
		mt, message, err := ws.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, websocket.TextMessage, mt)
		var resp TMQGetTopicAssignmentResp
		err = json.Unmarshal(message, &resp)
		assert.NoError(t, err)
		assert.Equal(t, 0, resp.Code)
		assert.Equal(t, vgroups, len(resp.Assignment))
		for i := 0; i < vgroups; i++ {
			assert.Equal(t, int64(0), resp.Assignment[0].Begin)
		}
	}
	//seek
	for i := 0; i < vgroups; i++ {
		req := TMQOffsetSeekReq{
			ReqID:    uint64(i),
			Topic:    topic,
			VgroupID: vgID[i],
			Offset:   0,
		}
		b, _ := json.Marshal(req)
		action, _ := json.Marshal(&wstool.WSAction{
			Action: TMQSeek,
			Args:   b,
		})
		err = ws.WriteMessage(
			websocket.TextMessage,
			action,
		)
		assert.NoError(t, err)
		mt, message, err := ws.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, websocket.TextMessage, mt)
		var resp TMQOffsetSeekResp
		err = json.Unmarshal(message, &resp)
		assert.NoError(t, err)
		assert.Equal(t, 0, resp.Code)
	}
	//assignment after seek
	{
		req := TMQGetTopicAssignmentReq{
			ReqID: 1,
			Topic: topic,
		}
		b, _ := json.Marshal(req)
		action, _ := json.Marshal(&wstool.WSAction{
			Action: TMQGetTopicAssignment,
			Args:   b,
		})
		err = ws.WriteMessage(
			websocket.TextMessage,
			action,
		)
		assert.NoError(t, err)
		mt, message, err := ws.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, websocket.TextMessage, mt)
		var resp TMQGetTopicAssignmentResp
		err = json.Unmarshal(message, &resp)
		assert.NoError(t, err)
		assert.Equal(t, 0, resp.Code)
		assert.Equal(t, vgroups, len(resp.Assignment))
		for i := 0; i < vgroups; i++ {
			assert.Equal(t, int64(0), resp.Assignment[i].Offset)
			assert.Equal(t, int64(0), resp.Assignment[i].Begin)
		}

	}
	//poll after seek
	{
		rowCount := 0
		for i := 0; i < tryPollCount; i++ {
			if rowCount >= insertCount {
				break
			}
			req := TMQPollReq{
				ReqID:        1,
				BlockingTime: 500,
			}
			b, _ := json.Marshal(req)
			action, _ := json.Marshal(&wstool.WSAction{
				Action: TMQPoll,
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			assert.NoError(t, err)
			mt, message, err := ws.ReadMessage()
			assert.NoError(t, err)
			assert.Equal(t, websocket.TextMessage, mt)
			var resp TMQPollResp
			err = json.Unmarshal(message, &resp)
			assert.NoError(t, err)
			assert.Equal(t, 0, resp.Code)
			if resp.HaveMessage {
				for {
					req := TMQFetchReq{
						ReqID:     1,
						MessageID: resp.MessageID,
					}
					b, _ := json.Marshal(req)
					action, _ := json.Marshal(&wstool.WSAction{
						Action: TMQFetch,
						Args:   b,
					})
					err = ws.WriteMessage(
						websocket.TextMessage,
						action,
					)
					assert.NoError(t, err)
					mt, message, err := ws.ReadMessage()
					assert.NoError(t, err)
					assert.Equal(t, websocket.TextMessage, mt)
					var tmqFetchResp TMQFetchResp
					err = json.Unmarshal(message, &tmqFetchResp)
					assert.NoError(t, err)
					assert.Equal(t, 0, tmqFetchResp.Code)
					if tmqFetchResp.Completed {
						break
					}
					fetchBlockReq := TMQFetchBlockReq{
						ReqID:     1,
						MessageID: tmqFetchResp.MessageID,
					}
					b, _ = json.Marshal(fetchBlockReq)
					action, _ = json.Marshal(&wstool.WSAction{
						Action: TMQFetchBlock,
						Args:   b,
					})
					err = ws.WriteMessage(
						websocket.TextMessage,
						action,
					)
					assert.NoError(t, err)
					mt, message, err = ws.ReadMessage()
					assert.NoError(t, err)
					assert.Equal(t, websocket.BinaryMessage, mt)
					_, _, value := parseblock.ParseTmqBlock(message[8:], tmqFetchResp.FieldsTypes, tmqFetchResp.Rows, tmqFetchResp.Precision)
					t.Log(value)
					rowCount += 1
				}
				{
					req := TMQCommitReq{
						ReqID:     1,
						MessageID: resp.MessageID,
					}
					b, _ := json.Marshal(req)
					action, _ := json.Marshal(&wstool.WSAction{
						Action: TMQCommit,
						Args:   b,
					})
					err = ws.WriteMessage(
						websocket.TextMessage,
						action,
					)
					assert.NoError(t, err)
					mt, message, err := ws.ReadMessage()
					assert.NoError(t, err)
					assert.Equal(t, websocket.TextMessage, mt)
					var resp TMQPollResp
					err = json.Unmarshal(message, &resp)
					assert.NoError(t, err)
					assert.Equal(t, 0, resp.Code)
				}
			}
		}
		assert.Equal(t, insertCount, rowCount)
	}
	//assignment after poll2
	{
		req := TMQGetTopicAssignmentReq{
			ReqID: 1,
			Topic: topic,
		}
		b, _ := json.Marshal(req)
		action, _ := json.Marshal(&wstool.WSAction{
			Action: TMQGetTopicAssignment,
			Args:   b,
		})
		err = ws.WriteMessage(
			websocket.TextMessage,
			action,
		)
		assert.NoError(t, err)
		mt, message, err := ws.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, websocket.TextMessage, mt)
		var resp TMQGetTopicAssignmentResp
		err = json.Unmarshal(message, &resp)
		assert.NoError(t, err)
		assert.Equal(t, 0, resp.Code)
		assert.Equal(t, vgroups, len(resp.Assignment))
		for i := 0; i < vgroups; i++ {
			assert.Equal(t, int64(0), resp.Assignment[i].Begin)
		}
	}

	b, _ := json.Marshal(TMQUnsubscribeReq{})
	action, _ := json.Marshal(&wstool.WSAction{
		Action: TMQUnsubscribe,
		Args:   b,
	})
	err = ws.WriteMessage(websocket.TextMessage, action)
	assert.NoError(t, err)
	_, _, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.NoError(t, err)
	time.Sleep(time.Second * 3)
	w = httptest.NewRecorder()
	body = strings.NewReader("drop topic if exists " + topic)
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("drop database if exists " + dbName)
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

func doHttpSql(sql string) (code int, message string) {
	w := httptest.NewRecorder()
	body := strings.NewReader(sql)
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	b, _ := io.ReadAll(w.Body)
	var res WSTMQErrorResp
	_ = json.Unmarshal(b, &res)
	return res.Code, res.Message
}

func doWebSocket(ws *websocket.Conn, action string, arg []byte) (resp []byte, err error) {
	a, _ := json.Marshal(&wstool.WSAction{Action: action, Args: arg})
	err = ws.WriteMessage(websocket.TextMessage, a)
	if err != nil {
		return nil, err
	}
	_, message, err := ws.ReadMessage()
	return message, err
}

func before(t *testing.T, dbName string, topic string) {
	doHttpSql(fmt.Sprintf("drop topic if exists %s", topic))
	doHttpSql(fmt.Sprintf("drop database if exists %s", dbName))
	code, message := doHttpSql(fmt.Sprintf("create database if not exists %s WAL_RETENTION_PERIOD 86400", dbName))
	assert.Equal(t, 0, code, message)

	code, message = doHttpSql(fmt.Sprintf("create table if not exists %s.ct0 (ts timestamp, c1 int)", dbName))
	assert.Equal(t, 0, code, message)

	code, message = doHttpSql(fmt.Sprintf("create table if not exists %s.ct1 (ts timestamp, c1 int, c2 float)", dbName))
	assert.Equal(t, 0, code, message)

	code, message = doHttpSql(fmt.Sprintf("create table if not exists %s.ct2 (ts timestamp, c1 int, c2 float, c3 binary(10))", dbName))
	assert.Equal(t, 0, code, message)

	code, message = doHttpSql(fmt.Sprintf("insert into %s.ct0 values (now, 1)", dbName))
	assert.Equal(t, 0, code, message)
	code, message = doHttpSql(fmt.Sprintf("insert into %s.ct1 values (now, 1, 2)", dbName))
	assert.Equal(t, 0, code, message)
	code, message = doHttpSql(fmt.Sprintf("insert into %s.ct2 values (now, 1, 2, '3')", dbName))
	assert.Equal(t, 0, code, message)

	code, message = doHttpSql(fmt.Sprintf("create topic if not exists %s as database %s", topic, dbName))
	assert.Equal(t, 0, code, message)
}

func after(ws *websocket.Conn, dbName string, topic string) error {
	b, _ := json.Marshal(TMQUnsubscribeReq{ReqID: 0})
	_, _ = doWebSocket(ws, TMQUnsubscribe, b)
	err := ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	if err != nil {
		return err
	}
	time.Sleep(time.Second * 5)
	doHttpSql(fmt.Sprintf("drop topic if exists %s", topic))
	doHttpSql(fmt.Sprintf("drop database if exists %s", dbName))
	return nil
}

func TestTMQ_Position_And_Committed(t *testing.T) {
	dbName := "test_ws_tmq_position_and_committed"
	topic := "test_ws_tmq_position_and_committed_topic"

	before(t, dbName, topic)

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	defer func() {
		err = after(ws, dbName, topic)
		assert.NoError(t, err)
	}()

	// subscribe
	b, _ := json.Marshal(TMQSubscribeReq{
		User:        "root",
		Password:    "taosdata",
		DB:          dbName,
		GroupID:     "test",
		Topics:      []string{topic},
		AutoCommit:  "false",
		OffsetReset: "earliest",
	})
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)

	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 0, BlockingTime: 500})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	var pollResp TMQPollResp
	err = json.Unmarshal(msg, &pollResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, pollResp.Message)
	assert.True(t, pollResp.HaveMessage)

	//commit
	b, _ = json.Marshal(TMQCommitReq{ReqID: 0, MessageID: pollResp.MessageID})
	msg, err = doWebSocket(ws, TMQCommit, b)
	assert.NoError(t, err)
	var commitResp TMQCommitResp
	err = json.Unmarshal(msg, &commitResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, commitResp.Code, commitResp.Message)

	// committed
	b, _ = json.Marshal(TMQCommittedReq{ReqID: 0, TopicVgroupIDs: []TopicVgroupID{{Topic: topic, VgroupID: pollResp.VgroupID}}})
	msg, err = doWebSocket(ws, TMQCommitted, b)
	assert.NoError(t, err)
	if err != nil {
		t.Fatal(err)
	}
	var committedResp TMQCommittedResp
	err = json.Unmarshal(msg, &committedResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, committedResp.Code, committedResp.Message)
	assert.Equal(t, 1, len(committedResp.Committed))
	assert.Equal(t, true, committedResp.Committed[0] > 0)

	// position
	b, _ = json.Marshal(TMQPositionReq{ReqID: 0, TopicVgroupIDs: []TopicVgroupID{{Topic: topic, VgroupID: pollResp.VgroupID}}})
	msg, err = doWebSocket(ws, TMQPosition, b)
	assert.NoError(t, err)
	if err != nil {
		t.Fatal(err)
	}
	var positionResp TMQPositionResp
	err = json.Unmarshal(msg, &positionResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, positionResp.Code, positionResp.Message)
	assert.Equal(t, 1, len(positionResp.Position))
	assert.Equal(t, true, positionResp.Position[0] > 0)
}

func TestTMQ_ListTopics(t *testing.T) {
	dbName := "test_ws_tmq_list_topics"
	topic := "test_ws_tmq_list_topics"

	before(t, dbName, topic)

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	defer func() {
		err = after(ws, dbName, topic)
		assert.NoError(t, err)
	}()

	// subscribe
	b, _ := json.Marshal(TMQSubscribeReq{
		User:        "root",
		Password:    "taosdata",
		DB:          dbName,
		GroupID:     "test",
		Topics:      []string{topic},
		AutoCommit:  "false",
		OffsetReset: "earliest",
	})
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)

	b, _ = json.Marshal(TMQListTopicsReq{ReqID: 0})
	msg, err = doWebSocket(ws, TMQListTopics, b)
	assert.NoError(t, err)
	var listTopicResp TMQListTopicsResp
	err = json.Unmarshal(msg, &listTopicResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, listTopicResp.Code, listTopicResp.Message)
	assert.Equal(t, []string{topic}, listTopicResp.Topics)
}

func TestTMQ_CommitOffset(t *testing.T) {
	dbName := "test_ws_tmq_commit_offset"
	topic := "test_ws_tmq_commit_offset_topic"

	before(t, dbName, topic)

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	defer func() {
		err = after(ws, dbName, topic)
		assert.NoError(t, err)
	}()

	// subscribe
	b, _ := json.Marshal(TMQSubscribeReq{
		User:        "root",
		Password:    "taosdata",
		DB:          dbName,
		GroupID:     "test",
		Topics:      []string{topic},
		AutoCommit:  "false",
		OffsetReset: "earliest",
	})
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)

	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 0, BlockingTime: 500})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	var pollResp TMQPollResp
	err = json.Unmarshal(msg, &pollResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))

	// insert
	code, message := doHttpSql(fmt.Sprintf("insert into %s.ct0 values (now, 2)", dbName))
	assert.Equal(t, 0, code, message)

	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 0, BlockingTime: 500})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	err = json.Unmarshal(msg, &pollResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	assert.True(t, pollResp.HaveMessage, string(msg))
	assert.True(t, pollResp.Offset >= 0, string(msg))

	//commit offset
	b, _ = json.Marshal(TMQCommitOffsetReq{ReqID: 0, Topic: topic, VgroupID: pollResp.VgroupID, Offset: pollResp.Offset})
	msg, err = doWebSocket(ws, TMQCommitOffset, b)
	assert.NoError(t, err)
	var commitOffsetResp TMQCommitOffsetResp
	err = json.Unmarshal(msg, &commitOffsetResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, commitOffsetResp.Code, commitOffsetResp.Message)

	// committed
	b, _ = json.Marshal(TMQCommittedReq{ReqID: 0, TopicVgroupIDs: []TopicVgroupID{{Topic: topic, VgroupID: pollResp.VgroupID}}})
	msg, err = doWebSocket(ws, TMQCommitted, b)
	assert.NoError(t, err)
	if err != nil {
		t.Fatal(err)
	}
	var committedResp TMQCommittedResp
	err = json.Unmarshal(msg, &committedResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, committedResp.Code, string(msg))
	assert.Equal(t, 1, len(committedResp.Committed), string(msg))
	assert.Equal(t, pollResp.Offset, committedResp.Committed[0], string(msg))
}

func TestTMQ_PollAfterClose(t *testing.T) {
	dbName := "test_ws_tmq_poll_after_close"
	topic := "test_ws_tmq_poll_after_close_topic"

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	before(t, dbName, topic)

	defer func() {
		err = after(ws, dbName, topic)
		assert.NoError(t, err)
	}()

	// subscribe
	b, _ := json.Marshal(TMQSubscribeReq{
		User:        "root",
		Password:    "taosdata",
		DB:          dbName,
		GroupID:     "test",
		Topics:      []string{topic},
		AutoCommit:  "false",
		OffsetReset: "earliest",
	})
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)
	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(time.Millisecond * 100)
			code, message := doHttpSql(fmt.Sprintf("insert into %s.ct2 values (now, 1, 2, '3')", dbName))
			if code == 902 {
				// exited, db not exists
				return
			}
			assert.Equal(t, 0, code, message)
		}
	}()
	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 0, BlockingTime: 500})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	var pollResp TMQPollResp
	err = json.Unmarshal(msg, &pollResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	assert.True(t, pollResp.HaveMessage, string(msg))
	type wsRespMsg struct {
		messageType int
		p           []byte
	}
	waitMap := make(map[uint64]chan *wsRespMsg)
	type tmqResp struct {
		ReqID uint64 `json:"req_id"`
	}
	go func() {
		count := 0
		for {
			mt, p, err := ws.ReadMessage()
			if err != nil {
				t.Error(err)
				return
			}
			t.Log(string(p))
			count += 1
			var resp tmqResp
			err = json.Unmarshal(p, &resp)
			if err != nil {
				t.Error(err)
			}
			ch := waitMap[resp.ReqID]
			ch <- &wsRespMsg{messageType: mt, p: p}
			if count == 10 {
				break
			}
		}
	}()
	wg := sync.WaitGroup{}
	locker := sync.Mutex{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		ch := make(chan *wsRespMsg, 1)
		waitMap[uint64(i)] = ch
		go func(index int) {
			defer wg.Done()
			b, _ := json.Marshal(TMQPollReq{ReqID: uint64(index), BlockingTime: 500})
			t.Log(string(b))
			locker.Lock()
			a, _ := json.Marshal(&wstool.WSAction{Action: TMQPoll, Args: b})
			err = ws.WriteMessage(websocket.TextMessage, a)
			locker.Unlock()
			if err != nil {
				t.Log(index, "poll send failed", err)
				return
			}
			timeout, cancel := context.WithTimeout(context.Background(), time.Second*20)
			defer cancel()
			select {
			case <-timeout.Done():
				t.Error(timeout)
			case msg := <-ch:
				var pollResp TMQPollResp
				err = json.Unmarshal(msg.p, &pollResp)
				assert.NoError(t, err)
				assert.Equal(t, 0, pollResp.Code, string(msg.p))
			}
		}(i)
	}

	wg.Wait()
	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 0, BlockingTime: 500})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	err = json.Unmarshal(msg, &pollResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
}

type fetchRawNewResponse struct {
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
}

func parseFetchRawNewResponse(bs []byte) *fetchRawNewResponse {
	resp := &fetchRawNewResponse{}
	resp.Flag = binary.LittleEndian.Uint64(bs)
	resp.Action = binary.LittleEndian.Uint64(bs[8:])
	resp.Version = binary.LittleEndian.Uint16(bs[16:])
	resp.Time = binary.LittleEndian.Uint64(bs[18:])
	resp.ReqID = binary.LittleEndian.Uint64(bs[26:])
	resp.Code = binary.LittleEndian.Uint32(bs[34:])
	resp.MessageLen = binary.LittleEndian.Uint32(bs[38:])
	resp.Message = string(bs[42 : 42+resp.MessageLen])
	resp.MessageID = binary.LittleEndian.Uint64(bs[42+resp.MessageLen:])
	if resp.Code != 0 {
		return resp
	}
	resp.MetaType = binary.LittleEndian.Uint16(bs[50+resp.MessageLen:])
	resp.RawBlockLength = binary.LittleEndian.Uint32(bs[52+resp.MessageLen:])
	resp.TMQRawBlock = bs[56+resp.MessageLen : 56+resp.MessageLen+resp.RawBlockLength]
	return resp
}
func TestTMQ_FetchRawNew(t *testing.T) {
	dbName := "test_ws_tmq_fetch_raw_new"
	topic := "test_ws_tmq_fetch_raw_new_topic"

	before(t, dbName, topic)

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	defer func() {
		err = after(ws, dbName, topic)
		assert.NoError(t, err)
	}()

	// subscribe
	b, _ := json.Marshal(TMQSubscribeReq{
		User:        "root",
		Password:    "taosdata",
		DB:          dbName,
		GroupID:     "test",
		Topics:      []string{topic},
		AutoCommit:  "false",
		OffsetReset: "earliest",
	})
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)

	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 0, BlockingTime: 500})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	var pollResp TMQPollResp
	err = json.Unmarshal(msg, &pollResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))

	// insert
	code, message := doHttpSql(fmt.Sprintf("insert into %s.ct0 values (now, 2)", dbName))
	assert.Equal(t, 0, code, message)

	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 0, BlockingTime: 500})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	err = json.Unmarshal(msg, &pollResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	assert.True(t, pollResp.HaveMessage, string(msg))
	assert.True(t, pollResp.Offset >= 0, string(msg))

	// fetch raw new
	b, _ = json.Marshal(TMQFetchRawReq{ReqID: 100, MessageID: pollResp.MessageID})
	msg, err = doWebSocket(ws, TMQFetchRawNew, b)
	assert.NoError(t, err)
	resp := parseFetchRawNewResponse(msg)
	assert.Equal(t, uint64(0xffffffffffffffff), resp.Flag, resp.Flag)
	assert.Equal(t, uint32(0), resp.Code, resp.Message)
	assert.Equal(t, uint16(1), resp.Version)
	assert.Equal(t, uint64(TMQFetchRawNewMessage), resp.Action)
	assert.Greater(t, resp.Time, uint64(0))
	assert.Equal(t, uint64(100), resp.ReqID)
	assert.Equal(t, pollResp.MessageID, resp.MessageID)
	assert.Equal(t, int(resp.RawBlockLength), len(resp.TMQRawBlock))
	ps := parser.NewTMQRawDataParser()
	blockInfo, err := ps.Parse(unsafe.Pointer(&resp.TMQRawBlock[0]))
	assert.NoError(t, err)
	for _, info := range blockInfo {
		t.Log(info.TableName)
		data := parser.ReadBlockSimple(info.RawBlock, info.Precision)
		for i, schema := range info.Schema {
			t.Log(schema.Name, schema.ColType, schema.Flag, schema.Bytes, schema.ColID)
			assert.Equal(t, i+1, schema.ColID)
		}
		v, err := json.Marshal(data)
		assert.NoError(t, err)
		t.Log(string(v))
	}

	// fetch wrong
	b, _ = json.Marshal(TMQFetchRawReq{ReqID: 100, MessageID: 8000})
	msg, err = doWebSocket(ws, TMQFetchRawNew, b)
	assert.NoError(t, err)
	resp = parseFetchRawNewResponse(msg)
	assert.Equal(t, uint64(0xffffffffffffffff), resp.Flag, resp.Flag)
	assert.Equal(t, uint32(65535), resp.Code, resp.Message)
	assert.Equal(t, uint16(1), resp.Version)
	assert.Equal(t, uint64(TMQFetchRawNewMessage), resp.Action)
	assert.Greater(t, resp.Time, uint64(0))
	assert.Equal(t, uint64(100), resp.ReqID)
	assert.Equal(t, uint64(8000), resp.MessageID)
	t.Log(resp.Message)

	//commit offset
	b, _ = json.Marshal(TMQCommitOffsetReq{ReqID: 0, Topic: topic, VgroupID: pollResp.VgroupID, Offset: pollResp.Offset})
	msg, err = doWebSocket(ws, TMQCommitOffset, b)
	assert.NoError(t, err)
	var commitOffsetResp TMQCommitOffsetResp
	err = json.Unmarshal(msg, &commitOffsetResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, commitOffsetResp.Code, commitOffsetResp.Message)

	// committed
	b, _ = json.Marshal(TMQCommittedReq{ReqID: 0, TopicVgroupIDs: []TopicVgroupID{{Topic: topic, VgroupID: pollResp.VgroupID}}})
	msg, err = doWebSocket(ws, TMQCommitted, b)
	assert.NoError(t, err)
	if err != nil {
		t.Fatal(err)
	}
	var committedResp TMQCommittedResp
	err = json.Unmarshal(msg, &committedResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, committedResp.Code, string(msg))
	assert.Equal(t, 1, len(committedResp.Committed), string(msg))
	assert.Equal(t, pollResp.Offset, committedResp.Committed[0], string(msg))
}

func TestTMQ_SetMsgConsumeExcluded(t *testing.T) {
	dbName := "test_ws_tmq_set_msg_consume_excluded"
	topic := "test_ws_tmq_set_msg_consume_excluded_topic"

	before(t, dbName, topic)

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	defer func() {
		err = after(ws, dbName, topic)
		assert.NoError(t, err)
	}()

	// subscribe
	b, _ := json.Marshal(TMQSubscribeReq{
		User:               "root",
		Password:           "taosdata",
		DB:                 dbName,
		GroupID:            "test",
		Topics:             []string{topic},
		AutoCommit:         "false",
		OffsetReset:        "earliest",
		MsgConsumeExcluded: "1",
	})
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)
}

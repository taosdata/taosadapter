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
	"testing"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	_ "github.com/taosdata/taosadapter/v3/controller/rest"
	"github.com/taosdata/taosadapter/v3/controller/ws/query"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	"github.com/taosdata/taosadapter/v3/driver/common/tmq"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/layout"
	"github.com/taosdata/taosadapter/v3/tools/parseblock"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
	"github.com/taosdata/taosadapter/v3/version"
)

var router *gin.Engine

func TestMain(m *testing.M) {
	viper.Set("pool.maxConnect", 10000)
	viper.Set("pool.maxIdle", 10000)
	viper.Set("logLevel", "trace")
	viper.Set("uploadKeeper.enable", false)
	config.Init()
	log.ConfigLog()
	db.PrepareConnection()
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
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	defer func() {
		w = httptest.NewRecorder()
		body = strings.NewReader("drop database if exists test_ws_tmq")
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}()

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct0 (ts timestamp, c1 int)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct1 (ts timestamp, c1 int, c2 float)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct2 (ts timestamp, c1 int, c2 float, c3 binary(10))")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create topic if not exists test_tmq_ws_topic as DATABASE test_ws_tmq")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct0 values('%s',1)`, ts1.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct1 values('%s',1,2)`, ts2.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct2 values('%s',1,2,'3')`, ts3.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
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
			_, _, value, err := parseblock.ParseTmqBlock(message[8:], tmqFetchResp.FieldsTypes, tmqFetchResp.Rows, tmqFetchResp.Precision)
			if err != nil {
				return err
			}
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
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("drop database if exists test_ws_tmq")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
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
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create topic if not exists test_tmq_meta_ws_topic with meta as DATABASE test_ws_tmq_meta")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_meta", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
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
			assert.Equal(t, version.TaosClientVersion, d.Version)
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
				"c13 nchar(20)," +
				"c14 varbinary(20)," +
				"c15 geometry(100)," +
				"c16 decimal(20,4)" +
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
				"tc13 nchar(20)," +
				"tc14 varbinary(20)," +
				"tc15 geometry(100)" +
				")")
			req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_meta", body)
			req.RemoteAddr = testtools.GetRandomRemoteAddr()
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
			req.RemoteAddr = testtools.GetRandomRemoteAddr()
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
				{"c14", "VARBINARY", float64(20), ""},
				{"c15", "GEOMETRY", float64(100), ""},
				{"c16", "DECIMAL(20, 4)", float64(16), ""},
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
				{"tc14", "VARBINARY", float64(20), "TAG"},
				{"tc15", "GEOMETRY", float64(100), "TAG"},
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
		EnableBatchMeta:      "1",
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
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
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
		{"c14", "VARBINARY", float64(20), ""},
		{"c15", "GEOMETRY", float64(100), ""},
		{"c16", "DECIMAL(20, 4)", float64(16), ""},
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
		{"tc14", "VARBINARY", float64(20), "TAG"},
		{"tc15", "GEOMETRY", float64(100), "TAG"},
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
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("drop database if exists test_ws_tmq_meta_target")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("drop database if exists test_ws_tmq_meta")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

func writeRaw(t *testing.T, rawData []byte) {
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists test_ws_tmq_meta_target WAL_RETENTION_PERIOD 86400")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
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
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct0 (ts timestamp, c1 int)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_auto_commit", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct1 (ts timestamp, c1 int, c2 float)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_auto_commit", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct2 (ts timestamp, c1 int, c2 float, c3 binary(10))")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_auto_commit", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create topic if not exists test_tmq_ws_auto_commit_topic as DATABASE test_ws_tmq_auto_commit")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_auto_commit", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct0 values('%s',1)`, ts1.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_auto_commit", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct1 values('%s',1,2)`, ts2.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_auto_commit", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct2 values('%s',1,2,'3')`, ts3.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_auto_commit", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
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
			_, _, value, err := parseblock.ParseTmqBlock(message[8:], tmqFetchResp.FieldsTypes, tmqFetchResp.Rows, tmqFetchResp.Precision)
			if err != nil {
				return err
			}
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
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("drop database if exists test_ws_tmq_auto_commit")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
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
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	defer func() {
		w = httptest.NewRecorder()
		body = strings.NewReader("drop database if exists test_ws_tmq_unsubscribe")
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}()

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct0 (ts timestamp, c1 int)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_unsubscribe", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct1 (ts timestamp, c1 int, c2 float)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_unsubscribe", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct2 (ts timestamp, c1 int, c2 float, c3 binary(10))")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_unsubscribe", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create topic if not exists test_tmq_ws_unsubscribe_topic as DATABASE test_ws_tmq_unsubscribe")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_unsubscribe", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct0 values('%s',1)`, ts1.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_unsubscribe", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create topic if not exists test_tmq_ws_unsubscribe2_topic as select * from ct0")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_unsubscribe", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct1 values('%s',1,2)`, ts2.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_unsubscribe", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into ct2 values('%s',1,2,'3')`, ts3.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_tmq_unsubscribe", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
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
			_, _, value, err := parseblock.ParseTmqBlock(message[8:], tmqFetchResp.FieldsTypes, tmqFetchResp.Rows, tmqFetchResp.Precision)
			if err != nil {
				return err
			}
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
			_, _, value, err := parseblock.ParseTmqBlock(message[8:], tmqFetchResp.FieldsTypes, tmqFetchResp.Rows, tmqFetchResp.Precision)
			if err != nil {
				return err
			}
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
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("drop topic if exists test_tmq_ws_unsubscribe2_topic")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("drop database if exists test_ws_tmq_unsubscribe")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
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
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct0 (ts timestamp, c1 int)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/"+dbName, body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct1 (ts timestamp, c1 int, c2 float)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/"+dbName, body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct2 (ts timestamp, c1 int, c2 float, c3 binary(10))")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/"+dbName, body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	for i := 0; i < insertCount; i++ {
		w = httptest.NewRecorder()
		body = strings.NewReader(insertSql[i])
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql/"+dbName, body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	w = httptest.NewRecorder()
	body = strings.NewReader("create topic if not exists " + topic + " as database " + dbName)
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/"+dbName, body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
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
					_, _, value, err := parseblock.ParseTmqBlock(message[8:], tmqFetchResp.FieldsTypes, tmqFetchResp.Rows, tmqFetchResp.Precision)
					assert.NoError(t, err)
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
					_, _, value, err := parseblock.ParseTmqBlock(message[8:], tmqFetchResp.FieldsTypes, tmqFetchResp.Rows, tmqFetchResp.Precision)
					assert.NoError(t, err)
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
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("drop database if exists " + dbName)
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

func doHttpSql(sql string) (code int, message string) {
	w := httptest.NewRecorder()
	body := strings.NewReader(sql)
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	b, _ := io.ReadAll(w.Body)
	var res WSTMQErrorResp
	_ = json.Unmarshal(b, &res)
	return res.Code, res.Message
}

func doWebSocket(ws *websocket.Conn, action string, arg []byte) (resp []byte, err error) {
	a, _ := json.Marshal(&wstool.WSAction{Action: action, Args: arg})
	message, err := sendWSMessage(ws, websocket.TextMessage, a)
	return message, err
}

func sendWSMessage(ws *websocket.Conn, messageType int, data []byte) (resp []byte, err error) {
	err = ws.WriteMessage(messageType, data)
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

func TestTMQ_PollWithMessageID(t *testing.T) {
	dbName := "test_ws_tmq_poll_with_message_id"
	topic := "test_ws_tmq_poll_with_message_id_topic"

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
	// poll
	messageID := uint64(0)
	b, _ = json.Marshal(TMQPollReq{ReqID: 100, BlockingTime: 500, MessageID: &messageID})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	var pollResp TMQPollResp
	err = json.Unmarshal(msg, &pollResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	assert.True(t, pollResp.HaveMessage, string(msg))
	assert.Equal(t, uint64(100), pollResp.ReqID, string(msg))

	// poll with old messageID
	b, _ = json.Marshal(TMQPollReq{ReqID: 101, BlockingTime: 500, MessageID: &messageID})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	var pollResp2 TMQPollResp
	err = json.Unmarshal(msg, &pollResp2)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	assert.True(t, pollResp2.HaveMessage, string(msg))
	assert.Equal(t, uint64(101), pollResp2.ReqID, string(msg))
	pollResp2.ReqID = 100
	pollResp2.Timing = pollResp.Timing
	assert.Equal(t, pollResp, pollResp2)

	// poll with new messageID
	messageID = pollResp2.MessageID
	b, _ = json.Marshal(TMQPollReq{ReqID: 102, BlockingTime: 500, MessageID: &messageID})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	var pollResp3 TMQPollResp
	err = json.Unmarshal(msg, &pollResp3)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	assert.True(t, pollResp3.HaveMessage, string(msg))
	assert.Equal(t, uint64(102), pollResp3.ReqID, string(msg))
	assert.NotEqual(t, pollResp2.MessageID, pollResp3.MessageID)

	// poll with old messageID
	b, _ = json.Marshal(TMQPollReq{ReqID: 103, BlockingTime: 500, MessageID: &messageID})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	var pollResp4 TMQPollResp
	err = json.Unmarshal(msg, &pollResp4)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	assert.True(t, pollResp4.HaveMessage, string(msg))
	assert.Equal(t, uint64(103), pollResp4.ReqID, string(msg))
	pollResp4.ReqID = pollResp3.ReqID
	pollResp4.Timing = pollResp3.Timing
	assert.Equal(t, pollResp3, pollResp4)
	latestMessageID := pollResp4.MessageID
	// poll until no message
	for {
		b, _ = json.Marshal(TMQPollReq{ReqID: 104, BlockingTime: 500, MessageID: &messageID})
		msg, err = doWebSocket(ws, TMQPoll, b)
		assert.NoError(t, err)
		t.Log(string(msg))
		var pollResp5 TMQPollResp
		err = json.Unmarshal(msg, &pollResp4)
		assert.NoError(t, err)
		assert.Equal(t, 0, pollResp.Code, string(msg))
		if !pollResp5.HaveMessage {
			break
		}
		latestMessageID = pollResp5.MessageID
	}
	// poll with new messageID
	b, _ = json.Marshal(TMQPollReq{ReqID: 105, BlockingTime: 500, MessageID: &latestMessageID})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	var pollResp6 TMQPollResp
	err = json.Unmarshal(msg, &pollResp6)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	assert.False(t, pollResp6.HaveMessage, string(msg))
	// insert data
	code, message := doHttpSql(fmt.Sprintf("insert into %s.ct2 values (now, 1, 2, '3')", dbName))
	assert.Equal(t, 0, code, message)
	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 106, BlockingTime: 1000, MessageID: &latestMessageID})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	var pollResp7 TMQPollResp
	err = json.Unmarshal(msg, &pollResp7)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	assert.True(t, pollResp7.HaveMessage, string(msg))
	assert.NotEqual(t, latestMessageID, pollResp7.MessageID)
	// poll with new messageID
	latestMessageID = pollResp7.MessageID
	b, _ = json.Marshal(TMQPollReq{ReqID: 107, BlockingTime: 500, MessageID: &latestMessageID})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	var pollResp8 TMQPollResp
	err = json.Unmarshal(msg, &pollResp8)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	assert.False(t, pollResp8.HaveMessage, string(msg))

	// commit
	b, _ = json.Marshal(TMQCommitReq{ReqID: 107, MessageID: latestMessageID})
	msg, err = doWebSocket(ws, TMQCommit, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	var commitResp TMQCommitResp
	err = json.Unmarshal(msg, &commitResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, commitResp.Code, string(msg))

	// commit all
	b, _ = json.Marshal(TMQCommitReq{ReqID: 108})
	msg, err = doWebSocket(ws, TMQCommit, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	err = json.Unmarshal(msg, &commitResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, commitResp.Code, string(msg))

	// unsubscribe
	b, _ = json.Marshal(TMQUnsubscribeReq{ReqID: 109})
	msg, err = doWebSocket(ws, TMQUnsubscribe, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	var unsubscribeResp TMQUnsubscribeResp
	err = json.Unmarshal(msg, &unsubscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, unsubscribeResp.Code, string(msg))

	// subscribe
	b, _ = json.Marshal(TMQSubscribeReq{
		Topics: []string{topic},
	})
	msg, err = doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, string(msg))

	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 107, BlockingTime: 500, MessageID: &latestMessageID})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	var pollResp9 TMQPollResp
	err = json.Unmarshal(msg, &pollResp9)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	assert.False(t, pollResp9.HaveMessage, string(msg))
	// insert data
	code, message = doHttpSql(fmt.Sprintf("insert into %s.ct2 values (now, 1, 2, '3')", dbName))
	assert.Equal(t, 0, code, message)
	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 107, BlockingTime: 10000, MessageID: &latestMessageID})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	var pollResp10 TMQPollResp
	err = json.Unmarshal(msg, &pollResp10)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp10.Code, string(msg))
	assert.True(t, pollResp10.HaveMessage, string(msg))
	assert.Greater(t, pollResp10.MessageID, latestMessageID)
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

func prepareAllType(t *testing.T, dbName string, topic string) {
	doHttpSql(fmt.Sprintf("drop topic if exists %s", topic))
	doHttpSql(fmt.Sprintf("drop database if exists %s", dbName))
	code, message := doHttpSql(fmt.Sprintf("create database if not exists %s WAL_RETENTION_PERIOD 86400", dbName))
	assert.Equal(t, 0, code, message)

	code, message = doHttpSql(fmt.Sprintf("create table %s.stb (ts timestamp,"+
		"c1 bool,"+
		"c2 tinyint,"+
		"c3 smallint,"+
		"c4 int,"+
		"c5 bigint,"+
		"c6 tinyint unsigned,"+
		"c7 smallint unsigned,"+
		"c8 int unsigned,"+
		"c9 bigint unsigned,"+
		"c10 float,"+
		"c11 double,"+
		"c12 binary(20),"+
		"c13 nchar(20),"+
		"c14 varbinary(20),"+
		"c15 geometry(100),"+
		"c16 decimal(20,4)"+
		")"+
		"tags(tts timestamp,"+
		"tc1 bool,"+
		"tc2 tinyint,"+
		"tc3 smallint,"+
		"tc4 int,"+
		"tc5 bigint,"+
		"tc6 tinyint unsigned,"+
		"tc7 smallint unsigned,"+
		"tc8 int unsigned,"+
		"tc9 bigint unsigned,"+
		"tc10 float,"+
		"tc11 double,"+
		"tc12 binary(20),"+
		"tc13 nchar(20),"+
		"tc14 varbinary(20),"+
		"tc15 geometry(100)"+
		")", dbName))
	assert.Equal(t, 0, code, message)

	now := time.Now().Round(time.Millisecond).UTC()
	nowStr := now.Format(time.RFC3339Nano)
	code, message = doHttpSql(fmt.Sprintf("create table %s.ctb using %s.stb tags('%s', true,1,1,1,1,1,1,1,1,1,1,'tg','ntg','\\xaabbcc','point(100 100)')", dbName, dbName, nowStr))
	if code != 0 {
		t.Fatalf("insert failed: %s", message)
	}
	code, message = doHttpSql(fmt.Sprintf("insert into %s.ctb values('%s',true,1,1,1,1,1,1,1,1,1,1,'vl','nvl','\\xaabbcc','point(100 100)',123456789.123)", dbName, nowStr))
	if code != 0 {
		t.Fatalf("insert failed: %s", message)
	}
	code, message = doHttpSql(fmt.Sprintf("create topic if not exists %s as database %s", topic, dbName))
	assert.Equal(t, 0, code, message)
}

func afterAllType(t *testing.T, ws *websocket.Conn, dbName string, topic string) error {
	b, _ := json.Marshal(TMQUnsubscribeReq{ReqID: 0})
	_, _ = doWebSocket(ws, TMQUnsubscribe, b)
	err := ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	if err != nil {
		return err
	}
	for i := 0; i < 5; i++ {
		time.Sleep(time.Second * 5)
		code, message := doHttpSql(fmt.Sprintf("drop topic if exists %s", topic))
		if code != 0 {
			t.Log("drop topic failed", message)
			continue
		}
		doHttpSql(fmt.Sprintf("drop database if exists %s", dbName))
		if code != 0 {
			t.Log("drop database failed", message)
			continue
		}
	}
	return nil
}

func TestTMQ_FetchRawNew(t *testing.T) {
	dbName := "test_ws_tmq_fetch_raw_new"
	topic := "test_ws_tmq_fetch_raw_new_topic"
	prepareAllType(t, dbName, topic)

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
		err = afterAllType(t, ws, dbName, topic)
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
		OffsetReset: "latest",
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
	now := time.Now().Round(time.Millisecond)
	nowStr := now.Format(time.RFC3339Nano)
	code, message := doHttpSql(fmt.Sprintf("insert into %s.ctb values('%s',true,1,1,1,1,1,1,1,1,1,1,'vl','nvl','\\xaabbcc','point(100 100)',123456789.123)", dbName, nowStr))
	assert.Equal(t, 0, code, message)

	// poll
	gotMessage := false
	for i := 0; i < 5; i++ {
		b, _ = json.Marshal(TMQPollReq{ReqID: 0, BlockingTime: 500})
		msg, err = doWebSocket(ws, TMQPoll, b)
		assert.NoError(t, err)
		err = json.Unmarshal(msg, &pollResp)
		assert.NoError(t, err)
		assert.Equal(t, 0, pollResp.Code, string(msg))
		if pollResp.HaveMessage {
			gotMessage = true
			assert.True(t, pollResp.Offset >= 0, string(msg))
			break
		}
	}
	if !assert.True(t, gotMessage) {
		return
	}

	// fetch raw new
	b, _ = json.Marshal(TMQFetchRawReq{ReqID: 100, MessageID: pollResp.MessageID})
	msg, err = doWebSocket(ws, TMQFetchRawData, b)
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
		data, err := parser.ReadBlockSimple(info.RawBlock, info.Precision)
		assert.NoError(t, err)
		for i, schema := range info.Schema {
			t.Log(schema.Name, schema.ColType, schema.Flag, schema.Bytes, schema.ColID)
			assert.Equal(t, i+1, schema.ColID)
		}
		expect := [][]driver.Value{
			{
				now,
				true,
				int8(1),
				int16(1),
				int32(1),
				int64(1),
				uint8(1),
				uint16(1),
				uint32(1),
				uint64(1),
				float32(1),
				float64(1),
				"vl",
				"nvl",
				[]byte{0xaa, 0xbb, 0xcc},
				[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
				"123456789.1230",
			},
		}
		assert.Equal(t, expect, data)
		v, err := json.Marshal(data)
		assert.NoError(t, err)
		t.Log(string(v))
	}

	// fetch wrong
	b, _ = json.Marshal(TMQFetchRawReq{ReqID: 100, MessageID: 8000})
	msg, err = doWebSocket(ws, TMQFetchRawData, b)
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

// todo: not implemented
//func TestDropUser(t *testing.T) {
//	defer doHttpSql("drop user test_tmq_drop_user")
//	code, message := doHttpSql("create user test_tmq_drop_user pass 'pass_123'")
//	assert.Equal(t, 0, code, message)
//
//	dbName := "test_ws_tmq_drop_user"
//	topic := "test_ws_tmq_drop_user_topic"
//
//	before(t, dbName, topic)
//
//	s := httptest.NewServer(router)
//	defer s.Close()
//	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	defer func() {
//		err = ws.Close()
//		assert.NoError(t, err)
//	}()
//
//	defer func() {
//		err = after(ws, dbName, topic)
//		assert.NoError(t, err)
//	}()
//
//	// subscribe
//	b, _ := json.Marshal(TMQSubscribeReq{
//		User:        "test_tmq_drop_user",
//		Password:    "pass_123",
//		DB:          dbName,
//		GroupID:     "test",
//		Topics:      []string{topic},
//		AutoCommit:  "false",
//		OffsetReset: "earliest",
//	})
//	msg, err := doWebSocket(ws, TMQSubscribe, b)
//	assert.NoError(t, err)
//	var subscribeResp TMQSubscribeResp
//	err = json.Unmarshal(msg, &subscribeResp)
//	assert.NoError(t, err)
//	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)
//	// drop user
//	code, message = doHttpSql("drop user test_tmq_drop_user")
//	assert.Equal(t, 0, code, message)
//	time.Sleep(time.Second * 3)
//	resp, err := doWebSocket(ws, wstool.ClientVersion, nil)
//	assert.Error(t, err, string(resp))
//}

//type httpQueryResp struct {
//	Code       int              `json:"code,omitempty"`
//	Desc       string           `json:"desc,omitempty"`
//	ColumnMeta [][]driver.Value `json:"column_meta,omitempty"`
//	Data       [][]driver.Value `json:"data,omitempty"`
//	Rows       int              `json:"rows,omitempty"`
//}
//
//func restQuery(sql string, db string) *httpQueryResp {
//	w := httptest.NewRecorder()
//	body := strings.NewReader(sql)
//	url := "/rest/sql"
//	if db != "" {
//		url = fmt.Sprintf("/rest/sql/%s", db)
//	}
//	req, _ := http.NewRequest(http.MethodPost, url, body)
//	req.RemoteAddr = testtools.GetRandomRemoteAddr()
//	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
//	router.ServeHTTP(w, req)
//	if w.Code != http.StatusOK {
//		return &httpQueryResp{
//			Code: w.Code,
//			Desc: w.Body.String(),
//		}
//	}
//	b, _ := io.ReadAll(w.Body)
//	var res httpQueryResp
//	_ = json.Unmarshal(b, &res)
//	return &res
//}

func TestConnectionOptions(t *testing.T) {
	dbName := "test_ws_tmq_conn_options"
	topic := "test_ws_tmq_conn_options_topic"

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
		User:             "root",
		Password:         "taosdata",
		DB:               dbName,
		GroupID:          "test",
		Topics:           []string{topic},
		AutoCommit:       "false",
		OffsetReset:      "earliest",
		SessionTimeoutMS: "100000",
		App:              "tmq_test_conn_protocol",
		IP:               "192.168.55.55",
		TZ:               "Asia/Shanghai",
		Connector:        "tmq_test_connector_info",
	})
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)

	// todo: check connection options,  C not implemented
	//got := false
	//for i := 0; i < 10; i++ {
	//	queryResp := restQuery("select conn_id from performance_schema.perf_connections where user_app = 'tmq_test_conn_protocol' and user_ip = '192.168.55.55'", "")
	//	if queryResp.Code == 0 && len(queryResp.Data) > 0 {
	//		got = true
	//		break
	//	}
	//	time.Sleep(time.Second)
	//}
	//assert.True(t, got)
}

func TestWrongPass(t *testing.T) {
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
	// subscribe
	b, _ := json.Marshal(TMQSubscribeReq{
		User:             "root",
		Password:         "wrong_pass",
		GroupID:          "test",
		Topics:           []string{"test"},
		AutoCommit:       "false",
		OffsetReset:      "earliest",
		SessionTimeoutMS: "100000",
		App:              "tmq_test_conn_protocol",
		IP:               "192.168.55.55",
		TZ:               "Asia/Shanghai",
		Connector:        "tmq_test_connector_info",
	})
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.NotEqual(t, 0, subscribeResp.Code, subscribeResp.Message)
}

func TestPollError(t *testing.T) {
	dbName := "test_ws_tmq_poll_error"
	topic := "test_ws_tmq_poll_error_topic"

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
		User:              "root",
		Password:          "taosdata",
		DB:                dbName,
		GroupID:           "test",
		Topics:            []string{topic},
		AutoCommit:        "false",
		OffsetReset:       "earliest",
		SessionTimeoutMS:  "10000",
		MaxPollIntervalMS: "1000",
	})
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)

	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 100, BlockingTime: 500})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	var pollResp TMQPollResp
	err = json.Unmarshal(msg, &pollResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	for {
		// poll until no message and no error
		b, _ = json.Marshal(TMQPollReq{ReqID: 101, BlockingTime: 500})
		msg, err = doWebSocket(ws, TMQPoll, b)
		assert.NoError(t, err)
		err = json.Unmarshal(msg, &pollResp)
		assert.NoError(t, err)
		if pollResp.Code != 0 {
			t.Errorf("poll error: %s", pollResp.Message)
			return
		}
		if !pollResp.HaveMessage {
			break
		}
	}
	t.Log("sleep 5s to wait for timeout")
	// sleep
	time.Sleep(time.Second * 5)
	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 102, BlockingTime: 500})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	err = json.Unmarshal(msg, &pollResp)
	assert.NoError(t, err)
	assert.NotEqual(t, 0, pollResp.Code, string(msg))
}

func TestConsumeRawdata(t *testing.T) {
	code, message := doHttpSql("create database if not exists test_ws_rawdata WAL_RETENTION_PERIOD 86400")
	if code != 0 {
		t.Fatalf("create database failed: %s", message)
	}
	code, message = doHttpSql("create topic if not exists test_tmq_rawdata_ws_topic with meta as DATABASE test_ws_rawdata")
	if code != 0 {
		t.Fatalf("create topic failed: %s", message)
	}

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
	init := &TMQSubscribeReq{
		ReqID:                0,
		User:                 "root",
		Password:             "taosdata",
		GroupID:              "test",
		Topics:               []string{"test_tmq_rawdata_ws_topic"},
		AutoCommit:           "true",
		AutoCommitIntervalMS: "5000",
		SnapshotEnable:       "true",
		WithTableName:        "true",
		OffsetReset:          "earliest",
		EnableBatchMeta:      "1",
		SessionTimeoutMS:     "12000",
		MaxPollIntervalMS:    "300000",
		MsgConsumeRawdata:    "1",
	}
	b, _ := json.Marshal(init)
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)
	code, message = doHttpSql("create table test_ws_rawdata.stb (ts timestamp," +
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
		"c13 nchar(20)," +
		"c14 varbinary(20)," +
		"c15 geometry(100)," +
		"c16 decimal(20,4)" +
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
		"tc13 nchar(20)," +
		"tc14 varbinary(20)," +
		"tc15 geometry(100)" +
		")")
	if code != 0 {
		t.Fatalf("create table failed: %s", message)
	}
	now := time.Now().Round(time.Millisecond).UTC()
	nowStr := now.Format(time.RFC3339Nano)
	code, message = doHttpSql(fmt.Sprintf("create table test_ws_rawdata.ctb using test_ws_rawdata.stb tags('%s', true,1,1,1,1,1,1,1,1,1,1,'tg','ntg','\\xaabbcc','point(100 100)')", nowStr))
	if code != 0 {
		t.Fatalf("insert failed: %s", message)
	}
	code, message = doHttpSql(fmt.Sprintf("insert into test_ws_rawdata.ctb values('%s',true,1,1,1,1,1,1,1,1,1,1,'vl','nvl','\\xaabbcc','point(100 100)',123456789.123)", nowStr))
	if code != 0 {
		t.Fatalf("insert failed: %s", message)
	}
	gotRawMessage := false
	for i := 0; i < 5; i++ {
		b, _ = json.Marshal(&TMQPollReq{
			ReqID:        3,
			BlockingTime: 500,
		})
		msg, err = doWebSocket(ws, TMQPoll, b)
		assert.NoError(t, err)
		var pollResp TMQPollResp
		err = json.Unmarshal(msg, &pollResp)
		assert.NoError(t, err)
		assert.Equal(t, 0, pollResp.Code, string(msg))
		if pollResp.HaveMessage {
			if pollResp.MessageType == common.TMQ_RES_RAWDATA {
				gotRawMessage = true
				// can not call fetch
				b, _ = json.Marshal(TMQFetchReq{ReqID: 101, MessageID: pollResp.MessageID})
				msg, err = doWebSocket(ws, TMQFetch, b)
				assert.NoError(t, err)
				var fetchResp TMQFetchResp
				err = json.Unmarshal(msg, &fetchResp)
				assert.NoError(t, err)
				assert.Equal(t, uint64(101), fetchResp.ReqID, fetchResp)
				assert.NotEqual(t, 0, fetchResp.Code, fetchResp)
				// can not call fetch_block
				b, _ = json.Marshal(TMQFetchBlockReq{ReqID: 102, MessageID: pollResp.MessageID})
				msg, err = doWebSocket(ws, TMQFetchBlock, b)
				assert.NoError(t, err)
				var fetchBlockResp WSTMQErrorResp
				err = json.Unmarshal(msg, &fetchBlockResp)
				assert.NoError(t, err)
				assert.Equal(t, uint64(102), fetchBlockResp.ReqID, fetchResp)
				assert.NotEqual(t, 0, fetchBlockResp.Code, fetchBlockResp)
				// can not call fetch_json_meta
				b, _ = json.Marshal(TMQFetchJsonMetaReq{ReqID: 103, MessageID: pollResp.MessageID})
				msg, err = doWebSocket(ws, TMQFetchJsonMeta, b)
				assert.NoError(t, err)
				var fetchJsonMetaResp TMQFetchJsonMetaResp
				err = json.Unmarshal(msg, &fetchJsonMetaResp)
				assert.NoError(t, err)
				assert.Equal(t, uint64(103), fetchJsonMetaResp.ReqID, fetchJsonMetaResp)
				assert.NotEqual(t, 0, fetchJsonMetaResp.Code, fetchJsonMetaResp)
			}
			b, _ = json.Marshal(TMQFetchRawReq{ReqID: 100, MessageID: pollResp.MessageID})
			msg, err = doWebSocket(ws, TMQFetchRawData, b)
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

			writeMsg := make([]byte, 30+resp.RawBlockLength)
			binary.LittleEndian.PutUint64(writeMsg, resp.ReqID)
			binary.LittleEndian.PutUint64(writeMsg[8:], resp.MessageID)
			binary.LittleEndian.PutUint64(writeMsg[16:], TMQRawMessage)
			binary.LittleEndian.PutUint32(writeMsg[24:], resp.RawBlockLength)
			binary.LittleEndian.PutUint16(writeMsg[28:], resp.MetaType)
			copy(writeMsg[30:], resp.TMQRawBlock)
			writeConsumeRawdata(t, writeMsg)
		}
	}
	if !assert.True(t, gotRawMessage) {
		return
	}
	b, _ = json.Marshal(&TMQUnsubscribeReq{
		ReqID: 6,
	})
	msg, err = doWebSocket(ws, TMQUnsubscribe, b)
	assert.NoError(t, err)
	var unsubscribeResp TMQUnsubscribeResp
	err = json.Unmarshal(msg, &unsubscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, unsubscribeResp.Code, unsubscribeResp.Message)

	err = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.NoError(t, err)
	time.Sleep(time.Second * 5)

	w := httptest.NewRecorder()
	body := strings.NewReader("describe stb")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql/test_ws_rawdata_target", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
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
		{"c14", "VARBINARY", float64(20), ""},
		{"c15", "GEOMETRY", float64(100), ""},
		{"c16", "DECIMAL(20, 4)", float64(16), ""},
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
		{"tc14", "VARBINARY", float64(20), "TAG"},
		{"tc15", "GEOMETRY", float64(100), "TAG"},
	}
	for index, values := range expect {
		for i := 0; i < 4; i++ {
			assert.Equal(t, values[i], resp.Data[index][i])
		}
	}

	w = httptest.NewRecorder()
	body = strings.NewReader("select * from stb limit 1")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_rawdata_target", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	err = jsoniter.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	expect = [][]driver.Value{
		{
			now.Format(layout.LayoutMillSecond),
			true,
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			"vl",
			"nvl",
			"aabbcc",
			"010100000000000000000059400000000000005940",
			"123456789.1230",
			now.Format(layout.LayoutMillSecond),
			true,
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			"tg",
			"ntg",
			"aabbcc",
			"010100000000000000000059400000000000005940",
		},
	}
	assert.Equal(t, expect, resp.Data)

	for i := 0; i < 5; i++ {
		time.Sleep(time.Second * 3)
		code, message := doHttpSql("drop topic if exists test_tmq_rawdata_ws_topic")
		if code != 0 {
			t.Log(message)
			continue
		}
		code, message = doHttpSql("drop database if exists test_ws_rawdata_target")
		if code != 0 {
			t.Log(message)
			continue
		}
		code, message = doHttpSql("drop database if exists test_ws_rawdata")
		if code != 0 {
			t.Log(message)
			continue
		}
		break
	}
}

func writeConsumeRawdata(t *testing.T, rawData []byte) {
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists test_ws_rawdata_target WAL_RETENTION_PERIOD 86400")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
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
			err = ws.WriteMessage(websocket.BinaryMessage, rawData)
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
		DB:       "test_ws_rawdata_target",
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

func TestSetConfig(t *testing.T) {
	code, message := doHttpSql("create database if not exists test_ws_tmq_set_conf WAL_RETENTION_PERIOD 86400")
	if code != 0 {
		t.Fatalf("create database failed: %s", message)
	}
	code, message = doHttpSql("create topic if not exists test_ws_tmq_set_conf_topic with meta as DATABASE test_ws_tmq_set_conf")
	if code != 0 {
		t.Fatalf("create topic failed: %s", message)
	}
	defer func() {
		for i := 0; i < 5; i++ {
			time.Sleep(time.Second * 3)
			code, message := doHttpSql("drop topic if exists test_ws_tmq_set_conf_topic")
			if code != 0 {
				t.Log(message)
				continue
			}
			code, message = doHttpSql("drop database if exists test_ws_tmq_set_conf")
			if code != 0 {
				t.Log(message)
				continue
			}
			break
		}
	}()
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
	initConfig := &TMQSubscribeReq{
		ReqID:                0,
		User:                 "root",
		Password:             "taosdata",
		GroupID:              "test",
		Topics:               []string{"test_ws_tmq_set_conf_topic"},
		AutoCommit:           "true",
		AutoCommitIntervalMS: "5000",
		SnapshotEnable:       "true",
		WithTableName:        "true",
		OffsetReset:          "earliest",
		EnableBatchMeta:      "1",
		SessionTimeoutMS:     "12000",
		MaxPollIntervalMS:    "300000",
		MsgConsumeRawdata:    "1",
		Config: map[string]string{
			"td.connect.user":         "wrong_user",
			"td.connect.pass":         "wrong_pass",
			"td.connect.ip":           "localhost",
			"td.connect.port":         "6030",
			"group.id":                "test_conf",
			"client.id":               "test_conf_client",
			"auto.offset.reset":       "latest",
			"enable.auto.commit":      "true",
			"auto.commit.interval.ms": "5000",
			"msg.with.table.name":     "true",
			"session.timeout.ms":      "10000",
			"max.poll.interval.ms":    "300000",
		},
	}
	b, _ := json.Marshal(initConfig)
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)

	// unsubscribe
	b, _ = json.Marshal(&TMQUnsubscribeReq{
		ReqID: 6,
	})
	msg, err = doWebSocket(ws, TMQUnsubscribe, b)
	assert.NoError(t, err)
	var unsubscribeResp TMQUnsubscribeResp
	err = json.Unmarshal(msg, &unsubscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, unsubscribeResp.Code, unsubscribeResp.Message)
	err = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.NoError(t, err)

	// unknown config key
	ws2, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws2.Close()
		assert.NoError(t, err)
	}()
	initConfig.Config["wrong_config"] = "wrong"
	b, _ = json.Marshal(initConfig)
	msg, err = doWebSocket(ws2, TMQSubscribe, b)
	assert.NoError(t, err)
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, TsdbCodeInvalidPara, subscribeResp.Code, subscribeResp.Message)

	// unknown value
	delete(initConfig.Config, "wrong_config")
	initConfig.Config["session.timeout.ms"] = "abcd"
	b, _ = json.Marshal(initConfig)
	msg, err = doWebSocket(ws2, TMQSubscribe, b)
	assert.NoError(t, err)
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, TsdbCodeInvalidPara, subscribeResp.Code, subscribeResp.Message)

	err = ws2.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.NoError(t, err)

}

func TestTMQPollReq_String(t *testing.T) {
	type fields struct {
		ReqID        uint64
		BlockingTime int64
		MessageID    *uint64
		ctx          context.Context
	}
	messageID := uint64(1)
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "no messageid",
			fields: fields{
				ReqID:        1,
				BlockingTime: 500,
				MessageID:    nil,
				ctx:          nil,
			},
			want: "&{ReqID:1 BlockingTime:500 MessageID:nil}",
		},
		{
			name: "normal",
			fields: fields{
				ReqID:        1,
				BlockingTime: 500,
				MessageID:    &messageID,
				ctx:          nil,
			},
			want: "&{ReqID:1 BlockingTime:500 MessageID:1}",
		},
		{
			name: "with context",
			fields: fields{
				ReqID:        1,
				BlockingTime: 500,
				MessageID:    &messageID,
				ctx:          context.Background(),
			},
			want: "&{ReqID:1 BlockingTime:500 MessageID:1}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &TMQPollReq{
				ReqID:        tt.fields.ReqID,
				BlockingTime: tt.fields.BlockingTime,
				MessageID:    tt.fields.MessageID,
				ctx:          tt.fields.ctx,
			}
			assert.Equalf(t, tt.want, req.String(), "String()")
		})
	}
}

func TestVersion(t *testing.T) {
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
	req := &versionRequest{
		ReqID: 0x123654,
	}
	bs, err := json.Marshal(req)
	assert.NoError(t, err)
	msg, err := doWebSocket(ws, wstool.ClientVersion, bs)
	assert.NoError(t, err)
	var versionResp versionResponse
	err = json.Unmarshal(msg, &versionResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, versionResp.Code, string(msg))
	assert.Equal(t, version.TaosClientVersion, versionResp.Version)
	assert.Equal(t, req.ReqID, versionResp.ReqID, string(msg))
	req2 := []byte(`{"action":"version"}`)
	msg, err = sendWSMessage(ws, websocket.TextMessage, req2)
	assert.NoError(t, err)
	err = json.Unmarshal(msg, &versionResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, versionResp.Code, string(msg))
	assert.Equal(t, version.TaosClientVersion, versionResp.Version)
	assert.Equal(t, uint64(0), versionResp.ReqID, string(msg))
}

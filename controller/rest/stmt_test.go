package rest

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
)

func TestSTMT(t *testing.T) {
	now := time.Now()
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists test_ws_stmt")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("drop table if exists ct")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_stmt", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct(ts timestamp," +
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
		")")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_stmt", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/stmt", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer ws.Close()
	const (
		AfterConnect = iota + 1
		AfterInit
		AfterPrepare
		AfterSetTableName
		AfterSetTags
		AfterBind
		AfterAddBatch
		AfterExec
	)
	status := 0
	finish := make(chan struct{})
	stmtID := uint64(0)
	testMessageHandler := func(messageType int, message []byte) error {
		t.Log(messageType, string(message))
		switch status {
		case AfterConnect:
			var d WSConnectResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", STMTConnect, d.Code, d.Message)
			}
			//init
			status = AfterInit
			b, _ := json.Marshal(&StmtInitReq{
				ReqID: 2,
			})
			action, _ := json.Marshal(&WSAction{
				Action: STMTInit,
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
		case AfterInit:
			var d StmtInitResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", STMTInit, d.Code, d.Message)
			}
			stmtID = d.StmtID
			status = AfterPrepare
			//prepare
			b, _ := json.Marshal(&StmtPrepareReq{
				ReqID:  3,
				StmtID: stmtID,
				SQL:    "insert into ? values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
			})
			action, _ := json.Marshal(&WSAction{
				Action: STMTPrepare,
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
		case AfterPrepare:
			var d StmtPrepareResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", STMTPrepare, d.Code, d.Message)
			}
			status = AfterSetTableName
			b, _ := json.Marshal(&StmtSetTableNameReq{
				ReqID:  4,
				StmtID: stmtID,
				Name:   "test_ws_stmt.ct",
			})
			action, _ := json.Marshal(&WSAction{
				Action: STMTSetTableName,
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
		case AfterSetTableName:
			var d StmtSetTableNameResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", STMTSetTableName, d.Code, d.Message)
			}
			status = AfterBind
			b, _ := json.Marshal(&StmtBindReq{
				ReqID:  5,
				StmtID: stmtID,
				Columns: [][]driver.Value{
					{
						now,
						now.Add(time.Second),
						now.Add(time.Second * 2),
					},
					{
						true,
						false,
						nil,
					},
					{
						2,
						22,
						nil,
					},
					{
						3,
						33,
						nil,
					},
					{
						4,
						44,
						nil,
					},
					{
						5,
						55,
						nil,
					},
					{
						6,
						66,
						nil,
					},
					{
						7,
						77,
						nil,
					},
					{
						8,
						88,
						nil,
					},
					{
						9,
						99,
						nil,
					},
					{
						10,
						1010,
						nil,
					},
					{
						11,
						1111,
						nil,
					},
					{
						"binary",
						"binary2",
						nil,
					},
					{
						"nchar",
						"nchar2",
						nil,
					},
				},
			})
			action, _ := json.Marshal(&WSAction{
				Action: STMTBind,
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
		case AfterBind:
			var d StmtSetTableNameResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", STMTBind, d.Code, d.Message)
			}
			status = AfterAddBatch
			b, _ := json.Marshal(&StmtAddBatchReq{
				ReqID:  6,
				StmtID: stmtID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: STMTAddBatch,
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
		case AfterAddBatch:
			var d StmtSetTableNameResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", STMTAddBatch, d.Code, d.Message)
			}
			status = AfterExec
			b, _ := json.Marshal(&StmtExecReq{
				ReqID:  7,
				StmtID: stmtID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: STMTExec,
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
		case AfterExec:
			var d StmtSetTableNameResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", STMTExec, d.Code, d.Message)
			}
			b, _ := json.Marshal(&StmtClose{
				ReqID:  8,
				StmtID: stmtID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: STMTClose,
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
			time.Sleep(time.Second)
			finish <- struct{}{}
		}
		return nil
	}
	ws.UnderlyingConn()
	go func() {
		for {
			mt, message, err := ws.ReadMessage()
			t.Log(string(message))
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

	connect := &StmtConnectReq{
		ReqID:    0,
		User:     "root",
		Password: "taosdata",
	}

	b, _ := json.Marshal(connect)
	action, _ := json.Marshal(&WSAction{
		Action: STMTConnect,
		Args:   b,
	})
	status = AfterConnect
	t.Log(string(action))
	err = ws.WriteMessage(
		websocket.TextMessage,
		action,
	)
	if err != nil {
		t.Error(err)
		return
	}
	<-finish

}

func TestBlock(t *testing.T) {
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists test_ws_stmt")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("drop table if exists ct")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_stmt", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists ct(ts timestamp," +
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
		")")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_stmt", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	//p0 uin64 代表 req_id
	//p0+8 uint64 代表 stmt_id
	//p0+16 uint64 代表 类型(1 set tag 2 bind)
	//p0+24 uint64 代表 列数
	//p0+32 uint64 代表 行数
	//p0+40 raw block
	rawBlock := []byte{150, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 0, 8, 0, 0, 0, 1, 0, 1, 0, 0, 0, 2, 0, 1, 0, 0, 0, 3, 0, 2, 0, 0, 0, 4, 0, 4, 0, 0, 0, 5, 0, 8, 0, 0, 0, 11, 0, 1, 0, 0, 0, 12, 0, 2, 0, 0, 0, 13, 0, 4, 0, 0, 0, 14, 0, 8, 0, 0, 0, 6, 0, 4, 0, 0, 0, 7, 0, 8, 0, 0, 0, 8, 0, 22, 0, 0, 0, 10, 0, 82, 0, 0, 0, 24, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 6, 0, 0, 0, 12, 0, 0, 0, 24, 0, 0, 0, 3, 0, 0, 0, 6, 0, 0, 0, 12, 0, 0, 0, 24, 0, 0, 0, 12, 0, 0, 0, 24, 0, 0, 0, 17, 0, 0, 0, 48, 0, 0, 0, 0, 142, 23, 228, 90, 129, 1, 0, 0, 118, 27, 228, 90, 129, 1, 0, 0, 94, 31, 228, 90, 129, 1, 0, 0, 32, 1, 0, 0, 32, 2, 22, 0, 32, 3, 0, 33, 0, 0, 0, 32, 4, 0, 0, 0, 44, 0, 0, 0, 0, 0, 0, 0, 32, 5, 0, 0, 0, 0, 0, 0, 0, 55, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 6, 66, 0, 32, 7, 0, 77, 0, 0, 0, 32, 8, 0, 0, 0, 88, 0, 0, 0, 0, 0, 0, 0, 32, 9, 0, 0, 0, 0, 0, 0, 0, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 32, 65, 0, 128, 124, 68, 0, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 38, 64, 0, 0, 0, 0, 0, 92, 145, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 255, 255, 255, 255, 6, 0, 98, 105, 110, 97, 114, 121, 7, 0, 98, 105, 110, 97, 114, 121, 50, 0, 0, 0, 0, 22, 0, 0, 0, 255, 255, 255, 255, 20, 0, 110, 0, 0, 0, 99, 0, 0, 0, 104, 0, 0, 0, 97, 0, 0, 0, 114, 0, 0, 0, 24, 0, 110, 0, 0, 0, 99, 0, 0, 0, 104, 0, 0, 0, 97, 0, 0, 0, 114, 0, 0, 0, 50, 0, 0, 0}
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/stmt", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer ws.Close()
	const (
		AfterConnect = iota + 1
		AfterInit
		AfterPrepare
		AfterSetTableName
		AfterSetTags
		AfterBind
		AfterAddBatch
		AfterExec
	)
	status := 0
	finish := make(chan struct{})
	stmtID := uint64(0)
	testMessageHandler := func(messageType int, message []byte) error {
		//json
		switch status {
		case AfterConnect:
			var d WSConnectResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", STMTConnect, d.Code, d.Message)
			}
			//init
			status = AfterInit
			b, _ := json.Marshal(&StmtInitReq{
				ReqID: 2,
			})
			action, _ := json.Marshal(&WSAction{
				Action: STMTInit,
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
		case AfterInit:
			var d StmtInitResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", STMTInit, d.Code, d.Message)
			}
			stmtID = d.StmtID
			status = AfterPrepare
			//prepare
			b, _ := json.Marshal(&StmtPrepareReq{
				ReqID:  3,
				StmtID: stmtID,
				SQL:    "insert into ? values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
			})
			action, _ := json.Marshal(&WSAction{
				Action: STMTPrepare,
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
		case AfterPrepare:
			var d StmtPrepareResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", STMTPrepare, d.Code, d.Message)
			}
			status = AfterSetTableName
			b, _ := json.Marshal(&StmtSetTableNameReq{
				ReqID:  4,
				StmtID: stmtID,
				Name:   "test_ws_stmt.ct",
			})
			action, _ := json.Marshal(&WSAction{
				Action: STMTSetTableName,
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
		case AfterSetTableName:
			var d StmtSetTableNameResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", STMTSetTableName, d.Code, d.Message)
			}
			status = AfterBind
			reqID := uint64(10)
			action := uint64(2)
			columns := uint64(14)
			rows := uint64(3)

			block := &bytes.Buffer{}
			writeUint64(block, reqID)
			writeUint64(block, stmtID)
			writeUint64(block, action)
			writeUint64(block, columns)
			writeUint64(block, rows)
			block.Write(rawBlock)
			blockData := block.Bytes()
			t.Log(blockData)
			err = ws.WriteMessage(
				websocket.BinaryMessage,
				block.Bytes(),
			)
			if err != nil {
				return err
			}
		case AfterBind:
			var d StmtSetTableNameResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", STMTBind, d.Code, d.Message)
			}
			status = AfterAddBatch
			b, _ := json.Marshal(&StmtAddBatchReq{
				ReqID:  6,
				StmtID: stmtID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: STMTAddBatch,
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
		case AfterAddBatch:
			var d StmtSetTableNameResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", STMTAddBatch, d.Code, d.Message)
			}
			status = AfterExec
			b, _ := json.Marshal(&StmtExecReq{
				ReqID:  7,
				StmtID: stmtID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: STMTExec,
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
		case AfterExec:
			var d StmtSetTableNameResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", STMTExec, d.Code, d.Message)
			}
			b, _ := json.Marshal(&StmtClose{
				ReqID:  8,
				StmtID: stmtID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: STMTClose,
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
			time.Sleep(time.Second)
			finish <- struct{}{}
		}
		return nil
	}
	go func() {
		for {
			mt, message, err := ws.ReadMessage()
			t.Log(string(message))
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

	connect := &StmtConnectReq{
		ReqID:    0,
		User:     "root",
		Password: "taosdata",
	}

	b, _ := json.Marshal(connect)
	action, _ := json.Marshal(&WSAction{
		Action: STMTConnect,
		Args:   b,
	})
	status = AfterConnect
	t.Log(string(action))
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
	time.Sleep(time.Second)
}

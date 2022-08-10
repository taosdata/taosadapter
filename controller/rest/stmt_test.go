package rest

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
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
	body := strings.NewReader("drop database if exists test_ws_stmt")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("create database if not exists test_ws_stmt precision 'ns'")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists st(ts timestamp," +
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
		") tags (info json)")
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
				SQL:    "insert into ? using test_ws_stmt.st tags (?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
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
				Name:   "test_ws_stmt.ct1",
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

			status = AfterSetTags

			b, _ := json.Marshal(&StmtSetTagsReq{
				ReqID:  5,
				StmtID: stmtID,
				// {"a":"b"}
				Tags: json.RawMessage(`["{\"a\":\"b\"}"]`),
			})
			action, _ := json.Marshal(&WSAction{
				Action: STMTSetTags,
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

		case AfterSetTags:
			var d StmtSetTagsResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", STMTSetTags, d.Code, d.Message)
			}
			status = AfterBind
			c, err := json.Marshal([][]driver.Value{
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
			})
			b, _ := json.Marshal(&StmtBindReq{
				ReqID:   5,
				StmtID:  stmtID,
				Columns: c,
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
	w = httptest.NewRecorder()
	body = strings.NewReader("select * from st")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_stmt", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	resultBody := fmt.Sprintf(`{"code":0,"column_meta":[["ts","TIMESTAMP",8],["c1","BOOL",1],["c2","TINYINT",1],["c3","SMALLINT",2],["c4","INT",4],["c5","BIGINT",8],["c6","TINYINT UNSIGNED",1],["c7","SMALLINT UNSIGNED",2],["c8","INT UNSIGNED",4],["c9","BIGINT UNSIGNED",8],["c10","FLOAT",4],["c11","DOUBLE",8],["c12","VARCHAR",20],["c13","NCHAR",20],["info","JSON",4095]],"data":[["%s",true,2,3,4,5,6,7,8,9,10,11,"binary","nchar",{"a":"b"}],["%s",false,22,33,44,55,66,77,88,99,1010,1111,"binary2","nchar2",{"a":"b"}],["%s",null,null,null,null,null,null,null,null,null,null,null,null,null,{"a":"b"}]],"rows":3}`, now.UTC().Format(time.RFC3339Nano), now.Add(time.Second).UTC().Format(time.RFC3339Nano), now.Add(time.Second*2).UTC().Format(time.RFC3339Nano))
	assert.Equal(t, resultBody, w.Body.String())
	w = httptest.NewRecorder()
	body = strings.NewReader("drop database if exists test_ws_stmt")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

func TestBlock(t *testing.T) {
	w := httptest.NewRecorder()
	body := strings.NewReader("drop database if exists test_ws_stmt")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("create database if not exists test_ws_stmt precision 'ns'")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists stb(ts timestamp," +
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
		") tags(info json)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_stmt", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	//p0 uin64 代表 req_id
	//p0+8 uint64 代表 stmt_id
	//p0+16 uint64 代表 类型(1 set tag 2 bind)
	//p0+24 raw block
	rawBlock := []byte{
		0x01, 0x00, 0x00, 0x00, 0x98, 0x01, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x08, 0x00, 0x00,
		0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x03, 0x02, 0x00, 0x00, 0x00,
		0x04, 0x04, 0x00, 0x00, 0x00, 0x05, 0x08, 0x00, 0x00, 0x00, 0x0b, 0x01, 0x00, 0x00, 0x00, 0x0c,
		0x02, 0x00, 0x00, 0x00, 0x0d, 0x04, 0x00, 0x00, 0x00, 0x0e, 0x08, 0x00, 0x00, 0x00, 0x06, 0x04,
		0x00, 0x00, 0x00, 0x07, 0x08, 0x00, 0x00, 0x00, 0x08, 0x16, 0x00, 0x00, 0x00, 0x0a, 0x52, 0x00,
		0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x06, 0x00,
		0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x06, 0x00,
		0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x18, 0x00,
		0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x00, 0x2c, 0x5b, 0x70, 0x86, 0x82,
		0x01, 0x00, 0x00, 0x14, 0x5f, 0x70, 0x86, 0x82, 0x01, 0x00, 0x00, 0xfc, 0x62, 0x70, 0x86, 0x82,
		0x01, 0x00, 0x00, 0x20, 0x01, 0x00, 0x00, 0x20, 0x02, 0x16, 0x00, 0x20, 0x03, 0x00, 0x21, 0x00,
		0x00, 0x00, 0x20, 0x04, 0x00, 0x00, 0x00, 0x2c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20,
		0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x37, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x06, 0x42, 0x00, 0x20, 0x07, 0x00, 0x4d,
		0x00, 0x00, 0x00, 0x20, 0x08, 0x00, 0x00, 0x00, 0x58, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x20, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x63, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x20, 0x41, 0x00, 0x80,
		0x7c, 0x44, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x26, 0x40, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x5c, 0x91, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x06, 0x00, 0x62, 0x69, 0x6e,
		0x61, 0x72, 0x79, 0x07, 0x00, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x32, 0x00, 0x00, 0x00, 0x00,
		0x16, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x14, 0x00, 0x6e, 0x00, 0x00, 0x00, 0x63, 0x00,
		0x00, 0x00, 0x68, 0x00, 0x00, 0x00, 0x61, 0x00, 0x00, 0x00, 0x72, 0x00, 0x00, 0x00, 0x18, 0x00,
		0x6e, 0x00, 0x00, 0x00, 0x63, 0x00, 0x00, 0x00, 0x68, 0x00, 0x00, 0x00, 0x61, 0x00, 0x00, 0x00,
		0x72, 0x00, 0x00, 0x00, 0x32, 0x00, 0x00, 0x00,
	}
	now := time.Now()
	binary.LittleEndian.PutUint64(rawBlock[155:], uint64(now.UnixNano()))
	binary.LittleEndian.PutUint64(rawBlock[163:], uint64(now.Add(time.Second).UnixNano()))
	binary.LittleEndian.PutUint64(rawBlock[171:], uint64(now.Add(time.Second*2).UnixNano()))
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
		AfterVersion
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
				SQL:    "insert into ? using test_ws_stmt.stb tags (?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
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
				Name:   "test_ws_stmt.ctb",
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
			status = AfterSetTags
			b, _ := json.Marshal(&StmtSetTagsReq{
				ReqID:  5,
				StmtID: stmtID,
				Tags:   json.RawMessage(`["{\"a\":\"b\"}"]`),
			})
			action, _ := json.Marshal(&WSAction{
				Action: STMTSetTags,
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
		case AfterSetTags:
			var d StmtSetTagsResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", STMTSetTags, d.Code, d.Message)
			}
			status = AfterBind
			reqID := uint64(10)
			action := uint64(2)

			block := &bytes.Buffer{}
			writeUint64(block, reqID)
			writeUint64(block, stmtID)
			writeUint64(block, action)
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
		case AfterVersion:
			var d WSVersionResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", ClientVersion, d.Code, d.Message)
			}
			assert.NotEmpty(t, d.Version)
			t.Log("client version", d.Version)

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
			return nil
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
	w = httptest.NewRecorder()
	body = strings.NewReader("select * from stb")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_ws_stmt", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	resultBody := fmt.Sprintf(`{"code":0,"column_meta":[["ts","TIMESTAMP",8],["c1","BOOL",1],["c2","TINYINT",1],["c3","SMALLINT",2],["c4","INT",4],["c5","BIGINT",8],["c6","TINYINT UNSIGNED",1],["c7","SMALLINT UNSIGNED",2],["c8","INT UNSIGNED",4],["c9","BIGINT UNSIGNED",8],["c10","FLOAT",4],["c11","DOUBLE",8],["c12","VARCHAR",20],["c13","NCHAR",20],["info","JSON",4095]],"data":[["%s",true,2,3,4,5,6,7,8,9,10,11,"binary","nchar",{"a":"b"}],["%s",false,22,33,44,55,66,77,88,99,1010,1111,"binary2","nchar2",{"a":"b"}],["%s",null,null,null,null,null,null,null,null,null,null,null,null,null,{"a":"b"}]],"rows":3}`, now.UTC().Format(time.RFC3339Nano), now.Add(time.Second).UTC().Format(time.RFC3339Nano), now.Add(time.Second*2).UTC().Format(time.RFC3339Nano))
	assert.Equal(t, resultBody, w.Body.String())
	w = httptest.NewRecorder()
	body = strings.NewReader("drop database if exists test_ws_stmt")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

}

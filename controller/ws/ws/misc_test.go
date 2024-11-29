package ws

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
)

func TestGetCurrentDB(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	db := "test_current_db"
	code, message := doRestful(fmt.Sprintf("drop database if exists %s", db), "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf("create database if not exists %s", db), "")
	assert.Equal(t, 0, code, message)

	defer doRestful(fmt.Sprintf("drop database if exists %s", db), "")

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	// connect
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata", DB: db}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp commonResp
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// current db
	currentDBReq := map[string]uint64{"req_id": 1}
	resp, err = doWebSocket(ws, WSGetCurrentDB, &currentDBReq)
	assert.NoError(t, err)
	var currentDBResp getCurrentDBResponse
	err = json.Unmarshal(resp, &currentDBResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), currentDBResp.ReqID)
	assert.Equal(t, 0, currentDBResp.Code, currentDBResp.Message)
	assert.Equal(t, db, currentDBResp.DB)
}

func TestGetServerInfo(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	// connect
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp commonResp
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// server info
	serverInfoReq := map[string]uint64{"req_id": 1}
	resp, err = doWebSocket(ws, WSGetServerInfo, &serverInfoReq)
	assert.NoError(t, err)
	var serverInfoResp getServerInfoResponse
	err = json.Unmarshal(resp, &serverInfoResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), serverInfoResp.ReqID)
	assert.Equal(t, 0, serverInfoResp.Code, serverInfoResp.Message)
	t.Log(serverInfoResp.Info)
}

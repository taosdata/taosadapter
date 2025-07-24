package ws

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/version"
)

func TestWSConnect(t *testing.T) {
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

	// wrong password
	connReq := connRequest{ReqID: 1, User: "root", Password: "wrong"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, "Authentication failure", connResp.Message)
	assert.Equal(t, 0x357, connResp.Code, connResp.Message)

	// connect
	connReq = connRequest{ReqID: 1, User: "root", Password: "taosdata"}
	resp, err = doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)
	//duplicate connections
	connReq = connRequest{ReqID: 1, User: "root", Password: "taosdata"}
	resp, err = doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0xffff, connResp.Code)
	assert.Equal(t, "duplicate connections", connResp.Message)
	assert.Equal(t, version.TaosClientVersion, connResp.Version)
}

func TestMode(t *testing.T) {
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

	wrongMode := 999
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata", Mode: &wrongMode}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0xffff, connResp.Code)
	assert.Equal(t, fmt.Sprintf("unexpected mode:%d", wrongMode), connResp.Message)

	//bi
	biMode := 0
	connReq = connRequest{ReqID: 1, User: "root", Password: "taosdata", Mode: &biMode}
	resp, err = doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

}

func TestConnectionOptions(t *testing.T) {
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
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata", IP: "192.168.44.55", App: "ws_test_conn_protocol", TZ: "Asia/Shanghai"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// check connection options
	got := false
	for i := 0; i < 10; i++ {
		queryResp := restQuery("select conn_id from performance_schema.perf_connections where user_app = 'ws_test_conn_protocol' and user_ip = '192.168.44.55'", "")
		if queryResp.Code == 0 && len(queryResp.Data) > 0 {
			got = true
			break
		}
		time.Sleep(time.Second)
	}
	assert.True(t, got)
}

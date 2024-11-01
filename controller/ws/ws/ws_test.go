package ws

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	"github.com/taosdata/driver-go/v3/common/parser"
	"github.com/taosdata/driver-go/v3/common/serializer"
	stmtCommon "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/ws/schemaless"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	_ "github.com/taosdata/taosadapter/v3/controller/rest"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/parseblock"
	"github.com/taosdata/taosadapter/v3/version"
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

type restResp struct {
	Code int    `json:"code"`
	Desc string `json:"desc"`
}

func doRestful(sql string, db string) (code int, message string) {
	w := httptest.NewRecorder()
	body := strings.NewReader(sql)
	url := "/rest/sql"
	if db != "" {
		url = fmt.Sprintf("/rest/sql/%s", db)
	}
	req, _ := http.NewRequest(http.MethodPost, url, body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		return w.Code, w.Body.String()
	}
	b, _ := io.ReadAll(w.Body)
	var res restResp
	_ = json.Unmarshal(b, &res)
	return res.Code, res.Desc
}

type queryResp struct {
	Code       int              `json:"code,omitempty"`
	Desc       string           `json:"desc,omitempty"`
	ColumnMeta [][]driver.Value `json:"column_meta,omitempty"`
	Data       [][]driver.Value `json:"data,omitempty"`
	Rows       int              `json:"rows,omitempty"`
}

func restQuery(sql string, db string) *queryResp {
	w := httptest.NewRecorder()
	body := strings.NewReader(sql)
	url := "/rest/sql"
	if db != "" {
		url = fmt.Sprintf("/rest/sql/%s", db)
	}
	req, _ := http.NewRequest(http.MethodPost, url, body)
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		return &queryResp{
			Code: w.Code,
			Desc: w.Body.String(),
		}
	}
	b, _ := io.ReadAll(w.Body)
	var res queryResp
	_ = json.Unmarshal(b, &res)
	return &res
}

func doWebSocket(ws *websocket.Conn, action string, arg interface{}) (resp []byte, err error) {
	var b []byte
	if arg != nil {
		b, _ = json.Marshal(arg)
	}
	a, _ := json.Marshal(Request{Action: action, Args: b})
	err = ws.WriteMessage(websocket.TextMessage, a)
	if err != nil {
		return nil, err
	}
	_, message, err := ws.ReadMessage()
	return message, err
}

func doWebSocketWithoutResp(ws *websocket.Conn, action string, arg interface{}) error {
	var b []byte
	if arg != nil {
		b, _ = json.Marshal(arg)
	}
	a, _ := json.Marshal(Request{Action: action, Args: b})
	err := ws.WriteMessage(websocket.TextMessage, a)
	if err != nil {
		return err
	}
	return nil
}

func TestVersion(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer ws.Close()
	resp, err := doWebSocket(ws, wstool.ClientVersion, nil)
	assert.NoError(t, err)
	var versionResp VersionResponse
	err = json.Unmarshal(resp, &versionResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, versionResp.Code, versionResp.Message)
	assert.Equal(t, version.TaosClientVersion, versionResp.Version)
}

func TestWsQuery(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful("drop database if exists test_ws_query", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_query", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(
		"create table if not exists stb1 (ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100)) tags (info json)",
		"test_ws_query")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(
		`insert into t1 using stb1 tags ('{\"table\":\"t1\"}') values (now-2s,true,2,3,4,5,6,7,8,9,10,11,'中文\"binary','中文nchar','\xaabbcc','point(100 100)')(now-1s,false,12,13,14,15,16,17,18,19,110,111,'中文\"binary','中文nchar','\xaabbcc','point(100 100)')(now,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)`,
		"test_ws_query")
	assert.Equal(t, 0, code, message)

	code, message = doRestful("create table t2 using stb1 tags('{\"table\":\"t2\"}')", "test_ws_query")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create table t3 using stb1 tags('{\"table\":\"t3\"}')", "test_ws_query")
	assert.Equal(t, 0, code, message)

	defer doRestful("drop database if exists test_ws_query", "")
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err := ws.Close()
		assert.NoError(t, err)
	}()

	// connect
	connReq := ConnRequest{ReqID: 1, User: "root", Password: "taosdata", DB: "test_ws_query"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp BaseResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// query
	queryReq := QueryRequest{ReqID: 2, Sql: "select * from stb1"}
	resp, err = doWebSocket(ws, WSQuery, &queryReq)
	assert.NoError(t, err)
	var queryResp QueryResponse
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), queryResp.ReqID)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// fetch
	fetchReq := FetchRequest{ReqID: 3, ID: queryResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	var fetchResp FetchResponse
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), fetchResp.ReqID)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)
	assert.Equal(t, 3, fetchResp.Rows)

	// fetch block
	fetchBlockReq := FetchBlockRequest{ReqID: 4, ID: queryResp.ID}
	fetchBlockResp, err := doWebSocket(ws, WSFetchBlock, &fetchBlockReq)
	assert.NoError(t, err)
	resultID, blockResult := parseblock.ParseBlock(fetchBlockResp[8:], queryResp.FieldsTypes, fetchResp.Rows, queryResp.Precision)
	assert.Equal(t, uint64(1), resultID)
	checkBlockResult(t, blockResult)

	fetchReq = FetchRequest{ReqID: 5, ID: queryResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), fetchResp.ReqID)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)

	assert.Equal(t, true, fetchResp.Completed)

	// write block
	var buffer bytes.Buffer
	wstool.WriteUint64(&buffer, 300)                     // req id
	wstool.WriteUint64(&buffer, 400)                     // message id
	wstool.WriteUint64(&buffer, uint64(RawBlockMessage)) // action
	wstool.WriteUint32(&buffer, uint32(fetchResp.Rows))  // rows
	wstool.WriteUint16(&buffer, uint16(2))               // table name length
	buffer.WriteString("t2")                             // table name
	buffer.Write(fetchBlockResp[16:])                    // raw block
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	var writeResp BaseResponse
	err = json.Unmarshal(resp, &writeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, writeResp.Code, writeResp.Message)

	// query
	queryReq = QueryRequest{ReqID: 6, Sql: "select * from t2"}
	resp, err = doWebSocket(ws, WSQuery, &queryReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// fetch
	fetchReq = FetchRequest{ReqID: 7, ID: queryResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)

	// fetch block
	fetchBlockReq = FetchBlockRequest{ReqID: 8, ID: queryResp.ID}
	fetchBlockResp, err = doWebSocket(ws, WSFetchBlock, &fetchBlockReq)
	assert.NoError(t, err)
	resultID, blockResult = parseblock.ParseBlock(fetchBlockResp[8:], queryResp.FieldsTypes, fetchResp.Rows, queryResp.Precision)
	checkBlockResult(t, blockResult)

	// fetch
	fetchReq = FetchRequest{ReqID: 9, ID: queryResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)

	assert.Equal(t, true, fetchResp.Completed)

	// write block with filed
	buffer.Reset()
	wstool.WriteUint64(&buffer, 300)                               // req id
	wstool.WriteUint64(&buffer, 400)                               // message id
	wstool.WriteUint64(&buffer, uint64(RawBlockMessageWithFields)) // action
	wstool.WriteUint32(&buffer, uint32(fetchResp.Rows))            // rows
	wstool.WriteUint16(&buffer, uint16(2))                         // table name length
	buffer.WriteString("t3")                                       // table name
	buffer.Write(fetchBlockResp[16:])                              // raw block
	fields := []byte{
		// ts
		0x74, 0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x09,
		// padding
		0x00, 0x00,
		// bytes
		0x08, 0x00, 0x00, 0x00,

		// v1
		0x76, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x01,
		// padding
		0x00, 0x00,
		// bytes
		0x01, 0x00, 0x00, 0x00,

		// v2
		0x76, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x02,
		// padding
		0x00, 0x00,
		// bytes
		0x01, 0x00, 0x00, 0x00,

		// v3
		0x76, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x03,
		// padding
		0x00, 0x00,
		// bytes
		0x02, 0x00, 0x00, 0x00,

		// v4
		0x76, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x04,
		// padding
		0x00, 0x00,
		// bytes
		0x04, 0x00, 0x00, 0x00,

		// v5
		0x76, 0x35, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x05,
		// padding
		0x00, 0x00,
		// bytes
		0x08, 0x00, 0x00, 0x00,

		// v6
		0x76, 0x36, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0b,
		// padding
		0x00, 0x00,
		// bytes
		0x01, 0x00, 0x00, 0x00,

		// v7
		0x76, 0x37, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0c,
		// padding
		0x00, 0x00,
		// bytes
		0x02, 0x00, 0x00, 0x00,

		// v8
		0x76, 0x38, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0d,
		// padding
		0x00, 0x00,
		// bytes
		0x04, 0x00, 0x00, 0x00,

		// v9
		0x76, 0x39, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0e,
		// padding
		0x00, 0x00,
		// bytes
		0x08, 0x00, 0x00, 0x00,

		// v10
		0x76, 0x31, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x06,
		// padding
		0x00, 0x00,
		// bytes
		0x04, 0x00, 0x00, 0x00,

		// v11
		0x76, 0x31, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x07,
		// padding
		0x00, 0x00,
		// bytes
		0x08, 0x00, 0x00, 0x00,

		// v12
		0x76, 0x31, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x08,
		// padding
		0x00, 0x00,
		// bytes
		0x14, 0x00, 0x00, 0x00,

		// v13
		0x76, 0x31, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0a,
		// padding
		0x00, 0x00,
		// bytes
		0x14, 0x00, 0x00, 0x00,

		// v14
		0x76, 0x31, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x10,
		// padding
		0x00, 0x00,
		// bytes
		0x14, 0x00, 0x00, 0x00,

		// v15
		0x76, 0x31, 0x35, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x14,
		// padding
		0x00, 0x00,
		// bytes
		0x64, 0x00, 0x00, 0x00,

		// info
		0x69, 0x6e, 0x66, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0f,
		// padding
		0x00, 0x00,
		// bytes
		0x00, 0x10, 0x00, 0x00,
	}
	buffer.Write(fields)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &writeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, writeResp.Code, writeResp.Message)

	// query
	queryReq = QueryRequest{ReqID: 10, Sql: "select * from t3"}
	resp, err = doWebSocket(ws, WSQuery, &queryReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// fetch
	fetchReq = FetchRequest{ReqID: 11, ID: queryResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)

	// fetch block
	fetchBlockReq = FetchBlockRequest{ReqID: 12, ID: queryResp.ID}
	fetchBlockResp, err = doWebSocket(ws, WSFetchBlock, &fetchBlockReq)
	assert.NoError(t, err)
	resultID, blockResult = parseblock.ParseBlock(fetchBlockResp[8:], queryResp.FieldsTypes, fetchResp.Rows, queryResp.Precision)

	// fetch
	fetchReq = FetchRequest{ReqID: 13, ID: queryResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)

	assert.Equal(t, true, fetchResp.Completed)
	time.Sleep(time.Second)
}

type FetchRawBlockResponse struct {
	Flag     uint64
	Version  uint16
	Time     uint64
	ReqID    uint64
	Code     uint32
	Message  string
	ResultID uint64
	Finished bool
	RawBlock []byte
}

func parseFetchRawBlock(message []byte) *FetchRawBlockResponse {
	var resp = &FetchRawBlockResponse{}
	resp.Flag = binary.LittleEndian.Uint64(message)
	resp.Version = binary.LittleEndian.Uint16(message[16:])
	resp.Time = binary.LittleEndian.Uint64(message[18:])
	resp.ReqID = binary.LittleEndian.Uint64(message[26:])
	resp.Code = binary.LittleEndian.Uint32(message[34:])
	msgLen := int(binary.LittleEndian.Uint32(message[38:]))
	resp.Message = string(message[42 : 42+msgLen])
	if resp.Code != 0 {
		return resp
	}
	resp.ResultID = binary.LittleEndian.Uint64(message[42+msgLen:])
	resp.Finished = message[50+msgLen] == 1
	if resp.Finished {
		return resp
	}
	blockLength := binary.LittleEndian.Uint32(message[51+msgLen:])
	resp.RawBlock = message[55+msgLen : 55+msgLen+int(blockLength)]
	return resp
}

func ReadBlockSimple(block unsafe.Pointer, precision int) [][]driver.Value {
	blockSize := parser.RawBlockGetNumOfRows(block)
	colCount := parser.RawBlockGetNumOfCols(block)
	colInfo := make([]parser.RawBlockColInfo, colCount)
	parser.RawBlockGetColInfo(block, colInfo)
	colTypes := make([]uint8, colCount)
	for i := int32(0); i < colCount; i++ {
		colTypes[i] = uint8(colInfo[i].ColType)
	}
	return parser.ReadBlock(block, int(blockSize), colTypes, precision)
}

func TestWsBinaryQuery(t *testing.T) {
	dbName := "test_ws_binary_query"
	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful(fmt.Sprintf("drop database if exists %s", dbName), "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf("create database if not exists %s", dbName), "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(
		"create table if not exists stb1 (ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100)) tags (info json)",
		dbName)
	assert.Equal(t, 0, code, message)
	code, message = doRestful(
		`insert into t1 using stb1 tags ('{\"table\":\"t1\"}') values (now-2s,true,2,3,4,5,6,7,8,9,10,11,'中文\"binary','中文nchar','\xaabbcc','point(100 100)')(now-1s,false,12,13,14,15,16,17,18,19,110,111,'中文\"binary','中文nchar','\xaabbcc','point(100 100)')(now,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)`,
		dbName)
	assert.Equal(t, 0, code, message)

	code, message = doRestful("create table t2 using stb1 tags('{\"table\":\"t2\"}')", dbName)
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create table t3 using stb1 tags('{\"table\":\"t3\"}')", dbName)
	assert.Equal(t, 0, code, message)

	defer doRestful(fmt.Sprintf("drop database if exists %s", dbName), "")
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err := ws.Close()
		assert.NoError(t, err)
	}()

	// connect
	connReq := ConnRequest{ReqID: 1, User: "root", Password: "taosdata", DB: dbName}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp BaseResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// query
	sql := "select * from stb1"
	var buffer bytes.Buffer
	wstool.WriteUint64(&buffer, 2) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 1)                // version
	wstool.WriteUint32(&buffer, uint32(len(sql))) // sql length
	buffer.WriteString(sql)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	var queryResp QueryResponse
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), queryResp.ReqID)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// fetch raw block
	buffer.Reset()
	wstool.WriteUint64(&buffer, 3)            // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1) // version
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp := parseFetchRawBlock(resp)
	assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
	assert.Equal(t, uint64(3), fetchRawBlockResp.ReqID)
	assert.Equal(t, uint32(0), fetchRawBlockResp.Code, fetchRawBlockResp.Message)
	assert.Equal(t, uint64(1), fetchRawBlockResp.ResultID)
	assert.Equal(t, false, fetchRawBlockResp.Finished)
	rows := parser.RawBlockGetNumOfRows(unsafe.Pointer(&fetchRawBlockResp.RawBlock[0]))
	assert.Equal(t, int32(3), rows)
	blockResult := ReadBlockSimple(unsafe.Pointer(&fetchRawBlockResp.RawBlock[0]), queryResp.Precision)
	checkBlockResult(t, blockResult)
	rawBlock := fetchRawBlockResp.RawBlock

	buffer.Reset()
	wstool.WriteUint64(&buffer, 5)            // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1) // version
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
	assert.Equal(t, uint64(5), fetchRawBlockResp.ReqID)
	assert.Equal(t, uint32(0), fetchRawBlockResp.Code, fetchRawBlockResp.Message)
	assert.Equal(t, uint64(1), fetchRawBlockResp.ResultID)
	assert.Equal(t, true, fetchRawBlockResp.Finished)

	// write block

	buffer.Reset()
	wstool.WriteUint64(&buffer, 300)                     // req id
	wstool.WriteUint64(&buffer, 400)                     // message id
	wstool.WriteUint64(&buffer, uint64(RawBlockMessage)) // action
	wstool.WriteUint32(&buffer, uint32(3))               // rows
	wstool.WriteUint16(&buffer, uint16(2))               // table name length
	buffer.WriteString("t2")                             // table name
	buffer.Write(rawBlock)                               // raw block
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	var writeResp BaseResponse
	err = json.Unmarshal(resp, &writeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, writeResp.Code, writeResp.Message)

	// query
	sql = "select * from t2"
	buffer.Reset()
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 1)                // version
	wstool.WriteUint32(&buffer, uint32(len(sql))) // sql length
	buffer.WriteString(sql)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// fetch raw block
	buffer.Reset()
	wstool.WriteUint64(&buffer, 7)            // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1) // version
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
	assert.Equal(t, uint64(7), fetchRawBlockResp.ReqID)
	assert.Equal(t, uint32(0), fetchRawBlockResp.Code, fetchRawBlockResp.Message)
	assert.Equal(t, false, fetchRawBlockResp.Finished)
	blockResult = ReadBlockSimple(unsafe.Pointer(&fetchRawBlockResp.RawBlock[0]), queryResp.Precision)
	checkBlockResult(t, blockResult)
	rawBlock = fetchRawBlockResp.RawBlock

	buffer.Reset()
	wstool.WriteUint64(&buffer, 9)            // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1) // version
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
	assert.Equal(t, uint64(9), fetchRawBlockResp.ReqID)
	assert.Equal(t, uint32(0), fetchRawBlockResp.Code, fetchRawBlockResp.Message)
	assert.Equal(t, true, fetchRawBlockResp.Finished)

	// write block with filed
	buffer.Reset()
	wstool.WriteUint64(&buffer, 300)                               // req id
	wstool.WriteUint64(&buffer, 400)                               // message id
	wstool.WriteUint64(&buffer, uint64(RawBlockMessageWithFields)) // action
	wstool.WriteUint32(&buffer, uint32(3))                         // rows
	wstool.WriteUint16(&buffer, uint16(2))                         // table name length
	buffer.WriteString("t3")                                       // table name
	buffer.Write(rawBlock)                                         // raw block
	fields := []byte{
		// ts
		0x74, 0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x09,
		// padding
		0x00, 0x00,
		// bytes
		0x08, 0x00, 0x00, 0x00,

		// v1
		0x76, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x01,
		// padding
		0x00, 0x00,
		// bytes
		0x01, 0x00, 0x00, 0x00,

		// v2
		0x76, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x02,
		// padding
		0x00, 0x00,
		// bytes
		0x01, 0x00, 0x00, 0x00,

		// v3
		0x76, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x03,
		// padding
		0x00, 0x00,
		// bytes
		0x02, 0x00, 0x00, 0x00,

		// v4
		0x76, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x04,
		// padding
		0x00, 0x00,
		// bytes
		0x04, 0x00, 0x00, 0x00,

		// v5
		0x76, 0x35, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x05,
		// padding
		0x00, 0x00,
		// bytes
		0x08, 0x00, 0x00, 0x00,

		// v6
		0x76, 0x36, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0b,
		// padding
		0x00, 0x00,
		// bytes
		0x01, 0x00, 0x00, 0x00,

		// v7
		0x76, 0x37, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0c,
		// padding
		0x00, 0x00,
		// bytes
		0x02, 0x00, 0x00, 0x00,

		// v8
		0x76, 0x38, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0d,
		// padding
		0x00, 0x00,
		// bytes
		0x04, 0x00, 0x00, 0x00,

		// v9
		0x76, 0x39, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0e,
		// padding
		0x00, 0x00,
		// bytes
		0x08, 0x00, 0x00, 0x00,

		// v10
		0x76, 0x31, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x06,
		// padding
		0x00, 0x00,
		// bytes
		0x04, 0x00, 0x00, 0x00,

		// v11
		0x76, 0x31, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x07,
		// padding
		0x00, 0x00,
		// bytes
		0x08, 0x00, 0x00, 0x00,

		// v12
		0x76, 0x31, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x08,
		// padding
		0x00, 0x00,
		// bytes
		0x14, 0x00, 0x00, 0x00,

		// v13
		0x76, 0x31, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0a,
		// padding
		0x00, 0x00,
		// bytes
		0x14, 0x00, 0x00, 0x00,

		// v14
		0x76, 0x31, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x10,
		// padding
		0x00, 0x00,
		// bytes
		0x14, 0x00, 0x00, 0x00,

		// v15
		0x76, 0x31, 0x35, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x14,
		// padding
		0x00, 0x00,
		// bytes
		0x64, 0x00, 0x00, 0x00,

		// info
		0x69, 0x6e, 0x66, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0f,
		// padding
		0x00, 0x00,
		// bytes
		0x00, 0x10, 0x00, 0x00,
	}
	buffer.Write(fields)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &writeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, writeResp.Code, writeResp.Message)

	// query
	sql = "select * from t3"
	buffer.Reset()
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 1)                // version
	wstool.WriteUint32(&buffer, uint32(len(sql))) // sql length
	buffer.WriteString(sql)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// fetch raw block
	buffer.Reset()
	wstool.WriteUint64(&buffer, 11)           // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1) // version
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
	assert.Equal(t, uint64(11), fetchRawBlockResp.ReqID)
	assert.Equal(t, uint32(0), fetchRawBlockResp.Code, fetchRawBlockResp.Message)
	assert.Equal(t, false, fetchRawBlockResp.Finished)
	blockResult = ReadBlockSimple(unsafe.Pointer(&fetchRawBlockResp.RawBlock[0]), queryResp.Precision)
	checkBlockResult(t, blockResult)
	rawBlock = fetchRawBlockResp.RawBlock

	buffer.Reset()
	wstool.WriteUint64(&buffer, 13)           // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1) // version
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
	assert.Equal(t, uint64(13), fetchRawBlockResp.ReqID)
	assert.Equal(t, uint32(0), fetchRawBlockResp.Code, fetchRawBlockResp.Message)
	assert.Equal(t, true, fetchRawBlockResp.Finished)

	// wrong message length
	buffer.Reset()
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 1) // version
	wstool.WriteUint32(&buffer, 0) // sql length
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 65535, queryResp.Code, queryResp.Message)

	// wrong sql length
	buffer.Reset()
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 1)   // version
	wstool.WriteUint32(&buffer, 100) // sql length
	buffer.WriteString("wrong sql length")
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 65535, queryResp.Code, queryResp.Message)

	// wrong version
	buffer.Reset()
	sql = "select 1"
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 100)              // version
	wstool.WriteUint32(&buffer, uint32(len(sql))) // sql length
	buffer.WriteString(sql)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 65535, queryResp.Code, queryResp.Message)

	// wrong sql
	buffer.Reset()
	sql = "wrong sql"
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 1)                // version
	wstool.WriteUint32(&buffer, uint32(len(sql))) // sql length
	buffer.WriteString(sql)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.NotEqual(t, 0, queryResp.Code, queryResp.Message)

	// insert sql
	buffer.Reset()
	sql = "create table t4 using stb1 tags('{\"table\":\"t4\"}')"
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 1)                // version
	wstool.WriteUint32(&buffer, uint32(len(sql))) // sql length
	buffer.WriteString(sql)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)
	assert.Equal(t, true, queryResp.IsUpdate)

	// wrong fetch
	buffer.Reset()
	sql = "select 1"
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 1)                // version
	wstool.WriteUint32(&buffer, uint32(len(sql))) // sql length
	buffer.WriteString(sql)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// wrong fetch raw block length
	buffer.Reset()
	wstool.WriteUint64(&buffer, 700)          // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(700), fetchRawBlockResp.ReqID)
	assert.NotEqual(t, uint32(0), fetchRawBlockResp.Code)

	// wrong fetch raw block version
	buffer.Reset()
	wstool.WriteUint64(&buffer, 800)          // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 100)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(800), fetchRawBlockResp.ReqID)
	assert.NotEqual(t, uint32(0), fetchRawBlockResp.Code)
	time.Sleep(time.Second)

	// wrong fetch raw block result
	buffer.Reset()
	wstool.WriteUint64(&buffer, 900)              // req id
	wstool.WriteUint64(&buffer, queryResp.ID+100) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(900), fetchRawBlockResp.ReqID)
	assert.NotEqual(t, uint32(0), fetchRawBlockResp.Code)

	// fetch freed raw block
	buffer.Reset()
	wstool.WriteUint64(&buffer, 600)          // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
	assert.Equal(t, uint64(600), fetchRawBlockResp.ReqID)
	assert.Equal(t, uint32(0), fetchRawBlockResp.Code, fetchRawBlockResp.Message)
	assert.Equal(t, int32(1), parser.RawBlockGetNumOfRows(unsafe.Pointer(&fetchRawBlockResp.RawBlock[0])))

	buffer.Reset()
	wstool.WriteUint64(&buffer, 700)          // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
	assert.Equal(t, uint64(700), fetchRawBlockResp.ReqID)
	assert.Equal(t, uint32(0), fetchRawBlockResp.Code, fetchRawBlockResp.Message)
	assert.Equal(t, true, fetchRawBlockResp.Finished)

	buffer.Reset()
	wstool.WriteUint64(&buffer, 400)          // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
	assert.Equal(t, uint64(400), fetchRawBlockResp.ReqID)
	assert.NotEqual(t, uint32(0), fetchRawBlockResp.Code)
	time.Sleep(time.Second)
}

func checkBlockResult(t *testing.T, blockResult [][]driver.Value) {
	assert.Equal(t, 3, len(blockResult))
	assert.Equal(t, true, blockResult[0][1])
	assert.Equal(t, int8(2), blockResult[0][2])
	assert.Equal(t, int16(3), blockResult[0][3])
	assert.Equal(t, int32(4), blockResult[0][4])
	assert.Equal(t, int64(5), blockResult[0][5])
	assert.Equal(t, uint8(6), blockResult[0][6])
	assert.Equal(t, uint16(7), blockResult[0][7])
	assert.Equal(t, uint32(8), blockResult[0][8])
	assert.Equal(t, uint64(9), blockResult[0][9])
	assert.Equal(t, float32(10), blockResult[0][10])
	assert.Equal(t, float64(11), blockResult[0][11])
	assert.Equal(t, "中文\"binary", blockResult[0][12])
	assert.Equal(t, "中文nchar", blockResult[0][13])
	assert.Equal(t, []byte{0xaa, 0xbb, 0xcc}, blockResult[0][14])
	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, blockResult[0][15])
	assert.Equal(t, false, blockResult[1][1])
	assert.Equal(t, int8(12), blockResult[1][2])
	assert.Equal(t, int16(13), blockResult[1][3])
	assert.Equal(t, int32(14), blockResult[1][4])
	assert.Equal(t, int64(15), blockResult[1][5])
	assert.Equal(t, uint8(16), blockResult[1][6])
	assert.Equal(t, uint16(17), blockResult[1][7])
	assert.Equal(t, uint32(18), blockResult[1][8])
	assert.Equal(t, uint64(19), blockResult[1][9])
	assert.Equal(t, float32(110), blockResult[1][10])
	assert.Equal(t, float64(111), blockResult[1][11])
	assert.Equal(t, "中文\"binary", blockResult[1][12])
	assert.Equal(t, "中文nchar", blockResult[1][13])
	assert.Equal(t, []byte{0xaa, 0xbb, 0xcc}, blockResult[1][14])
	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, blockResult[1][15])
	assert.Equal(t, nil, blockResult[2][1])
	assert.Equal(t, nil, blockResult[2][2])
	assert.Equal(t, nil, blockResult[2][3])
	assert.Equal(t, nil, blockResult[2][4])
	assert.Equal(t, nil, blockResult[2][5])
	assert.Equal(t, nil, blockResult[2][6])
	assert.Equal(t, nil, blockResult[2][7])
	assert.Equal(t, nil, blockResult[2][8])
	assert.Equal(t, nil, blockResult[2][9])
	assert.Equal(t, nil, blockResult[2][10])
	assert.Equal(t, nil, blockResult[2][11])
	assert.Equal(t, nil, blockResult[2][12])
	assert.Equal(t, nil, blockResult[2][13])
	assert.Equal(t, nil, blockResult[2][14])
	assert.Equal(t, nil, blockResult[2][15])
}

func TestWsSchemaless(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful("drop database if exists test_ws_schemaless", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_schemaless", "")
	assert.Equal(t, 0, code, message)

	//defer doRestful("drop database if exists test_ws_schemaless", "")

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err := ws.Close()
		assert.NoError(t, err)
	}()

	cases := []struct {
		name         string
		protocol     int
		precision    string
		data         string
		ttl          int
		code         int
		totalRows    int32
		affectedRows int
		tableNameKey string
	}{
		{
			name:      "influxdb",
			protocol:  schemaless.InfluxDBLineProtocol,
			precision: "ms",
			data: "measurement,host=host1 field1=2i,field2=2.0 1577837300000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837400000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837500000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837600000",
			ttl:          1000,
			code:         0,
			totalRows:    4,
			affectedRows: 4,
		},
		{
			name:      "opentsdb_telnet",
			protocol:  schemaless.OpenTSDBTelnetLineProtocol,
			precision: "ms",
			data: "meters.current 1648432611249 10.3 location=California.SanFrancisco group=2\n" +
				"meters.current 1648432611250 12.6 location=California.SanFrancisco group=2\n" +
				"meters.current 1648432611249 10.8 location=California.LosAngeles group=3\n" +
				"meters.current 1648432611250 11.3 location=California.LosAngeles group=3\n" +
				"meters.voltage 1648432611249 219 location=California.SanFrancisco group=2\n" +
				"meters.voltage 1648432611250 218 location=California.SanFrancisco group=2\n" +
				"meters.voltage 1648432611249 221 location=California.LosAngeles group=3\n" +
				"meters.voltage 1648432611250 217 location=California.LosAngeles group=3",
			ttl:          1000,
			code:         0,
			totalRows:    8,
			affectedRows: 8,
		},
		{
			name:      "opentsdb_json",
			protocol:  schemaless.OpenTSDBJsonFormatProtocol,
			precision: "ms",
			data: `[
    {
        "metric": "meters2.current",
        "timestamp": 1648432611249,
        "value": 10.3,
        "tags": {
            "location": "California.SanFrancisco",
            "groupid": 2
        }
    },
    {
        "metric": "meters2.voltage",
        "timestamp": 1648432611249,
        "value": 219,
        "tags": {
            "location": "California.LosAngeles",
            "groupid": 1
        }
    },
    {
        "metric": "meters2.current",
        "timestamp": 1648432611250,
        "value": 12.6,
        "tags": {
            "location": "California.SanFrancisco",
            "groupid": 2
        }
    },
    {
        "metric": "meters2.voltage",
        "timestamp": 1648432611250,
        "value": 221,
        "tags": {
            "location": "California.LosAngeles",
            "groupid": 1
        }
    }
]`,
			ttl:          100,
			code:         0,
			affectedRows: 4,
		},
		{
			name:      "influxdb_tbnamekey",
			protocol:  schemaless.InfluxDBLineProtocol,
			precision: "ms",
			data: "measurement,host=host1 field1=2i,field2=2.0 1577837300000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837400000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837500000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837600000",
			ttl:          1000,
			code:         0,
			totalRows:    4,
			affectedRows: 4,
			tableNameKey: "host",
		},
	}

	// connect
	connReq := ConnRequest{ReqID: 1, User: "root", Password: "taosdata", DB: "test_ws_schemaless"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp BaseResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	for _, c := range cases {
		reqID := uint64(1)
		t.Run(c.name, func(t *testing.T) {
			reqID += 1
			req := SchemalessWriteRequest{
				ReqID:        reqID,
				Protocol:     c.protocol,
				Precision:    c.precision,
				TTL:          c.ttl,
				Data:         c.data,
				TableNameKey: c.tableNameKey,
			}
			resp, err = doWebSocket(ws, SchemalessWrite, &req)
			assert.NoError(t, err)
			var schemalessResp SchemalessWriteResponse
			err = json.Unmarshal(resp, &schemalessResp)
			assert.NoError(t, err, string(resp))
			assert.Equal(t, reqID, schemalessResp.ReqID)
			assert.Equal(t, 0, schemalessResp.Code, schemalessResp.Message)
			if c.protocol != schemaless.OpenTSDBJsonFormatProtocol {
				assert.Equal(t, c.totalRows, schemalessResp.TotalRows)
			}
			assert.Equal(t, c.affectedRows, schemalessResp.AffectedRows)
		})
	}
}

func TestWsStmt(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful("drop database if exists test_ws_stmt_ws", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_stmt_ws precision 'ns'", "")
	assert.Equal(t, 0, code, message)

	defer doRestful("drop database if exists test_ws_stmt_ws", "")

	code, message = doRestful(
		"create table if not exists stb (ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100)) tags (info json)",
		"test_ws_stmt_ws")
	assert.Equal(t, 0, code, message)

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err := ws.Close()
		assert.NoError(t, err)
	}()

	// connect
	connReq := ConnRequest{ReqID: 1, User: "root", Password: "taosdata", DB: "test_ws_stmt_ws"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp BaseResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// init
	initReq := map[string]uint64{"req_id": 2}
	resp, err = doWebSocket(ws, STMTInit, &initReq)
	assert.NoError(t, err)
	var initResp StmtInitResponse
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), initResp.ReqID)
	assert.Equal(t, 0, initResp.Code, initResp.Message)

	// prepare
	prepareReq := StmtPrepareRequest{ReqID: 3, StmtID: initResp.StmtID, SQL: "insert into ? using test_ws_stmt_ws.stb tags (?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"}
	resp, err = doWebSocket(ws, STMTPrepare, &prepareReq)
	assert.NoError(t, err)
	var prepareResp StmtPrepareResponse
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)
	assert.True(t, prepareResp.IsInsert)

	// set table name
	setTableNameReq := StmtSetTableNameRequest{ReqID: 4, StmtID: prepareResp.StmtID, Name: "test_ws_stmt_ws.ct1"}
	resp, err = doWebSocket(ws, STMTSetTableName, &setTableNameReq)
	assert.NoError(t, err)
	var setTableNameResp BaseResponse
	err = json.Unmarshal(resp, &setTableNameResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), setTableNameResp.ReqID)
	assert.Equal(t, 0, setTableNameResp.Code, setTableNameResp.Message)

	// get tag fields
	getTagFieldsReq := StmtGetTagFieldsRequest{ReqID: 5, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTGetTagFields, &getTagFieldsReq)
	assert.NoError(t, err)
	var getTagFieldsResp StmtGetTagFieldsResponse
	err = json.Unmarshal(resp, &getTagFieldsResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), getTagFieldsResp.ReqID)
	assert.Equal(t, 0, getTagFieldsResp.Code, getTagFieldsResp.Message)

	// get col fields
	getColFieldsReq := StmtGetColFieldsRequest{ReqID: 6, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTGetColFields, &getColFieldsReq)
	assert.NoError(t, err)
	var getColFieldsResp StmtGetColFieldsResponse
	err = json.Unmarshal(resp, &getColFieldsResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), getColFieldsResp.ReqID)
	assert.Equal(t, 0, getColFieldsResp.Code, getColFieldsResp.Message)

	// set tags
	setTagsReq := StmtSetTagsRequest{ReqID: 7, StmtID: prepareResp.StmtID, Tags: json.RawMessage(`["{\"a\":\"b\"}"]`)}
	resp, err = doWebSocket(ws, STMTSetTags, &setTagsReq)
	assert.NoError(t, err)
	var setTagsResp BaseResponse
	err = json.Unmarshal(resp, &setTagsResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(7), setTagsResp.ReqID)
	assert.Equal(t, 0, setTagsResp.Code, setTagsResp.Message)

	// bind
	now := time.Now()
	columns, _ := json.Marshal([][]driver.Value{
		{now, now.Add(time.Second), now.Add(time.Second * 2)},
		{true, false, nil},
		{2, 22, nil},
		{3, 33, nil},
		{4, 44, nil},
		{5, 55, nil},
		{6, 66, nil},
		{7, 77, nil},
		{8, 88, nil},
		{9, 99, nil},
		{10, 1010, nil},
		{11, 1111, nil},
		{"binary", "binary2", nil},
		{"nchar", "nchar2", nil},
		{"aabbcc", "aabbcc", nil},
		{"010100000000000000000059400000000000005940", "010100000000000000000059400000000000005940", nil},
	})
	bindReq := StmtBindRequest{ReqID: 8, StmtID: prepareResp.StmtID, Columns: columns}
	resp, err = doWebSocket(ws, STMTBind, &bindReq)
	assert.NoError(t, err)
	var bindResp StmtBindResponse
	err = json.Unmarshal(resp, &bindResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(8), bindResp.ReqID)
	assert.Equal(t, 0, bindResp.Code, bindResp.Message)

	// add batch
	addBatchReq := StmtAddBatchRequest{ReqID: 9, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTAddBatch, &addBatchReq)
	assert.NoError(t, err)
	var addBatchResp StmtAddBatchResponse
	err = json.Unmarshal(resp, &addBatchResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(9), addBatchResp.ReqID)
	assert.Equal(t, 0, bindResp.Code, bindResp.Message)

	// exec
	execReq := StmtExecRequest{ReqID: 10, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTExec, &execReq)
	assert.NoError(t, err)
	var execResp StmtExecResponse
	err = json.Unmarshal(resp, &execResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), execResp.ReqID)
	assert.Equal(t, 0, execResp.Code, execResp.Message)

	// close
	closeReq := StmtCloseRequest{ReqID: 11, StmtID: prepareResp.StmtID}
	err = doWebSocketWithoutResp(ws, STMTClose, &closeReq)
	assert.NoError(t, err)

	// query
	queryReq := QueryRequest{Sql: "select * from test_ws_stmt_ws.stb"}
	resp, err = doWebSocket(ws, WSQuery, &queryReq)
	assert.NoError(t, err)
	var queryResp QueryResponse
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// fetch
	fetchReq := FetchRequest{ID: queryResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	var fetchResp FetchResponse
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)

	// fetch block
	fetchBlockReq := FetchBlockRequest{ID: queryResp.ID}
	fetchBlockResp, err := doWebSocket(ws, WSFetchBlock, &fetchBlockReq)
	assert.NoError(t, err)
	_, blockResult := parseblock.ParseBlock(fetchBlockResp[8:], queryResp.FieldsTypes, fetchResp.Rows, queryResp.Precision)
	assert.Equal(t, 3, len(blockResult))
	assert.Equal(t, now.UnixNano(), blockResult[0][0].(time.Time).UnixNano())

	assert.Equal(t, true, blockResult[0][1])
	assert.Equal(t, int8(2), blockResult[0][2])
	assert.Equal(t, int16(3), blockResult[0][3])
	assert.Equal(t, int32(4), blockResult[0][4])
	assert.Equal(t, int64(5), blockResult[0][5])
	assert.Equal(t, uint8(6), blockResult[0][6])
	assert.Equal(t, uint16(7), blockResult[0][7])
	assert.Equal(t, uint32(8), blockResult[0][8])
	assert.Equal(t, uint64(9), blockResult[0][9])
	assert.Equal(t, float32(10), blockResult[0][10])
	assert.Equal(t, float64(11), blockResult[0][11])
	assert.Equal(t, "binary", blockResult[0][12])
	assert.Equal(t, "nchar", blockResult[0][13])
	assert.Equal(t, []byte{0xaa, 0xbb, 0xcc}, blockResult[1][14])
	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, blockResult[0][15])

	assert.Equal(t, now.Add(time.Second).UnixNano(), blockResult[1][0].(time.Time).UnixNano())
	assert.Equal(t, false, blockResult[1][1])
	assert.Equal(t, int8(22), blockResult[1][2])
	assert.Equal(t, int16(33), blockResult[1][3])
	assert.Equal(t, int32(44), blockResult[1][4])
	assert.Equal(t, int64(55), blockResult[1][5])
	assert.Equal(t, uint8(66), blockResult[1][6])
	assert.Equal(t, uint16(77), blockResult[1][7])
	assert.Equal(t, uint32(88), blockResult[1][8])
	assert.Equal(t, uint64(99), blockResult[1][9])
	assert.Equal(t, float32(1010), blockResult[1][10])
	assert.Equal(t, float64(1111), blockResult[1][11])
	assert.Equal(t, "binary2", blockResult[1][12])
	assert.Equal(t, "nchar2", blockResult[1][13])
	assert.Equal(t, []byte{0xaa, 0xbb, 0xcc}, blockResult[1][14])
	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, blockResult[1][15])

	assert.Equal(t, now.Add(time.Second*2).UnixNano(), blockResult[2][0].(time.Time).UnixNano())
	for i := 1; i < 16; i++ {
		assert.Nil(t, blockResult[2][i])
	}

	// block message
	// init
	resp, err = doWebSocket(ws, STMTInit, nil)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, initResp.Code, initResp.Message)

	// prepare
	prepareReq = StmtPrepareRequest{StmtID: initResp.StmtID, SQL: "insert into ? using test_ws_stmt_ws.stb tags(?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"}
	resp, err = doWebSocket(ws, STMTPrepare, &prepareReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)

	// set table name
	setTableNameReq = StmtSetTableNameRequest{StmtID: prepareResp.StmtID, Name: "test_ws_stmt_ws.ct2"}
	resp, err = doWebSocket(ws, STMTSetTableName, &setTableNameReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &setTableNameResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, setTableNameResp.Code, setTableNameResp.Message)

	// set tags
	var tagBuffer bytes.Buffer
	wstool.WriteUint64(&tagBuffer, 100)
	wstool.WriteUint64(&tagBuffer, prepareResp.StmtID)
	wstool.WriteUint64(&tagBuffer, uint64(SetTagsMessage))
	tags, err := json.Marshal(map[string]string{"a": "b"})
	assert.NoError(t, err)
	b, err := serializer.SerializeRawBlock(
		[]*param.Param{
			param.NewParam(1).AddJson(tags),
		},
		param.NewColumnType(1).AddJson(50))
	assert.NoError(t, err)
	assert.NoError(t, err)
	tagBuffer.Write(b)

	err = ws.WriteMessage(websocket.BinaryMessage, tagBuffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &setTagsResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, setTagsResp.Code, setTagsResp.Message)

	// bind binary
	var block bytes.Buffer
	wstool.WriteUint64(&block, 10)
	wstool.WriteUint64(&block, prepareResp.StmtID)
	wstool.WriteUint64(&block, uint64(BindMessage))
	rawBlock := []byte{
		0x01, 0x00, 0x00, 0x00,
		0x11, 0x02, 0x00, 0x00,
		0x03, 0x00, 0x00, 0x00,
		0x10, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x80,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

		0x09, 0x08, 0x00, 0x00, 0x00,
		0x01, 0x01, 0x00, 0x00, 0x00,
		0x02, 0x01, 0x00, 0x00, 0x00,
		0x03, 0x02, 0x00, 0x00, 0x00,
		0x04, 0x04, 0x00, 0x00, 0x00,
		0x05, 0x08, 0x00, 0x00, 0x00,
		0x0b, 0x01, 0x00, 0x00, 0x00,
		0x0c, 0x02, 0x00, 0x00, 0x00,
		0x0d, 0x04, 0x00, 0x00, 0x00,
		0x0e, 0x08, 0x00, 0x00, 0x00,
		0x06, 0x04, 0x00, 0x00, 0x00,
		0x07, 0x08, 0x00, 0x00, 0x00,
		0x08, 0x16, 0x00, 0x00, 0x00,
		0x0a, 0x52, 0x00, 0x00, 0x00,
		0x10, 0x20, 0x00, 0x00, 0x00,
		0x14, 0x20, 0x00, 0x00, 0x00,

		0x18, 0x00, 0x00, 0x00,
		0x03, 0x00, 0x00, 0x00,
		0x03, 0x00, 0x00, 0x00,
		0x06, 0x00, 0x00, 0x00,
		0x0c, 0x00, 0x00, 0x00,
		0x18, 0x00, 0x00, 0x00,
		0x03, 0x00, 0x00, 0x00,
		0x06, 0x00, 0x00, 0x00,
		0x0c, 0x00, 0x00, 0x00,
		0x18, 0x00, 0x00, 0x00,
		0x0c, 0x00, 0x00, 0x00,
		0x18, 0x00, 0x00, 0x00,
		0x11, 0x00, 0x00, 0x00,
		0x30, 0x00, 0x00, 0x00,
		0x21, 0x00, 0x00, 0x00,
		0x2e, 0x00, 0x00, 0x00,

		0x00,
		0x2c, 0x5b, 0x70, 0x86, 0x82, 0x01, 0x00, 0x00,
		0x14, 0x5f, 0x70, 0x86, 0x82, 0x01, 0x00, 0x00,
		0xfc, 0x62, 0x70, 0x86, 0x82, 0x01, 0x00, 0x00,

		0x20,
		0x01,
		0x00,
		0x00,

		0x20,
		0x02,
		0x16,
		0x00,

		0x20,
		0x03, 0x00,
		0x21, 0x00,
		0x00, 0x00,

		0x20,
		0x04, 0x00, 0x00, 0x00,
		0x2c, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,

		0x20,
		0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x37, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

		0x20,
		0x06,
		0x42,
		0x00,

		0x20,
		0x07, 0x00,
		0x4d, 0x00,
		0x00, 0x00,

		0x20,
		0x08, 0x00, 0x00, 0x00,
		0x58, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,

		0x20,
		0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x63, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

		0x20,
		0x00, 0x00, 0x20, 0x41,
		0x00, 0x80, 0x7c, 0x44,
		0x00, 0x00, 0x00, 0x00,

		0x20,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x26, 0x40,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x5c, 0x91, 0x40,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

		0x00, 0x00, 0x00, 0x00,
		0x08, 0x00, 0x00, 0x00,
		0xff, 0xff, 0xff, 0xff,
		0x06, 0x00,
		0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,
		0x07, 0x00,
		0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x32,

		0x00, 0x00, 0x00, 0x00,
		0x16, 0x00, 0x00, 0x00,
		0xff, 0xff, 0xff, 0xff,
		0x14, 0x00,
		0x6e, 0x00, 0x00, 0x00, 0x63, 0x00, 0x00, 0x00, 0x68, 0x00,
		0x00, 0x00, 0x61, 0x00, 0x00, 0x00, 0x72, 0x00, 0x00, 0x00,
		0x18, 0x00,
		0x6e, 0x00, 0x00, 0x00, 0x63, 0x00, 0x00, 0x00, 0x68, 0x00, 0x00, 0x00,
		0x61, 0x00, 0x00, 0x00, 0x72, 0x00, 0x00, 0x00, 0x32, 0x00, 0x00, 0x00,

		0x00, 0x00, 0x00, 0x00,
		0x10, 0x00, 0x00, 0x00,
		0xff, 0xff, 0xff, 0xff,
		0x0e, 0x00,
		0x74, 0x65, 0x73, 0x74, 0x5f, 0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,
		0x0f, 0x00,
		0x74, 0x65, 0x73, 0x74, 0x5f, 0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x32,

		0x00, 0x00, 0x00, 0x00,
		0x17, 0x00, 0x00, 0x00,
		0xff, 0xff, 0xff, 0xff,
		0x15, 0x00,
		0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
		0x15, 0x00,
		0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
	}
	binary.LittleEndian.PutUint64(rawBlock[173:], uint64(now.UnixNano()))
	binary.LittleEndian.PutUint64(rawBlock[181:], uint64(now.Add(time.Second).UnixNano()))
	binary.LittleEndian.PutUint64(rawBlock[189:], uint64(now.Add(time.Second*2).UnixNano()))
	block.Write(rawBlock)
	err = ws.WriteMessage(
		websocket.BinaryMessage,
		block.Bytes(),
	)
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &bindResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, bindResp.Code, bindResp.Message)

	// add batch
	addBatchReq = StmtAddBatchRequest{StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTAddBatch, &addBatchReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &addBatchResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, bindResp.Code, bindResp.Message)

	// exec
	execReq = StmtExecRequest{StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTExec, &execReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &execResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, execResp.Code, execResp.Message)

	// query
	queryReq = QueryRequest{Sql: "select * from test_ws_stmt_ws.ct2"}
	resp, err = doWebSocket(ws, WSQuery, &queryReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// fetch
	fetchReq = FetchRequest{ID: queryResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)

	// fetch block
	fetchBlockReq = FetchBlockRequest{ID: queryResp.ID}
	fetchBlockResp, err = doWebSocket(ws, WSFetchBlock, &fetchBlockReq)
	assert.NoError(t, err)
	_, blockResult = parseblock.ParseBlock(fetchBlockResp[8:], queryResp.FieldsTypes, fetchResp.Rows, queryResp.Precision)
	assert.Equal(t, now.UnixNano(), blockResult[0][0].(time.Time).UnixNano())
	assert.Equal(t, true, blockResult[0][1])
	assert.Equal(t, int8(2), blockResult[0][2])
	assert.Equal(t, int16(3), blockResult[0][3])
	assert.Equal(t, int32(4), blockResult[0][4])
	assert.Equal(t, int64(5), blockResult[0][5])
	assert.Equal(t, uint8(6), blockResult[0][6])
	assert.Equal(t, uint16(7), blockResult[0][7])
	assert.Equal(t, uint32(8), blockResult[0][8])
	assert.Equal(t, uint64(9), blockResult[0][9])
	assert.Equal(t, float32(10), blockResult[0][10])
	assert.Equal(t, float64(11), blockResult[0][11])
	assert.Equal(t, "binary", blockResult[0][12])
	assert.Equal(t, "nchar", blockResult[0][13])
	assert.Equal(t, []byte("test_varbinary2"), blockResult[1][14])
	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, blockResult[0][15])

	assert.Equal(t, now.Add(time.Second).UnixNano(), blockResult[1][0].(time.Time).UnixNano())
	assert.Equal(t, false, blockResult[1][1])
	assert.Equal(t, int8(22), blockResult[1][2])
	assert.Equal(t, int16(33), blockResult[1][3])
	assert.Equal(t, int32(44), blockResult[1][4])
	assert.Equal(t, int64(55), blockResult[1][5])
	assert.Equal(t, uint8(66), blockResult[1][6])
	assert.Equal(t, uint16(77), blockResult[1][7])
	assert.Equal(t, uint32(88), blockResult[1][8])
	assert.Equal(t, uint64(99), blockResult[1][9])
	assert.Equal(t, float32(1010), blockResult[1][10])
	assert.Equal(t, float64(1111), blockResult[1][11])
	assert.Equal(t, "binary2", blockResult[1][12])
	assert.Equal(t, "nchar2", blockResult[1][13])
	assert.Equal(t, []byte("test_varbinary2"), blockResult[1][14])
	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, blockResult[1][15])

	assert.Equal(t, now.Add(time.Second*2).UnixNano(), blockResult[2][0].(time.Time).UnixNano())
	for i := 1; i < 16; i++ {
		assert.Nil(t, blockResult[2][i])
	}
}

func TestStmtQuery(t *testing.T) {
	//for stable
	prepareDataSql := []string{
		"create stable meters (ts timestamp,current float,voltage int,phase float) tags (group_id int, location varchar(24))",
		"insert into d0 using meters tags (2, 'California.SanFrancisco') values ('2023-09-13 17:53:52.123', 10.2, 219, 0.32) ",
		"insert into d1 using meters tags (1, 'California.SanFrancisco') values ('2023-09-13 17:54:43.321', 10.3, 218, 0.31) ",
	}
	StmtQuery(t, "test_ws_stmt_query_for_stable", prepareDataSql)

	// for table
	prepareDataSql = []string{
		"create table meters (ts timestamp,current float,voltage int,phase float, group_id int, location varchar(24))",
		"insert into meters values ('2023-09-13 17:53:52.123', 10.2, 219, 0.32, 2, 'California.SanFrancisco') ",
		"insert into meters values ('2023-09-13 17:54:43.321', 10.3, 218, 0.31, 1, 'California.SanFrancisco') ",
	}
	StmtQuery(t, "test_ws_stmt_query_for_table", prepareDataSql)
}

func StmtQuery(t *testing.T, db string, prepareDataSql []string) {
	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful(fmt.Sprintf("drop database if exists %s", db), "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf("create database if not exists %s", db), "")
	assert.Equal(t, 0, code, message)

	defer doRestful(fmt.Sprintf("drop database if exists %s", db), "")

	for _, sql := range prepareDataSql {
		code, message = doRestful(sql, db)
		assert.Equal(t, 0, code, message)
	}

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err := ws.Close()
		assert.NoError(t, err)
	}()

	// connect
	connReq := ConnRequest{ReqID: 1, User: "root", Password: "taosdata", DB: db}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp BaseResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// init
	initReq := map[string]uint64{"req_id": 2}
	resp, err = doWebSocket(ws, STMTInit, &initReq)
	assert.NoError(t, err)
	var initResp StmtInitResponse
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), initResp.ReqID)
	assert.Equal(t, 0, initResp.Code, initResp.Message)

	// prepare
	prepareReq := StmtPrepareRequest{
		ReqID:  3,
		StmtID: initResp.StmtID,
		SQL:    fmt.Sprintf("select * from %s.meters where group_id=? and location=?", db),
	}
	resp, err = doWebSocket(ws, STMTPrepare, &prepareReq)
	assert.NoError(t, err)
	var prepareResp StmtPrepareResponse
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)
	assert.False(t, prepareResp.IsInsert)

	// bind
	var block bytes.Buffer
	wstool.WriteUint64(&block, 5)
	wstool.WriteUint64(&block, prepareResp.StmtID)
	wstool.WriteUint64(&block, uint64(BindMessage))
	b, err := serializer.SerializeRawBlock(
		[]*param.Param{
			param.NewParam(1).AddInt(1),
			param.NewParam(1).AddBinary([]byte("California.SanFrancisco")),
		},
		param.NewColumnType(2).AddInt().AddBinary(24))
	assert.NoError(t, err)
	block.Write(b)

	err = ws.WriteMessage(websocket.BinaryMessage, block.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	var bindResp StmtBindResponse
	err = json.Unmarshal(resp, &bindResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), bindResp.ReqID)
	assert.Equal(t, 0, bindResp.Code, bindResp.Message)

	// add batch
	addBatchReq := StmtAddBatchRequest{StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTAddBatch, &addBatchReq)
	assert.NoError(t, err)
	var addBatchResp StmtAddBatchResponse
	err = json.Unmarshal(resp, &addBatchResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, bindResp.Code, bindResp.Message)

	// exec
	execReq := StmtExecRequest{ReqID: 6, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTExec, &execReq)
	assert.NoError(t, err)
	var execResp StmtExecResponse
	err = json.Unmarshal(resp, &execResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), execResp.ReqID)
	assert.Equal(t, 0, execResp.Code, execResp.Message)

	// use result
	useResultReq := StmtUseResultRequest{ReqID: 7, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTUseResult, &useResultReq)
	assert.NoError(t, err)
	var useResultResp StmtUseResultResponse
	err = json.Unmarshal(resp, &useResultResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(7), useResultResp.ReqID)
	assert.Equal(t, 0, useResultResp.Code, useResultResp.Message)

	// fetch
	fetchReq := FetchRequest{ReqID: 8, ID: useResultResp.ResultID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	var fetchResp FetchResponse
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(8), fetchResp.ReqID)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)
	assert.Equal(t, 1, fetchResp.Rows)

	// fetch block
	fetchBlockReq := FetchBlockRequest{ReqID: 9, ID: useResultResp.ResultID}
	fetchBlockResp, err := doWebSocket(ws, WSFetchBlock, &fetchBlockReq)
	assert.NoError(t, err)
	_, blockResult := parseblock.ParseBlock(fetchBlockResp[8:], useResultResp.FieldsTypes, fetchResp.Rows, useResultResp.Precision)
	assert.Equal(t, 1, len(blockResult))
	assert.Equal(t, float32(10.3), blockResult[0][1])
	assert.Equal(t, int32(218), blockResult[0][2])
	assert.Equal(t, float32(0.31), blockResult[0][3])

	// free result
	freeResultReq, _ := json.Marshal(FreeResultRequest{ReqID: 10, ID: useResultResp.ResultID})
	a, _ := json.Marshal(Request{Action: WSFreeResult, Args: freeResultReq})
	err = ws.WriteMessage(websocket.TextMessage, a)
	assert.NoError(t, err)

	// close
	closeReq := StmtCloseRequest{ReqID: 11, StmtID: prepareResp.StmtID}
	err = doWebSocketWithoutResp(ws, STMTClose, &closeReq)
	assert.NoError(t, err)
}

func TestStmtNumParams(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	db := "test_ws_stmt_num_params"
	code, message := doRestful(fmt.Sprintf("drop database if exists %s", db), "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf("create database if not exists %s", db), "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf("create stable if not exists %s.meters (ts timestamp,current float,voltage int,phase float) tags (groupid int,location varchar(24))", db), "")
	assert.Equal(t, 0, code, message)

	defer doRestful(fmt.Sprintf("drop database if exists %s", db), "")

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err := ws.Close()
		assert.NoError(t, err)
	}()

	// connect
	connReq := ConnRequest{ReqID: 1, User: "root", Password: "taosdata", DB: db}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp BaseResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// init
	initReq := map[string]uint64{"req_id": 2}
	resp, err = doWebSocket(ws, STMTInit, &initReq)
	assert.NoError(t, err)
	var initResp StmtInitResponse
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), initResp.ReqID)
	assert.Equal(t, 0, initResp.Code, initResp.Message)

	// prepare
	prepareReq := StmtPrepareRequest{
		ReqID:  3,
		StmtID: initResp.StmtID,
		SQL:    fmt.Sprintf("insert into d1 using %s.meters tags(?, ?) values (?, ?, ?, ?)", db),
	}
	resp, err = doWebSocket(ws, STMTPrepare, &prepareReq)
	assert.NoError(t, err)
	var prepareResp StmtPrepareResponse
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)

	// num params
	numParamsReq := StmtNumParamsRequest{ReqID: 4, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTNumParams, &numParamsReq)
	assert.NoError(t, err)
	var numParamsResp StmtNumParamsResponse
	err = json.Unmarshal(resp, &numParamsResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, numParamsResp.Code, numParamsResp.Message)
	assert.Equal(t, uint64(4), numParamsResp.ReqID)
	assert.Equal(t, 4, numParamsResp.NumParams)
}

func TestStmtGetParams(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	db := "test_ws_stmt_get_params"
	code, message := doRestful(fmt.Sprintf("drop database if exists %s", db), "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf("create database if not exists %s", db), "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf("create stable if not exists %s.meters (ts timestamp,current float,voltage int,phase float) tags (groupid int,location varchar(24))", db), "")
	assert.Equal(t, 0, code, message)

	defer doRestful(fmt.Sprintf("drop database if exists %s", db), "")

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err := ws.Close()
		assert.NoError(t, err)
	}()

	// connect
	connReq := ConnRequest{ReqID: 1, User: "root", Password: "taosdata", DB: db}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp BaseResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// init
	initReq := map[string]uint64{"req_id": 2}
	resp, err = doWebSocket(ws, STMTInit, &initReq)
	assert.NoError(t, err)
	var initResp StmtInitResponse
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), initResp.ReqID)
	assert.Equal(t, 0, initResp.Code, initResp.Message)

	// prepare
	prepareReq := StmtPrepareRequest{
		ReqID:  3,
		StmtID: initResp.StmtID,
		SQL:    fmt.Sprintf("insert into d1 using %s.meters tags(?, ?) values (?, ?, ?, ?)", db),
	}
	resp, err = doWebSocket(ws, STMTPrepare, &prepareReq)
	assert.NoError(t, err)
	var prepareResp StmtPrepareResponse
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)

	// get param
	getParamsReq := StmtGetParamRequest{ReqID: 4, StmtID: prepareResp.StmtID, Index: 0}
	resp, err = doWebSocket(ws, STMTGetParam, &getParamsReq)
	assert.NoError(t, err)
	var getParamsResp StmtGetParamResponse
	err = json.Unmarshal(resp, &getParamsResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, getParamsResp.Code, getParamsResp.Message)
	assert.Equal(t, uint64(4), getParamsResp.ReqID)
	assert.Equal(t, 0, getParamsResp.Index)
	assert.Equal(t, 9, getParamsResp.DataType)
	assert.Equal(t, 8, getParamsResp.Length)
}

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
		err := ws.Close()
		assert.NoError(t, err)
	}()

	// connect
	connReq := ConnRequest{ReqID: 1, User: "root", Password: "taosdata", DB: db}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp BaseResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// current db
	currentDBReq := map[string]uint64{"req_id": 1}
	resp, err = doWebSocket(ws, WSGetCurrentDB, &currentDBReq)
	assert.NoError(t, err)
	var currentDBResp GetCurrentDBResponse
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
		err := ws.Close()
		assert.NoError(t, err)
	}()

	// connect
	connReq := ConnRequest{ReqID: 1, User: "root", Password: "taosdata"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp BaseResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// server info
	serverInfoReq := map[string]uint64{"req_id": 1}
	resp, err = doWebSocket(ws, WSGetServerInfo, &serverInfoReq)
	assert.NoError(t, err)
	var serverInfoResp GetServerInfoResponse
	err = json.Unmarshal(resp, &serverInfoResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), serverInfoResp.ReqID)
	assert.Equal(t, 0, serverInfoResp.Code, serverInfoResp.Message)
	t.Log(serverInfoResp.Info)
}

func TestNumFields(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	db := "test_ws_num_fields"
	code, message := doRestful(fmt.Sprintf("drop database if exists %s", db), db)
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf("create database if not exists %s", db), db)
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf("create stable if not exists %s.meters (ts timestamp,current float,voltage int,phase float) tags (groupid int,location varchar(24))", db), db)
	assert.Equal(t, 0, code, message)
	code, message = doRestful("INSERT INTO d1 USING meters TAGS (1, 'location1') VALUES (now, 10.2, 219, 0.31) "+
		"d2 USING meters TAGS (2, 'location2') VALUES (now, 10.3, 220, 0.32)", db)
	assert.Equal(t, 0, code, message)

	defer doRestful(fmt.Sprintf("drop database if exists %s", db), "")

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err := ws.Close()
		assert.NoError(t, err)
	}()

	// connect
	connReq := ConnRequest{ReqID: 1, User: "root", Password: "taosdata", DB: db}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp BaseResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// query
	queryReq := QueryRequest{ReqID: 2, Sql: "select * from meters"}
	resp, err = doWebSocket(ws, WSQuery, &queryReq)
	assert.NoError(t, err)
	var queryResp QueryResponse
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), queryResp.ReqID)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// num fields
	numFieldsReq := NumFieldsRequest{ReqID: 3, ResultID: queryResp.ID}
	resp, err = doWebSocket(ws, WSNumFields, &numFieldsReq)
	assert.NoError(t, err)
	var numFieldsResp NumFieldsResponse
	err = json.Unmarshal(resp, &numFieldsResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), numFieldsResp.ReqID)
	assert.Equal(t, 0, numFieldsResp.Code, numFieldsResp.Message)
	assert.Equal(t, 6, numFieldsResp.NumFields)
}

func TestWsStmt2(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful("drop database if exists test_ws_stmt2_ws", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_stmt2_ws precision 'ns'", "")
	assert.Equal(t, 0, code, message)

	defer doRestful("drop database if exists test_ws_stmt2_ws", "")

	code, message = doRestful(
		"create table if not exists stb (ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100)) tags (info json)",
		"test_ws_stmt2_ws")
	assert.Equal(t, 0, code, message)

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err := ws.Close()
		assert.NoError(t, err)
	}()

	// connect
	connReq := ConnRequest{ReqID: 1, User: "root", Password: "taosdata", DB: "test_ws_stmt2_ws"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp BaseResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// init
	initReq := Stmt2InitRequest{
		ReqID:               0x123,
		SingleStbInsert:     false,
		SingleTableBindOnce: false,
	}
	resp, err = doWebSocket(ws, STMT2Init, &initReq)
	assert.NoError(t, err)
	var initResp Stmt2InitResponse
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0x123), initResp.ReqID)
	assert.Equal(t, 0, initResp.Code, initResp.Message)

	// prepare
	prepareReq := Stmt2PrepareRequest{ReqID: 3, StmtID: initResp.StmtID, SQL: "insert into ct1 using test_ws_stmt2_ws.stb tags (?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"}
	resp, err = doWebSocket(ws, STMT2Prepare, &prepareReq)
	assert.NoError(t, err)
	var prepareResp Stmt2PrepareResponse
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)
	assert.True(t, prepareResp.IsInsert)

	// get tag fields
	getTagFieldsReq := Stmt2GetFieldsRequest{ReqID: 5, StmtID: prepareResp.StmtID, FieldTypes: []int8{stmtCommon.TAOS_FIELD_TAG}}
	resp, err = doWebSocket(ws, STMT2GetFields, &getTagFieldsReq)
	assert.NoError(t, err)
	var getTagFieldsResp Stmt2GetFieldsResponse
	err = json.Unmarshal(resp, &getTagFieldsResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), getTagFieldsResp.ReqID)
	assert.Equal(t, 0, getTagFieldsResp.Code, getTagFieldsResp.Message)

	// get col fields
	getColFieldsReq := Stmt2GetFieldsRequest{ReqID: 6, StmtID: prepareResp.StmtID, FieldTypes: []int8{stmtCommon.TAOS_FIELD_COL}}
	resp, err = doWebSocket(ws, STMT2GetFields, &getColFieldsReq)
	assert.NoError(t, err)
	var getColFieldsResp Stmt2GetFieldsResponse
	err = json.Unmarshal(resp, &getColFieldsResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), getColFieldsResp.ReqID)
	assert.Equal(t, 0, getColFieldsResp.Code, getColFieldsResp.Message)

	// bind
	now := time.Now()
	cols := [][]driver.Value{
		// ts
		{now, now.Add(time.Second), now.Add(time.Second * 2)},
		// bool
		{true, false, nil},
		// tinyint
		{int8(2), int8(22), nil},
		// smallint
		{int16(3), int16(33), nil},
		// int
		{int32(4), int32(44), nil},
		// bigint
		{int64(5), int64(55), nil},
		// tinyint unsigned
		{uint8(6), uint8(66), nil},
		// smallint unsigned
		{uint16(7), uint16(77), nil},
		// int unsigned
		{uint32(8), uint32(88), nil},
		// bigint unsigned
		{uint64(9), uint64(99), nil},
		// float
		{float32(10), float32(1010), nil},
		// double
		{float64(11), float64(1111), nil},
		// binary
		{"binary", "binary2", nil},
		// nchar
		{"nchar", "nchar2", nil},
		// varbinary
		{[]byte{0xaa, 0xbb, 0xcc}, []byte{0xaa, 0xbb, 0xcc}, nil},
		// geometry
		{[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, nil},
	}
	tbName := "test_ws_stmt2_ws.ct1"
	tag := []driver.Value{"{\"a\":\"b\"}"}
	binds := &stmtCommon.TaosStmt2BindData{
		TableName: tbName,
		Tags:      tag,
		Cols:      cols,
	}
	bs, err := stmtCommon.MarshalStmt2Binary([]*stmtCommon.TaosStmt2BindData{binds}, true, getColFieldsResp.ColFields, getTagFieldsResp.TagFields)
	assert.NoError(t, err)
	bindReq := make([]byte, len(bs)+30)
	// req_id
	binary.LittleEndian.PutUint64(bindReq, 0x12345)
	// stmt_id
	binary.LittleEndian.PutUint64(bindReq[8:], prepareResp.StmtID)
	// action
	binary.LittleEndian.PutUint64(bindReq[16:], Stmt2BindMessage)
	// version
	binary.LittleEndian.PutUint16(bindReq[24:], Stmt2BindProtocolVersion1)
	// col_idx
	idx := int32(-1)
	binary.LittleEndian.PutUint32(bindReq[26:], uint32(idx))
	// data
	copy(bindReq[30:], bs)
	err = ws.WriteMessage(websocket.BinaryMessage, bindReq)
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	var bindResp Stmt2BindResponse
	err = json.Unmarshal(resp, &bindResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, bindResp.Code, bindResp.Message)

	//exec
	execReq := Stmt2ExecRequest{ReqID: 10, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMT2Exec, &execReq)
	assert.NoError(t, err)
	var execResp Stmt2ExecResponse
	err = json.Unmarshal(resp, &execResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), execResp.ReqID)
	assert.Equal(t, 0, execResp.Code, execResp.Message)
	assert.Equal(t, 3, execResp.Affected)

	// close
	closeReq := Stmt2CloseRequest{ReqID: 11, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMT2Close, &closeReq)
	assert.NoError(t, err)
	var closeResp Stmt2CloseResponse
	err = json.Unmarshal(resp, &closeResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(11), closeResp.ReqID)
	assert.Equal(t, 0, closeResp.Code, closeResp.Message)

	// query
	queryReq := QueryRequest{Sql: "select * from test_ws_stmt2_ws.stb"}
	resp, err = doWebSocket(ws, WSQuery, &queryReq)
	assert.NoError(t, err)
	var queryResp QueryResponse
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// fetch
	fetchReq := FetchRequest{ID: queryResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	var fetchResp FetchResponse
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)

	// fetch block
	fetchBlockReq := FetchBlockRequest{ID: queryResp.ID}
	fetchBlockResp, err := doWebSocket(ws, WSFetchBlock, &fetchBlockReq)
	assert.NoError(t, err)
	_, blockResult := parseblock.ParseBlock(fetchBlockResp[8:], queryResp.FieldsTypes, fetchResp.Rows, queryResp.Precision)
	assert.Equal(t, 3, len(blockResult))
	assert.Equal(t, now.UnixNano(), blockResult[0][0].(time.Time).UnixNano())

	assert.Equal(t, true, blockResult[0][1])
	assert.Equal(t, int8(2), blockResult[0][2])
	assert.Equal(t, int16(3), blockResult[0][3])
	assert.Equal(t, int32(4), blockResult[0][4])
	assert.Equal(t, int64(5), blockResult[0][5])
	assert.Equal(t, uint8(6), blockResult[0][6])
	assert.Equal(t, uint16(7), blockResult[0][7])
	assert.Equal(t, uint32(8), blockResult[0][8])
	assert.Equal(t, uint64(9), blockResult[0][9])
	assert.Equal(t, float32(10), blockResult[0][10])
	assert.Equal(t, float64(11), blockResult[0][11])
	assert.Equal(t, "binary", blockResult[0][12])
	assert.Equal(t, "nchar", blockResult[0][13])
	assert.Equal(t, []byte{0xaa, 0xbb, 0xcc}, blockResult[1][14])
	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, blockResult[0][15])

	assert.Equal(t, now.Add(time.Second).UnixNano(), blockResult[1][0].(time.Time).UnixNano())
	assert.Equal(t, false, blockResult[1][1])
	assert.Equal(t, int8(22), blockResult[1][2])
	assert.Equal(t, int16(33), blockResult[1][3])
	assert.Equal(t, int32(44), blockResult[1][4])
	assert.Equal(t, int64(55), blockResult[1][5])
	assert.Equal(t, uint8(66), blockResult[1][6])
	assert.Equal(t, uint16(77), blockResult[1][7])
	assert.Equal(t, uint32(88), blockResult[1][8])
	assert.Equal(t, uint64(99), blockResult[1][9])
	assert.Equal(t, float32(1010), blockResult[1][10])
	assert.Equal(t, float64(1111), blockResult[1][11])
	assert.Equal(t, "binary2", blockResult[1][12])
	assert.Equal(t, "nchar2", blockResult[1][13])
	assert.Equal(t, []byte{0xaa, 0xbb, 0xcc}, blockResult[1][14])
	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, blockResult[1][15])

	assert.Equal(t, now.Add(time.Second*2).UnixNano(), blockResult[2][0].(time.Time).UnixNano())
	for i := 1; i < 16; i++ {
		assert.Nil(t, blockResult[2][i])
	}

}

func TestStmt2Prepare(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful("drop database if exists test_ws_stmt2_prepare_ws", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_stmt2_prepare_ws precision 'ns'", "")
	assert.Equal(t, 0, code, message)

	defer doRestful("drop database if exists test_ws_stmt2_prepare_ws", "")

	code, message = doRestful(
		"create table if not exists stb (ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100)) tags (info json)",
		"test_ws_stmt2_prepare_ws")
	assert.Equal(t, 0, code, message)

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err := ws.Close()
		assert.NoError(t, err)
	}()

	// connect
	connReq := ConnRequest{ReqID: 1, User: "root", Password: "taosdata", DB: "test_ws_stmt2_prepare_ws"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp BaseResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// init
	initReq := Stmt2InitRequest{
		ReqID:               0x123,
		SingleStbInsert:     false,
		SingleTableBindOnce: false,
	}
	resp, err = doWebSocket(ws, STMT2Init, &initReq)
	assert.NoError(t, err)
	var initResp Stmt2InitResponse
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0x123), initResp.ReqID)
	assert.Equal(t, 0, initResp.Code, initResp.Message)

	// prepare
	prepareReq := Stmt2PrepareRequest{
		ReqID:     3,
		StmtID:    initResp.StmtID,
		SQL:       "insert into ctb using test_ws_stmt2_prepare_ws.stb tags (?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		GetFields: true,
	}
	resp, err = doWebSocket(ws, STMT2Prepare, &prepareReq)
	assert.NoError(t, err)
	var prepareResp Stmt2PrepareResponse
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)
	assert.Equal(t, true, prepareResp.IsInsert)
	names := [17]string{
		"info",
		"ts",
		"v1",
		"v2",
		"v3",
		"v4",
		"v5",
		"v6",
		"v7",
		"v8",
		"v9",
		"v10",
		"v11",
		"v12",
		"v13",
		"v14",
		"v15",
	}
	fieldTypes := [17]int8{
		common.TSDB_DATA_TYPE_JSON,
		common.TSDB_DATA_TYPE_TIMESTAMP,
		common.TSDB_DATA_TYPE_BOOL,
		common.TSDB_DATA_TYPE_TINYINT,
		common.TSDB_DATA_TYPE_SMALLINT,
		common.TSDB_DATA_TYPE_INT,
		common.TSDB_DATA_TYPE_BIGINT,
		common.TSDB_DATA_TYPE_UTINYINT,
		common.TSDB_DATA_TYPE_USMALLINT,
		common.TSDB_DATA_TYPE_UINT,
		common.TSDB_DATA_TYPE_UBIGINT,
		common.TSDB_DATA_TYPE_FLOAT,
		common.TSDB_DATA_TYPE_DOUBLE,
		common.TSDB_DATA_TYPE_BINARY,
		common.TSDB_DATA_TYPE_NCHAR,
		common.TSDB_DATA_TYPE_VARBINARY,
		common.TSDB_DATA_TYPE_GEOMETRY,
	}
	assert.True(t, prepareResp.IsInsert)
	assert.Equal(t, 17, len(prepareResp.Fields))
	for i := 0; i < 17; i++ {
		assert.Equal(t, names[i], prepareResp.Fields[i].Name)
		assert.Equal(t, fieldTypes[i], prepareResp.Fields[i].FieldType)
	}
	// prepare query
	prepareReq = Stmt2PrepareRequest{
		ReqID:     4,
		StmtID:    initResp.StmtID,
		SQL:       "select * from test_ws_stmt2_prepare_ws.stb where ts = ? and v1 = ?",
		GetFields: true,
	}
	resp, err = doWebSocket(ws, STMT2Prepare, &prepareReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)
	assert.Equal(t, false, prepareResp.IsInsert)
	assert.Nil(t, prepareResp.Fields)
	assert.Equal(t, 2, prepareResp.FieldsCount)
}

func TestStmt2GetFields(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful("drop database if exists test_ws_stmt2_getfields_ws", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_stmt2_getfields_ws precision 'ns'", "")
	assert.Equal(t, 0, code, message)

	defer doRestful("drop database if exists test_ws_stmt2_getfields_ws", "")

	code, message = doRestful(
		"create table if not exists stb (ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100)) tags (info json)",
		"test_ws_stmt2_getfields_ws")
	assert.Equal(t, 0, code, message)

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err := ws.Close()
		assert.NoError(t, err)
	}()

	// connect
	connReq := ConnRequest{ReqID: 1, User: "root", Password: "taosdata", DB: "test_ws_stmt2_getfields_ws"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp BaseResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// init
	initReq := Stmt2InitRequest{
		ReqID:               0x123,
		SingleStbInsert:     false,
		SingleTableBindOnce: false,
	}
	resp, err = doWebSocket(ws, STMT2Init, &initReq)
	assert.NoError(t, err)
	var initResp Stmt2InitResponse
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0x123), initResp.ReqID)
	assert.Equal(t, 0, initResp.Code, initResp.Message)

	// prepare
	prepareReq := Stmt2PrepareRequest{
		ReqID:     3,
		StmtID:    initResp.StmtID,
		SQL:       "insert into ctb using test_ws_stmt2_getfields_ws.stb tags (?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		GetFields: false,
	}
	resp, err = doWebSocket(ws, STMT2Prepare, &prepareReq)
	assert.NoError(t, err)
	var prepareResp Stmt2PrepareResponse
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)
	assert.Equal(t, true, prepareResp.IsInsert)

	// get fields
	getFieldsReq := Stmt2GetFieldsRequest{
		ReqID:  4,
		StmtID: prepareResp.StmtID,
		FieldTypes: []int8{
			stmtCommon.TAOS_FIELD_TAG,
			stmtCommon.TAOS_FIELD_COL,
		},
	}
	resp, err = doWebSocket(ws, STMT2GetFields, &getFieldsReq)
	assert.NoError(t, err)
	var getFieldsResp Stmt2GetFieldsResponse
	err = json.Unmarshal(resp, &getFieldsResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), getFieldsResp.ReqID)
	assert.Equal(t, 0, getFieldsResp.Code, getFieldsResp.Message)
	names := [16]string{
		"ts",
		"v1",
		"v2",
		"v3",
		"v4",
		"v5",
		"v6",
		"v7",
		"v8",
		"v9",
		"v10",
		"v11",
		"v12",
		"v13",
		"v14",
		"v15",
	}
	fieldTypes := [16]int8{
		common.TSDB_DATA_TYPE_TIMESTAMP,
		common.TSDB_DATA_TYPE_BOOL,
		common.TSDB_DATA_TYPE_TINYINT,
		common.TSDB_DATA_TYPE_SMALLINT,
		common.TSDB_DATA_TYPE_INT,
		common.TSDB_DATA_TYPE_BIGINT,
		common.TSDB_DATA_TYPE_UTINYINT,
		common.TSDB_DATA_TYPE_USMALLINT,
		common.TSDB_DATA_TYPE_UINT,
		common.TSDB_DATA_TYPE_UBIGINT,
		common.TSDB_DATA_TYPE_FLOAT,
		common.TSDB_DATA_TYPE_DOUBLE,
		common.TSDB_DATA_TYPE_BINARY,
		common.TSDB_DATA_TYPE_NCHAR,
		common.TSDB_DATA_TYPE_VARBINARY,
		common.TSDB_DATA_TYPE_GEOMETRY,
	}
	assert.Equal(t, 16, len(getFieldsResp.ColFields))
	assert.Equal(t, 1, len(getFieldsResp.TagFields))
	for i := 0; i < 16; i++ {
		assert.Equal(t, names[i], getFieldsResp.ColFields[i].Name)
		assert.Equal(t, fieldTypes[i], getFieldsResp.ColFields[i].FieldType)
	}
	assert.Equal(t, "info", getFieldsResp.TagFields[0].Name)
	assert.Equal(t, int8(common.TSDB_DATA_TYPE_JSON), getFieldsResp.TagFields[0].FieldType)

	// prepare get tablename
	prepareReq = Stmt2PrepareRequest{
		ReqID:     5,
		StmtID:    initResp.StmtID,
		SQL:       "insert into ? using test_ws_stmt2_getfields_ws.stb tags (?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		GetFields: false,
	}
	resp, err = doWebSocket(ws, STMT2Prepare, &prepareReq)
	assert.NoError(t, err)

	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)
	assert.Equal(t, true, prepareResp.IsInsert)
	// get fields
	getFieldsReq = Stmt2GetFieldsRequest{
		ReqID:  6,
		StmtID: prepareResp.StmtID,
		FieldTypes: []int8{
			stmtCommon.TAOS_FIELD_TBNAME,
		},
	}
	resp, err = doWebSocket(ws, STMT2GetFields, &getFieldsReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &getFieldsResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), getFieldsResp.ReqID)
	assert.Equal(t, 0, getFieldsResp.Code, getFieldsResp.Message)

	assert.Nil(t, getFieldsResp.ColFields)
	assert.Nil(t, getFieldsResp.TagFields)
	assert.Equal(t, int32(1), getFieldsResp.TableCount)

	// prepare query
	prepareReq = Stmt2PrepareRequest{
		ReqID:     7,
		StmtID:    initResp.StmtID,
		SQL:       "select * from test_ws_stmt2_getfields_ws.stb where ts = ? and v1 = ?",
		GetFields: false,
	}
	resp, err = doWebSocket(ws, STMT2Prepare, &prepareReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(7), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)
	assert.Equal(t, false, prepareResp.IsInsert)
	// get fields
	getFieldsReq = Stmt2GetFieldsRequest{
		ReqID:  8,
		StmtID: prepareResp.StmtID,
		FieldTypes: []int8{
			stmtCommon.TAOS_FIELD_QUERY,
		},
	}
	resp, err = doWebSocket(ws, STMT2GetFields, &getFieldsReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &getFieldsResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(8), getFieldsResp.ReqID)
	assert.Equal(t, 0, getFieldsResp.Code, getFieldsResp.Message)

	assert.Nil(t, getFieldsResp.ColFields)
	assert.Nil(t, getFieldsResp.TagFields)
	assert.Equal(t, int32(2), getFieldsResp.QueryCount)

}

func TestStmt2Query(t *testing.T) {
	//for stable
	prepareDataSql := []string{
		"create stable meters (ts timestamp,current float,voltage int,phase float) tags (group_id int, location varchar(24))",
		"insert into d0 using meters tags (2, 'California.SanFrancisco') values ('2023-09-13 17:53:52.123', 10.2, 219, 0.32) ",
		"insert into d1 using meters tags (1, 'California.SanFrancisco') values ('2023-09-13 17:54:43.321', 10.3, 218, 0.31) ",
	}
	Stmt2Query(t, "test_ws_stmt2_query_for_stable", prepareDataSql)

	// for table
	prepareDataSql = []string{
		"create table meters (ts timestamp,current float,voltage int,phase float, group_id int, location varchar(24))",
		"insert into meters values ('2023-09-13 17:53:52.123', 10.2, 219, 0.32, 2, 'California.SanFrancisco') ",
		"insert into meters values ('2023-09-13 17:54:43.321', 10.3, 218, 0.31, 1, 'California.SanFrancisco') ",
	}
	Stmt2Query(t, "test_ws_stmt2_query_for_table", prepareDataSql)
}

func Stmt2Query(t *testing.T, db string, prepareDataSql []string) {
	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful(fmt.Sprintf("drop database if exists %s", db), "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf("create database if not exists %s", db), "")
	assert.Equal(t, 0, code, message)

	defer doRestful(fmt.Sprintf("drop database if exists %s", db), "")

	for _, sql := range prepareDataSql {
		code, message = doRestful(sql, db)
		assert.Equal(t, 0, code, message)
	}

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err := ws.Close()
		assert.NoError(t, err)
	}()

	// connect
	connReq := ConnRequest{ReqID: 1, User: "root", Password: "taosdata", DB: db}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp BaseResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// init
	initReq := map[string]uint64{"req_id": 2}
	resp, err = doWebSocket(ws, STMT2Init, &initReq)
	assert.NoError(t, err)
	var initResp Stmt2InitResponse
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), initResp.ReqID)
	assert.Equal(t, 0, initResp.Code, initResp.Message)

	// prepare
	prepareReq := Stmt2PrepareRequest{
		ReqID:     3,
		StmtID:    initResp.StmtID,
		SQL:       fmt.Sprintf("select * from %s.meters where group_id=? and location=?", db),
		GetFields: false,
	}
	resp, err = doWebSocket(ws, STMT2Prepare, &prepareReq)
	assert.NoError(t, err)
	var prepareResp Stmt2PrepareResponse
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)
	assert.False(t, prepareResp.IsInsert)

	// bind
	var block bytes.Buffer
	wstool.WriteUint64(&block, 5)
	wstool.WriteUint64(&block, prepareResp.StmtID)
	wstool.WriteUint64(&block, uint64(Stmt2BindMessage))
	wstool.WriteUint16(&block, Stmt2BindProtocolVersion1)
	idx := int32(-1)
	wstool.WriteUint32(&block, uint32(idx))
	params := []*stmtCommon.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{int32(1)},
				{"California.SanFrancisco"},
			},
		},
	}
	b, err := stmtCommon.MarshalStmt2Binary(params, false, nil, nil)
	block.Write(b)

	err = ws.WriteMessage(websocket.BinaryMessage, block.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	var bindResp Stmt2BindResponse
	err = json.Unmarshal(resp, &bindResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), bindResp.ReqID)
	assert.Equal(t, 0, bindResp.Code, bindResp.Message)

	// exec
	execReq := StmtExecRequest{ReqID: 6, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMT2Exec, &execReq)
	assert.NoError(t, err)
	var execResp StmtExecResponse
	err = json.Unmarshal(resp, &execResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), execResp.ReqID)
	assert.Equal(t, 0, execResp.Code, execResp.Message)

	// use result
	useResultReq := Stmt2UseResultRequest{ReqID: 7, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMT2Result, &useResultReq)
	assert.NoError(t, err)
	var useResultResp Stmt2UseResultResponse
	err = json.Unmarshal(resp, &useResultResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(7), useResultResp.ReqID)
	assert.Equal(t, 0, useResultResp.Code, useResultResp.Message)

	// fetch
	fetchReq := FetchRequest{ReqID: 8, ID: useResultResp.ResultID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	var fetchResp FetchResponse
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(8), fetchResp.ReqID)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)
	assert.Equal(t, 1, fetchResp.Rows)

	// fetch block
	fetchBlockReq := FetchBlockRequest{ReqID: 9, ID: useResultResp.ResultID}
	fetchBlockResp, err := doWebSocket(ws, WSFetchBlock, &fetchBlockReq)
	assert.NoError(t, err)
	_, blockResult := parseblock.ParseBlock(fetchBlockResp[8:], useResultResp.FieldsTypes, fetchResp.Rows, useResultResp.Precision)
	assert.Equal(t, 1, len(blockResult))
	assert.Equal(t, float32(10.3), blockResult[0][1])
	assert.Equal(t, int32(218), blockResult[0][2])
	assert.Equal(t, float32(0.31), blockResult[0][3])

	// free result
	freeResultReq, _ := json.Marshal(FreeResultRequest{ReqID: 10, ID: useResultResp.ResultID})
	a, _ := json.Marshal(Request{Action: WSFreeResult, Args: freeResultReq})
	err = ws.WriteMessage(websocket.TextMessage, a)
	assert.NoError(t, err)

	// close
	closeReq := StmtCloseRequest{ReqID: 11, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMT2Close, &closeReq)
	assert.NoError(t, err)
	var closeResp Stmt2CloseResponse
	err = json.Unmarshal(resp, &fetchResp)
	assert.Equal(t, 0, closeResp.Code, closeResp.Message)
}

func TestWSConnect(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err := ws.Close()
		assert.NoError(t, err)
	}()

	// wrong password
	connReq := ConnRequest{ReqID: 1, User: "root", Password: "wrong"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp BaseResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, "Authentication failure", connResp.Message)
	assert.Equal(t, 0x357, connResp.Code, connResp.Message)

	// connect
	connReq = ConnRequest{ReqID: 1, User: "root", Password: "taosdata"}
	resp, err = doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)
	//duplicate connections
	connReq = ConnRequest{ReqID: 1, User: "root", Password: "taosdata"}
	resp, err = doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0xffff, connResp.Code)
	assert.Equal(t, "duplicate connections", connResp.Message)

}

type TestConnRequest struct {
	ReqID    uint64 `json:"req_id"`
	User     string `json:"user"`
	Password string `json:"password"`
	DB       string `json:"db"`
	Mode     int    `json:"mode"`
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
		err := ws.Close()
		assert.NoError(t, err)
	}()

	wrongMode := 999
	connReq := TestConnRequest{ReqID: 1, User: "root", Password: "taosdata", Mode: wrongMode}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp BaseResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0xffff, connResp.Code)
	assert.Equal(t, fmt.Sprintf("unexpected mode:%d", wrongMode), connResp.Message)

	//bi
	biMode := 0
	connReq = TestConnRequest{ReqID: 1, User: "root", Password: "taosdata", Mode: biMode}
	resp, err = doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

}

func TestStmtBinary(t *testing.T) {

}

func TestWSTMQWriteRaw(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err := ws.Close()
		assert.NoError(t, err)
	}()

	data := []byte{
		0x64, 0x01, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x58, 0x01, 0x00, 0x00, 0x04, 0x73, 0x74, 0x62,
		0x00, 0xd5, 0xf0, 0xed, 0x8a, 0xe0, 0x23, 0xf3, 0x45, 0x00, 0x1c, 0x02, 0x09, 0x01, 0x10, 0x02,
		0x03, 0x74, 0x73, 0x00, 0x01, 0x01, 0x02, 0x04, 0x03, 0x63, 0x31, 0x00, 0x02, 0x01, 0x02, 0x06,
		0x03, 0x63, 0x32, 0x00, 0x03, 0x01, 0x04, 0x08, 0x03, 0x63, 0x33, 0x00, 0x04, 0x01, 0x08, 0x0a,
		0x03, 0x63, 0x34, 0x00, 0x05, 0x01, 0x10, 0x0c, 0x03, 0x63, 0x35, 0x00, 0x0b, 0x01, 0x02, 0x0e,
		0x03, 0x63, 0x36, 0x00, 0x0c, 0x01, 0x04, 0x10, 0x03, 0x63, 0x37, 0x00, 0x0d, 0x01, 0x08, 0x12,
		0x03, 0x63, 0x38, 0x00, 0x0e, 0x01, 0x10, 0x14, 0x03, 0x63, 0x39, 0x00, 0x06, 0x01, 0x08, 0x16,
		0x04, 0x63, 0x31, 0x30, 0x00, 0x07, 0x01, 0x10, 0x18, 0x04, 0x63, 0x31, 0x31, 0x00, 0x08, 0x01,
		0x2c, 0x1a, 0x04, 0x63, 0x31, 0x32, 0x00, 0x0a, 0x01, 0xa4, 0x01, 0x1c, 0x04, 0x63, 0x31, 0x33,
		0x00, 0x1c, 0x02, 0x09, 0x02, 0x10, 0x1e, 0x04, 0x74, 0x74, 0x73, 0x00, 0x01, 0x00, 0x02, 0x20,
		0x04, 0x74, 0x63, 0x31, 0x00, 0x02, 0x00, 0x02, 0x22, 0x04, 0x74, 0x63, 0x32, 0x00, 0x03, 0x00,
		0x04, 0x24, 0x04, 0x74, 0x63, 0x33, 0x00, 0x04, 0x00, 0x08, 0x26, 0x04, 0x74, 0x63, 0x34, 0x00,
		0x05, 0x00, 0x10, 0x28, 0x04, 0x74, 0x63, 0x35, 0x00, 0x0b, 0x00, 0x02, 0x2a, 0x04, 0x74, 0x63,
		0x36, 0x00, 0x0c, 0x00, 0x04, 0x2c, 0x04, 0x74, 0x63, 0x37, 0x00, 0x0d, 0x00, 0x08, 0x2e, 0x04,
		0x74, 0x63, 0x38, 0x00, 0x0e, 0x00, 0x10, 0x30, 0x04, 0x74, 0x63, 0x39, 0x00, 0x06, 0x00, 0x08,
		0x32, 0x05, 0x74, 0x63, 0x31, 0x30, 0x00, 0x07, 0x00, 0x10, 0x34, 0x05, 0x74, 0x63, 0x31, 0x31,
		0x00, 0x08, 0x00, 0x2c, 0x36, 0x05, 0x74, 0x63, 0x31, 0x32, 0x00, 0x0a, 0x00, 0xa4, 0x01, 0x38,
		0x05, 0x74, 0x63, 0x31, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x1c, 0x02, 0x02, 0x02,
		0x01, 0x00, 0x02, 0x04, 0x02, 0x01, 0x00, 0x03, 0x06, 0x02, 0x01, 0x00, 0x01, 0x08, 0x02, 0x01,
		0x00, 0x01, 0x0a, 0x02, 0x01, 0x00, 0x01, 0x0c, 0x02, 0x01, 0x00, 0x01, 0x0e, 0x02, 0x01, 0x00,
		0x01, 0x10, 0x02, 0x01, 0x00, 0x01, 0x12, 0x02, 0x01, 0x00, 0x01, 0x14, 0x02, 0x01, 0x00, 0x01,
		0x16, 0x02, 0x01, 0x00, 0x04, 0x18, 0x02, 0x01, 0x00, 0x04, 0x1a, 0x02, 0x01, 0x00, 0xff, 0x1c,
		0x02, 0x01, 0x00, 0xff,
	}
	length := uint32(356)
	metaType := uint16(531)
	code, message := doRestful("create database if not exists test_ws_tmq_write_raw", "")
	assert.Equal(t, 0, code, message)
	defer func() {
		code, message := doRestful("drop database if exists test_ws_tmq_write_raw", "")
		assert.Equal(t, 0, code, message)
	}()
	// connect
	connReq := ConnRequest{ReqID: 1, User: "root", Password: "taosdata", DB: "test_ws_tmq_write_raw"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp BaseResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)
	buffer := bytes.Buffer{}
	wstool.WriteUint64(&buffer, 2) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(TMQRawMessage))
	wstool.WriteUint32(&buffer, length)
	wstool.WriteUint16(&buffer, metaType)
	buffer.Write(data)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	var tmqResp BaseResponse
	err = json.Unmarshal(resp, &tmqResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), tmqResp.ReqID)
	assert.Equal(t, 0, tmqResp.Code, tmqResp.Message)

	d := restQuery("describe stb", "test_ws_tmq_write_raw")
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
	for rowIndex, values := range d.Data {
		for i := 0; i < 4; i++ {
			assert.Equal(t, expect[rowIndex][i], values[i])
		}
	}
}

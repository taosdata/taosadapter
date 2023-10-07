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

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	_ "github.com/taosdata/taosadapter/v3/controller/rest"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/tools/parseblock"
	"github.com/taosdata/taosadapter/v3/version"
)

var router *gin.Engine

func TestMain(m *testing.M) {
	viper.Set("monitor.writeToTD", true)
	viper.Set("monitor.writeInterval", time.Millisecond*10)
	viper.Set("pool.maxConnect", 10000)
	viper.Set("pool.maxIdle", 10000)
	viper.Set("logLevel", "debug")
	config.Init()
	db.PrepareConnection()
	log.ConfigLog()
	gin.SetMode(gin.ReleaseMode)
	router = gin.New()
	controllers := controller.GetControllers()
	for _, webController := range controllers {
		webController.Init(router)
	}
	monitor.StartMonitor()
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
	code, message := doRestful("drop database if exists test_ws_query", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_query", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(
		"create table if not exists stb1 (ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20)) tags (info json)",
		"test_ws_query")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(
		"insert into t1 using stb1 tags ('{\"table\":\"t1\"}') values (now-2s,true,2,3,4,5,6,7,8,9,10,11,'中文\"binary','中文nchar')(now-1s,false,12,13,14,15,16,17,18,19,110,111,'中文\"binary','中文nchar')(now,null,null,null,null,null,null,null,null,null,null,null,null,null)",
		"test_ws_query")
	assert.Equal(t, 0, code, message)

	code, message = doRestful("create table t2 using stb1 tags('{\"table\":\"t2\"}')", "test_ws_query")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create table t3 using stb1 tags('{\"table\":\"t3\"}')", "test_ws_query")
	assert.Equal(t, 0, code, message)

	defer doRestful("drop database if exists test_ws_query", "")

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
}

func TestWsSchemaless(t *testing.T) {
	code, message := doRestful("drop database if exists test_ws_schemaless", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_schemaless", "")
	assert.Equal(t, 0, code, message)

	defer doRestful("drop database if exists test_ws_schemaless", "")

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

	cases := []struct {
		name      string
		protocol  int
		precision string
		data      string
		ttl       int
		code      int
	}{
		{
			name:      "influxdb",
			protocol:  1,
			precision: "ms",
			data: "measurement,host=host1 field1=2i,field2=2.0 1577837300000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837400000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837500000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837600000",
			ttl:  1000,
			code: 0,
		},
		{
			name:      "opentsdb_telnet",
			protocol:  2,
			precision: "ms",
			data: "meters.current 1648432611249 10.3 location=California.SanFrancisco group=2\n" +
				"meters.current 1648432611250 12.6 location=California.SanFrancisco group=2\n" +
				"meters.current 1648432611249 10.8 location=California.LosAngeles group=3\n" +
				"meters.current 1648432611250 11.3 location=California.LosAngeles group=3\n" +
				"meters.voltage 1648432611249 219 location=California.SanFrancisco group=2\n" +
				"meters.voltage 1648432611250 218 location=California.SanFrancisco group=2\n" +
				"meters.voltage 1648432611249 221 location=California.LosAngeles group=3\n" +
				"meters.voltage 1648432611250 217 location=California.LosAngeles group=3",
			ttl:  1000,
			code: 0,
		},
		{
			name:      "opentsdb_json",
			protocol:  3,
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
			ttl:  100,
			code: 0,
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
				ReqID:     reqID,
				Protocol:  c.protocol,
				Precision: c.precision,
				TTL:       c.ttl,
				Data:      c.data,
			}
			resp, err = doWebSocket(ws, SchemalessWrite, &req)
			assert.NoError(t, err)
			var schemalessResp BaseResponse
			err = json.Unmarshal(resp, &schemalessResp)
			assert.NoError(t, err)
			assert.Equal(t, reqID, schemalessResp.ReqID)
			assert.Equal(t, 0, schemalessResp.Code, schemalessResp.Message)
		})
	}
}

func TestWsStmt(t *testing.T) {
	code, message := doRestful("drop database if exists test_ws_stmt", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_stmt precision 'ns'", "")
	assert.Equal(t, 0, code, message)

	defer doRestful("drop database if exists test_ws_stmt", "")

	code, message = doRestful(
		"create table if not exists stb (ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20)) tags (info json)",
		"test_ws_stmt")
	assert.Equal(t, 0, code, message)

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
	connReq := ConnRequest{ReqID: 1, User: "root", Password: "taosdata", DB: "test_ws_stmt"}
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
	prepareReq := StmtPrepareRequest{ReqID: 3, StmtID: initResp.StmtID, SQL: "insert into ? using test_ws_stmt.stb tags (?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"}
	resp, err = doWebSocket(ws, STMTPrepare, &prepareReq)
	assert.NoError(t, err)
	var prepareResp StmtPrepareResponse
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)

	// set table name
	setTableNameReq := StmtSetTableNameRequest{ReqID: 4, StmtID: prepareResp.StmtID, Name: "test_ws_stmt.ct1"}
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
	resp, err = doWebSocket(ws, STMTClose, &closeReq)
	assert.NoError(t, err)
	var closeResp BaseResponse
	err = json.Unmarshal(resp, &closeResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(11), closeResp.ReqID)
	assert.Equal(t, 0, closeResp.Code, closeResp.Message)

	// query
	queryReq := QueryRequest{Sql: "select * from test_ws_stmt.stb"}
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

	// block message
	// init
	resp, err = doWebSocket(ws, STMTInit, nil)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, initResp.Code, initResp.Message)

	// prepare
	prepareReq = StmtPrepareRequest{StmtID: initResp.StmtID, SQL: "insert into ? using test_ws_stmt.stb tags(?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"}
	resp, err = doWebSocket(ws, STMTPrepare, &prepareReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)

	// set table name
	setTableNameReq = StmtSetTableNameRequest{StmtID: prepareResp.StmtID, Name: "test_ws_stmt.ct2"}
	resp, err = doWebSocket(ws, STMTSetTableName, &setTableNameReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &setTableNameResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, setTableNameResp.Code, setTableNameResp.Message)

	// set tags
	setTagsReq = StmtSetTagsRequest{StmtID: prepareResp.StmtID, Tags: json.RawMessage(`["{\"c\":\"d\"}"]`)}
	resp, err = doWebSocket(ws, STMTSetTags, &setTagsReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &setTagsResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, setTagsResp.Code, setTagsResp.Message)

	// bind binary
	var block bytes.Buffer
	wstool.WriteUint64(&block, 10)
	wstool.WriteUint64(&block, prepareResp.StmtID)
	wstool.WriteUint64(&block, 2)
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
	binary.LittleEndian.PutUint64(rawBlock[155:], uint64(now.UnixNano()))
	binary.LittleEndian.PutUint64(rawBlock[163:], uint64(now.Add(time.Second).UnixNano()))
	binary.LittleEndian.PutUint64(rawBlock[171:], uint64(now.Add(time.Second*2).UnixNano()))
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
	queryReq = QueryRequest{Sql: "select * from test_ws_stmt.ct2"}
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
}

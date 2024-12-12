package query

import (
	"bytes"
	"database/sql/driver"
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
	"github.com/taosdata/taosadapter/v3/tools/parseblock"
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

// @author: xftan
// @date: 2022/2/22 14:42
// @description: test websocket bulk pulling
func TestWebsocket(t *testing.T) {
	now := time.Now().Local().UnixNano() / 1e6
	code, message := doRestful("create database if not exists test_ws WAL_RETENTION_PERIOD 86400", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("drop table if exists test_ws", "test_ws")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create table if not exists test_ws(ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20)) tags (info json)", "test_ws")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf(`insert into t1 using test_ws tags('{"table":"t1"}') values (%d,true,2,3,4,5,6,7,8,9,10,11,'中文"binary','中文nchar')(%d,false,12,13,14,15,16,17,18,19,110,111,'中文"binary','中文nchar')(%d,null,null,null,null,null,null,null,null,null,null,null,null,null)`, now, now+1, now+3), "test_ws")
	assert.Equal(t, 0, code, message)
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
		AfterConnect    = 1
		AfterQuery      = 2
		AfterFetch      = 3
		AfterFetchBlock = 5
		AfterVersion    = 6
	)

	status := 0
	var lengths []int
	var queryResult *WSQueryResult
	var rows int
	//total := 0
	finish := make(chan struct{})
	//var jsonResult [][]interface{}
	var resultID uint64
	var blockResult [][]driver.Value
	testMessageHandler := func(_ int, message []byte) error {
		//json
		switch status {
		case AfterConnect:
			var d WSConnectResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSConnect, d.Code, d.Message)
			}
			//query
			status = AfterQuery
			b, _ := json.Marshal(&WSQueryReq{
				ReqID: 2,
				SQL:   "select test_ws.*,info->'table' from test_ws",
			})
			action, _ := json.Marshal(&WSAction{
				Action: "query",
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterQuery:
			var d WSQueryResult
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSQuery, d.Code, d.Message)
			}
			queryResult = &d
			status = AfterFetch
			//fetch
			b, _ := json.Marshal(&WSFetchReq{
				ReqID: 3,
				ID:    queryResult.ID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: WSFetch,
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterFetch:
			var d WSFetchResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSFetch, d.Code, d.Message)
			}
			lengths = d.Lengths
			rows = d.Rows
			if d.Completed {
				status = AfterVersion
				action, _ := json.Marshal(&WSAction{
					Action: wstool.ClientVersion,
					Args:   nil,
				})
				err = ws.WriteMessage(
					websocket.TextMessage,
					action,
				)
				return nil
			}

			status = AfterFetchBlock
			b, _ := json.Marshal(&WSFetchBlockReq{
				ReqID: 4,
				ID:    queryResult.ID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: WSFetchBlock,
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterFetchBlock:
			//block
			resultID, blockResult = parseblock.ParseBlock(message[8:], queryResult.FieldsTypes, rows, queryResult.Precision)

			_ = lengths
			status = AfterFetch
			b, _ := json.Marshal(&WSFetchReq{
				ReqID: 3,
				ID:    queryResult.ID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: WSFetch,
				Args:   b,
			})
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

	connect := &WSConnectReq{
		ReqID:    0,
		User:     "root",
		Password: "taosdata",
		DB:       "test_ws",
	}

	b, _ := json.Marshal(connect)
	action, _ := json.Marshal(&WSAction{
		Action: WSConnect,
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
	assert.Equal(t, uint64(1), resultID)
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
	assert.Equal(t, []byte(`{"table":"t1"}`), blockResult[0][14])
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
	assert.Equal(t, []byte(`{"table":"t1"}`), blockResult[1][14])
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
	assert.Equal(t, []byte(`{"table":"t1"}`), blockResult[2][14])
	code, message = doRestful("drop database if exists test_ws", "")
	assert.Equal(t, 0, code, message)
}

func TestWriteBlock(t *testing.T) {
	now := time.Now().Local().UnixNano() / 1e6
	code, message := doRestful("create database if not exists test_ws_write_block WAL_RETENTION_PERIOD 86400", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("drop table if exists test_ws_write_block", "test_ws_write_block")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create table if not exists test_ws_write_block(ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20)) tags (info json)", "test_ws_write_block")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf(`insert into t1 using test_ws_write_block tags('{"table":"t1"}') values (%d,true,2,3,4,5,6,7,8,9,10,11,'中文"binary','中文nchar')(%d,false,12,13,14,15,16,17,18,19,110,111,'中文"binary','中文nchar')(%d,null,null,null,null,null,null,null,null,null,null,null,null,null)`, now, now+1, now+3), "test_ws_write_block")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(`create table t2 using test_ws_write_block tags('{"table":"t2"}')`, "test_ws_write_block")
	assert.Equal(t, 0, code, message)

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	const (
		AfterConnect       = 1
		AfterQuery         = 2
		AfterFetch         = 3
		AfterWriteRawBlock = 4
		AfterFetchBlock    = 5
		AfterVersion       = 6
	)

	status := 0
	var queryResult *WSQueryResult
	var rows int
	finish := make(chan struct{})
	testMessageHandler := func(_ int, message []byte) error {
		//json
		switch status {
		case AfterConnect:
			var d WSConnectResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSConnect, d.Code, d.Message)
			}
			//query
			status = AfterQuery
			b, _ := json.Marshal(&WSQueryReq{
				ReqID: 2,
				SQL:   "select * from t1",
			})
			action, _ := json.Marshal(&WSAction{
				Action: "query",
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterQuery:
			var d WSQueryResult
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSQuery, d.Code, d.Message)
			}
			queryResult = &d
			status = AfterFetch
			//fetch
			b, _ := json.Marshal(&WSFetchReq{
				ReqID: 3,
				ID:    queryResult.ID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: WSFetch,
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterFetch:
			var d WSFetchResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSFetch, d.Code, d.Message)
			}
			rows = d.Rows
			if d.Completed {
				status = AfterVersion
				action, _ := json.Marshal(&WSAction{
					Action: wstool.ClientVersion,
					Args:   nil,
				})
				err = ws.WriteMessage(
					websocket.TextMessage,
					action,
				)
				return nil
			}

			status = AfterFetchBlock
			b, _ := json.Marshal(&WSFetchBlockReq{
				ReqID: 4,
				ID:    queryResult.ID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: WSFetchBlock,
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterFetchBlock:
			//block
			buffer := &bytes.Buffer{}
			// req id
			wstool.WriteUint64(buffer, 300)
			// message id
			wstool.WriteUint64(buffer, 400)
			// action
			wstool.WriteUint64(buffer, RawBlockMessage)
			// rows
			wstool.WriteUint32(buffer, uint32(rows))
			// table name length
			wstool.WriteUint16(buffer, uint16(2))
			// table name
			buffer.WriteString("t2")
			// raw block
			buffer.Write(message[16:])
			status = AfterWriteRawBlock
			err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
			if err != nil {
				return err
			}
		case AfterWriteRawBlock:
			var d WSWriteRawBlockResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSWriteRawBlock, d.Code, d.Message)
			}
			status = AfterFetch
			b, _ := json.Marshal(&WSFetchReq{
				ReqID: 3,
				ID:    queryResult.ID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: WSFetch,
				Args:   b,
			})
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

	connect := &WSConnectReq{
		ReqID:    0,
		User:     "root",
		Password: "taosdata",
		DB:       "test_ws_write_block",
	}

	b, _ := json.Marshal(connect)
	action, _ := json.Marshal(&WSAction{
		Action: WSConnect,
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
	err = ws.Close()
	assert.NoError(t, err)
	ws, _, err = websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	var blockResult [][]driver.Value
	testMessageHandler2 := func(_ int, message []byte) error {
		switch status {
		case AfterConnect:
			var d WSConnectResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSConnect, d.Code, d.Message)
			}
			//query
			status = AfterQuery
			b, _ := json.Marshal(&WSQueryReq{
				ReqID: 2,
				SQL:   "select * from t2",
			})
			action, _ := json.Marshal(&WSAction{
				Action: "query",
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterQuery:
			var d WSQueryResult
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSQuery, d.Code, d.Message)
			}
			queryResult = &d
			status = AfterFetch
			//fetch
			b, _ := json.Marshal(&WSFetchReq{
				ReqID: 3,
				ID:    queryResult.ID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: WSFetch,
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterFetch:
			var d WSFetchResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSFetch, d.Code, d.Message)
			}
			rows = d.Rows
			if d.Completed {
				status = AfterVersion
				action, _ := json.Marshal(&WSAction{
					Action: wstool.ClientVersion,
					Args:   nil,
				})
				err = ws.WriteMessage(
					websocket.TextMessage,
					action,
				)
				return nil
			}

			status = AfterFetchBlock
			b, _ := json.Marshal(&WSFetchBlockReq{
				ReqID: 4,
				ID:    queryResult.ID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: WSFetchBlock,
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterFetchBlock:
			_, blockResult = parseblock.ParseBlock(message[8:], queryResult.FieldsTypes, rows, queryResult.Precision)
			status = AfterFetch
			b, _ := json.Marshal(&WSFetchReq{
				ReqID: 3,
				ID:    queryResult.ID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: WSFetch,
				Args:   b,
			})
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
					return
				}
				t.Error(err)
				finish <- struct{}{}
				return
			}
			err = testMessageHandler2(mt, message)
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
	connect = &WSConnectReq{
		ReqID:    0,
		User:     "root",
		Password: "taosdata",
		DB:       "test_ws_write_block",
	}

	b, _ = json.Marshal(connect)
	action, _ = json.Marshal(&WSAction{
		Action: WSConnect,
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
	err = ws.Close()
	assert.NoError(t, err)
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
	code, message = doRestful("drop database if exists test_ws_write_block", "")
	assert.Equal(t, 0, code, message)
}

func TestWriteBlockWithFields(t *testing.T) {
	now := time.Now().Local().UnixNano() / 1e6
	code, message := doRestful("create database if not exists test_ws_write_block_with_fields WAL_RETENTION_PERIOD 86400", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("drop table if exists test_ws_write_block_with_fields", "test_ws_write_block_with_fields")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create table if not exists test_ws_write_block_with_fields(ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20)) tags (info json)", "test_ws_write_block_with_fields")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf(`insert into t1 using test_ws_write_block_with_fields tags('{"table":"t1"}') values (%d,true,2,3,4,5,6,7,8,9,10,11,'中文"binary','中文nchar')(%d,false,12,13,14,15,16,17,18,19,110,111,'中文"binary','中文nchar')(%d,null,null,null,null,null,null,null,null,null,null,null,null,null)`, now, now+1, now+3), "test_ws_write_block_with_fields")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(`create table t2 using test_ws_write_block_with_fields tags('{"table":"t2"}')`, "test_ws_write_block_with_fields")
	assert.Equal(t, 0, code, message)

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	const (
		AfterConnect                 = 1
		AfterQuery                   = 2
		AfterFetch                   = 3
		AfterWriteRawBlockWithFields = 4
		AfterFetchBlock              = 5
		AfterVersion                 = 6
	)

	status := 0
	var queryResult *WSQueryResult
	var rows int
	finish := make(chan struct{})
	testMessageHandler := func(_ int, message []byte) error {
		//json
		switch status {
		case AfterConnect:
			var d WSConnectResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSConnect, d.Code, d.Message)
			}
			//query
			status = AfterQuery
			b, _ := json.Marshal(&WSQueryReq{
				ReqID: 2,
				SQL:   "select ts,v1 from t1",
			})
			action, _ := json.Marshal(&WSAction{
				Action: "query",
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterQuery:
			var d WSQueryResult
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSQuery, d.Code, d.Message)
			}
			queryResult = &d
			status = AfterFetch
			//fetch
			b, _ := json.Marshal(&WSFetchReq{
				ReqID: 3,
				ID:    queryResult.ID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: WSFetch,
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterFetch:
			var d WSFetchResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSFetch, d.Code, d.Message)
			}
			rows = d.Rows
			if d.Completed {
				status = AfterVersion
				action, _ := json.Marshal(&WSAction{
					Action: wstool.ClientVersion,
					Args:   nil,
				})
				err = ws.WriteMessage(
					websocket.TextMessage,
					action,
				)
				return nil
			}

			status = AfterFetchBlock
			b, _ := json.Marshal(&WSFetchBlockReq{
				ReqID: 4,
				ID:    queryResult.ID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: WSFetchBlock,
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterFetchBlock:
			//block
			buffer := &bytes.Buffer{}
			// req id
			wstool.WriteUint64(buffer, 300)
			// message id
			wstool.WriteUint64(buffer, 400)
			// action
			wstool.WriteUint64(buffer, RawBlockMessageWithFields)
			// rows
			wstool.WriteUint32(buffer, uint32(rows))
			// table name length
			wstool.WriteUint16(buffer, uint16(2))
			// table name
			buffer.WriteString("t2")
			// raw block
			buffer.Write(message[16:])
			// fields
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
			status = AfterWriteRawBlockWithFields
			err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
			if err != nil {
				return err
			}
		case AfterWriteRawBlockWithFields:
			var d WSWriteRawBlockWithFieldsResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSWriteRawBlockWithFields, d.Code, d.Message)
			}
			status = AfterFetch
			b, _ := json.Marshal(&WSFetchReq{
				ReqID: 3,
				ID:    queryResult.ID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: WSFetch,
				Args:   b,
			})
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

	connect := &WSConnectReq{
		ReqID:    0,
		User:     "root",
		Password: "taosdata",
		DB:       "test_ws_write_block_with_fields",
	}

	b, _ := json.Marshal(connect)
	action, _ := json.Marshal(&WSAction{
		Action: WSConnect,
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
	err = ws.Close()
	assert.NoError(t, err)
	ws, _, err = websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	var blockResult [][]driver.Value
	testMessageHandler2 := func(_ int, message []byte) error {
		switch status {
		case AfterConnect:
			var d WSConnectResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSConnect, d.Code, d.Message)
			}
			//query
			status = AfterQuery
			b, _ := json.Marshal(&WSQueryReq{
				ReqID: 2,
				SQL:   "select * from t2",
			})
			action, _ := json.Marshal(&WSAction{
				Action: "query",
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterQuery:
			var d WSQueryResult
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSQuery, d.Code, d.Message)
			}
			queryResult = &d
			status = AfterFetch
			//fetch
			b, _ := json.Marshal(&WSFetchReq{
				ReqID: 3,
				ID:    queryResult.ID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: WSFetch,
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterFetch:
			var d WSFetchResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSFetch, d.Code, d.Message)
			}
			rows = d.Rows
			if d.Completed {
				status = AfterVersion
				action, _ := json.Marshal(&WSAction{
					Action: wstool.ClientVersion,
					Args:   nil,
				})
				err = ws.WriteMessage(
					websocket.TextMessage,
					action,
				)
				return nil
			}

			status = AfterFetchBlock
			b, _ := json.Marshal(&WSFetchBlockReq{
				ReqID: 4,
				ID:    queryResult.ID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: WSFetchBlock,
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterFetchBlock:
			_, blockResult = parseblock.ParseBlock(message[8:], queryResult.FieldsTypes, rows, queryResult.Precision)
			status = AfterFetch
			b, _ := json.Marshal(&WSFetchReq{
				ReqID: 3,
				ID:    queryResult.ID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: WSFetch,
				Args:   b,
			})
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
					return
				}
				t.Error(err)
				finish <- struct{}{}
				return
			}
			err = testMessageHandler2(mt, message)
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
	connect = &WSConnectReq{
		ReqID:    0,
		User:     "root",
		Password: "taosdata",
		DB:       "test_ws_write_block_with_fields",
	}

	b, _ = json.Marshal(connect)
	action, _ = json.Marshal(&WSAction{
		Action: WSConnect,
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
	err = ws.Close()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(blockResult))
	assert.Equal(t, now, blockResult[0][0].(time.Time).UnixNano()/1e6)
	assert.Equal(t, true, blockResult[0][1])
	for i := 2; i < 14; i++ {
		assert.Equal(t, nil, blockResult[0][i])
	}
	assert.Equal(t, now+1, blockResult[1][0].(time.Time).UnixNano()/1e6)
	assert.Equal(t, false, blockResult[1][1])
	for i := 2; i < 14; i++ {
		assert.Equal(t, nil, blockResult[1][i])
	}
	assert.Equal(t, now+3, blockResult[2][0].(time.Time).UnixNano()/1e6)
	for i := 1; i < 14; i++ {
		assert.Equal(t, nil, blockResult[2][i])
	}
	code, message = doRestful("drop database if exists test_ws_write_block_with_fields", "")
	assert.Equal(t, 0, code, message)
}

func TestQueryAllType(t *testing.T) {
	now := time.Now().Local().UnixNano() / 1e6
	code, message := doRestful("create database if not exists test_ws_all_query WAL_RETENTION_PERIOD 86400", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("drop table if exists test_ws_all_query", "test_ws_all_query")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create table if not exists test_ws_all_query(ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100))", "test_ws_all_query")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf(`insert into test_ws_all_query values (%d,true,2,3,4,5,6,7,8,9,10,11,'中文"binary','中文nchar','\xaabbcc','POINT(100 100)')(%d,false,12,13,14,15,16,17,18,19,110,111,'中文"binary','中文nchar','\xaabbcc','POINT(100 100)')(%d,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)`, now, now+1, now+3), "test_ws_all_query")
	assert.Equal(t, 0, code, message)
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
		AfterConnect    = 1
		AfterQuery      = 2
		AfterFetch      = 3
		AfterFetchBlock = 5
		AfterVersion    = 6
	)

	status := 0
	var lengths []int
	var queryResult *WSQueryResult
	var rows int
	//total := 0
	finish := make(chan struct{})
	//var jsonResult [][]interface{}
	var resultID uint64
	var blockResult [][]driver.Value
	testMessageHandler := func(_ int, message []byte) error {
		//json
		switch status {
		case AfterConnect:
			var d WSConnectResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSConnect, d.Code, d.Message)
			}
			//query
			status = AfterQuery
			b, _ := json.Marshal(&WSQueryReq{
				ReqID: 2,
				SQL:   "select * from test_ws_all_query order by ts asc ",
			})
			action, _ := json.Marshal(&WSAction{
				Action: "query",
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterQuery:
			var d WSQueryResult
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSQuery, d.Code, d.Message)
			}
			queryResult = &d
			status = AfterFetch
			//fetch
			b, _ := json.Marshal(&WSFetchReq{
				ReqID: 3,
				ID:    queryResult.ID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: WSFetch,
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterFetch:
			var d WSFetchResp
			err = json.Unmarshal(message, &d)
			if err != nil {
				return err
			}
			if d.Code != 0 {
				return fmt.Errorf("%s %d,%s", WSFetch, d.Code, d.Message)
			}
			lengths = d.Lengths
			rows = d.Rows
			if d.Completed {
				status = AfterVersion
				action, _ := json.Marshal(&WSAction{
					Action: wstool.ClientVersion,
					Args:   nil,
				})
				err = ws.WriteMessage(
					websocket.TextMessage,
					action,
				)
				return nil
			}

			status = AfterFetchBlock
			b, _ := json.Marshal(&WSFetchBlockReq{
				ReqID: 4,
				ID:    queryResult.ID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: WSFetchBlock,
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			if err != nil {
				return err
			}
		case AfterFetchBlock:
			//block
			resultID, blockResult = parseblock.ParseBlock(message[8:], queryResult.FieldsTypes, rows, queryResult.Precision)

			_ = lengths
			status = AfterFetch
			b, _ := json.Marshal(&WSFetchReq{
				ReqID: 3,
				ID:    queryResult.ID,
			})
			action, _ := json.Marshal(&WSAction{
				Action: WSFetch,
				Args:   b,
			})
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

	connect := &WSConnectReq{
		ReqID:    0,
		User:     "root",
		Password: "taosdata",
		DB:       "test_ws_all_query",
	}

	b, _ := json.Marshal(connect)
	action, _ := json.Marshal(&WSAction{
		Action: WSConnect,
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
	assert.Equal(t, uint64(1), resultID)
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
	assert.Equal(t, []byte{
		0x01,
		0x01,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x59,
		0x40,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x59,
		0x40,
	}, blockResult[1][15])

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
	assert.Equal(t, []byte{0xaa, 0xbb, 0xcc}, blockResult[0][14])
	assert.Equal(t, []byte{
		0x01,
		0x01,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x59,
		0x40,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x59,
		0x40,
	}, blockResult[1][15])
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
	code, message = doRestful("drop database if exists test_ws_all_query", "")
	assert.Equal(t, 0, code, message)
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

func doWebSocket(ws *websocket.Conn, action string, arg interface{}) (resp []byte, err error) {
	var b []byte
	if arg != nil {
		b, _ = json.Marshal(arg)
	}
	a, _ := json.Marshal(WSAction{Action: action, Args: b})
	err = ws.WriteMessage(websocket.TextMessage, a)
	if err != nil {
		return nil, err
	}
	_, message, err := ws.ReadMessage()
	return message, err
}

func TestDropUser(t *testing.T) {
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
	defer doRestful("drop user test_ws_query_drop_user", "")
	code, message := doRestful("create user test_ws_query_drop_user pass 'pass_123'", "")
	assert.Equal(t, 0, code, message)
	// connect
	connReq := &WSConnectReq{ReqID: 1, User: "test_ws_query_drop_user", Password: "pass_123"}
	resp, err := doWebSocket(ws, WSConnect, &connReq)
	assert.NoError(t, err)
	var connResp WSConnectResp
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)
	// drop user
	code, message = doRestful("drop user test_ws_query_drop_user", "")
	assert.Equal(t, 0, code, message)
	time.Sleep(time.Second * 3)
	resp, err = doWebSocket(ws, wstool.ClientVersion, nil)
	assert.Error(t, err, resp)
}

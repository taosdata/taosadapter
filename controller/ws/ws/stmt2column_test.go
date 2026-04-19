package ws

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/driver/common"
	stmtCommon "github.com/taosdata/taosadapter/v3/driver/common/stmt"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/tools/parseblock"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

func TestWsStmt2BindExecQueryRequiresSingleRow(t *testing.T) {
	requireStmt2BindExecTestEnv(t)

	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful("drop database if exists test_ws_stmt2_bind_exec_q", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_stmt2_bind_exec_q precision 'ms'", "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_stmt2_bind_exec_q"))
	defer doRestful("drop database if exists test_ws_stmt2_bind_exec_q", "")
	code, message = doRestful("create table if not exists t (ts timestamp, v int)", "test_ws_stmt2_bind_exec_q")
	assert.Equal(t, 0, code, message)

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, ws.Close())
	}()

	resp, err := doWebSocket(ws, Connect, &connRequest{ReqID: 1, User: "root", Password: "taosdata", DB: "test_ws_stmt2_bind_exec_q"})
	require.NoError(t, err)
	var connResp connResponse
	require.NoError(t, json.Unmarshal(resp, &connResp))
	require.Equal(t, 0, connResp.Code, connResp.Message)

	resp, err = doWebSocket(ws, STMT2Init, &stmt2InitRequest{ReqID: 2})
	require.NoError(t, err)
	var initResp stmt2InitResponse
	require.NoError(t, json.Unmarshal(resp, &initResp))
	require.Equal(t, 0, initResp.Code, initResp.Message)

	resp, err = doWebSocket(ws, STMT2Prepare, &stmt2PrepareRequest{
		ReqID:     3,
		StmtID:    initResp.StmtID,
		SQL:       "select * from t where ts = ?",
		GetFields: true,
	})
	require.NoError(t, err)
	var prepareResp stmt2PrepareResponse
	require.NoError(t, json.Unmarshal(resp, &prepareResp))
	require.Equal(t, 0, prepareResp.Code, prepareResp.Message)
	require.False(t, prepareResp.IsInsert)
	require.Equal(t, 1, prepareResp.FieldsCount)
	require.Nil(t, prepareResp.Fields)

	payload, err := stmtCommon.MarshalStmt2ColumnBinary([][]driver.Value{
		{time.UnixMilli(0)},
	}, nil)
	require.NoError(t, err)
	binary.LittleEndian.PutUint32(payload[stmtCommon.Stmt2ColumnRowCountPosition:], 2)

	req := buildStmt2BindExecMessage(4, prepareResp.StmtID, payload)
	require.NoError(t, ws.WriteMessage(websocket.BinaryMessage, req))
	_, resp, err = ws.ReadMessage()
	require.NoError(t, err)

	var execResp stmt2ExecResponse
	require.NoError(t, json.Unmarshal(resp, &execResp))
	assert.Equal(t, STMT2BindExec, execResp.Action)
	assert.Equal(t, uint64(4), execResp.ReqID)
	assert.Equal(t, 0xffff, execResp.Code)
	assert.Equal(t, "query only supports one row", execResp.Message)
}

func TestWsStmt2BindExec(t *testing.T) {
	requireStmt2BindExecTestEnv(t)

	if !wrapper.TaosStmt2BindColumnSupported() {
		t.Skip("taos_stmt2_bind_param_column not available in current taosnative")
	}

	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful("drop database if exists test_ws_stmt2_bind_exec", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_stmt2_bind_exec precision 'ms'", "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_stmt2_bind_exec"))
	defer doRestful("drop database if exists test_ws_stmt2_bind_exec", "")
	code, message = doRestful("create table if not exists stb (ts timestamp, v int) tags (id int)", "test_ws_stmt2_bind_exec")
	assert.Equal(t, 0, code, message)

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, ws.Close())
	}()

	resp, err := doWebSocket(ws, Connect, &connRequest{ReqID: 1, User: "root", Password: "taosdata", DB: "test_ws_stmt2_bind_exec"})
	require.NoError(t, err)
	var connResp connResponse
	require.NoError(t, json.Unmarshal(resp, &connResp))
	require.Equal(t, 0, connResp.Code, connResp.Message)

	insertStmtID := initStmt2ForBindExec(t, ws)
	insertPrepareResp := prepareStmt2ForBindExec(t, ws, insertStmtID, 3, "insert into ? using test_ws_stmt2_bind_exec.stb tags (?) values (?,?)")
	now := time.Now().Round(time.Millisecond)
	insertPayload, err := stmtCommon.MarshalStmt2ColumnBinary(buildInsertColumns(insertPrepareResp.Fields, "ct1", int32(7), now, int32(11)), insertPrepareResp.Fields)
	require.NoError(t, err)
	req := buildStmt2BindExecMessage(4, insertStmtID, insertPayload)
	require.NoError(t, ws.WriteMessage(websocket.BinaryMessage, req))
	_, resp, err = ws.ReadMessage()
	require.NoError(t, err)
	var insertExecResp stmt2ExecResponse
	require.NoError(t, json.Unmarshal(resp, &insertExecResp))
	assert.Equal(t, STMT2BindExec, insertExecResp.Action)
	assert.Equal(t, 0, insertExecResp.Code, insertExecResp.Message)
	assert.Equal(t, 1, insertExecResp.Affected)

	queryBindBinaryBefore := monitor.TaosStmt2BindBinaryCounter.Value()
	queryBindColumnBefore := monitor.TaosStmt2BindColumnBinaryCounter.Value()

	selectStmtID := initStmt2ForBindExec(t, ws)
	prepareStmt2ForBindExec(t, ws, selectStmtID, 6, "select ts, v from test_ws_stmt2_bind_exec.stb where ts = ?")
	selectPayload, err := stmtCommon.MarshalStmt2ColumnBinary([][]driver.Value{
		{now},
	}, nil)
	require.NoError(t, err)
	req = buildStmt2BindExecMessage(7, selectStmtID, selectPayload)
	require.NoError(t, ws.WriteMessage(websocket.BinaryMessage, req))
	_, resp, err = ws.ReadMessage()
	require.NoError(t, err)
	var selectExecResp stmt2ExecResponse
	require.NoError(t, json.Unmarshal(resp, &selectExecResp))
	assert.Equal(t, STMT2BindExec, selectExecResp.Action)
	assert.Equal(t, 0, selectExecResp.Code, selectExecResp.Message)
	assert.Equal(t, queryBindBinaryBefore, monitor.TaosStmt2BindBinaryCounter.Value())
	assert.Equal(t, queryBindColumnBefore+1, monitor.TaosStmt2BindColumnBinaryCounter.Value())

	resp, err = doWebSocket(ws, STMT2Result, &stmt2UseResultRequest{ReqID: 8, StmtID: selectStmtID})
	require.NoError(t, err)
	var useResultResp stmt2UseResultResponse
	require.NoError(t, json.Unmarshal(resp, &useResultResp))
	require.Equal(t, 0, useResultResp.Code, useResultResp.Message)

	resp, err = doWebSocket(ws, WSFetch, &fetchRequest{ReqID: 9, ID: useResultResp.ID})
	require.NoError(t, err)
	var fetchResp fetchResponse
	require.NoError(t, json.Unmarshal(resp, &fetchResp))
	require.Equal(t, 0, fetchResp.Code, fetchResp.Message)
	require.Equal(t, 1, fetchResp.Rows)

	fetchBlockResp, err := doWebSocket(ws, WSFetchBlock, &fetchBlockRequest{ReqID: 10, ID: useResultResp.ID})
	require.NoError(t, err)
	_, blockResult, err := parseblock.ParseBlock(fetchBlockResp[8:], useResultResp.FieldsTypes, fetchResp.Rows, useResultResp.Precision)
	require.NoError(t, err)
	require.Len(t, blockResult, 1)
	assert.Equal(t, int32(11), blockResult[0][1])
}

func initStmt2ForBindExec(t *testing.T, ws *websocket.Conn) uint64 {
	resp, err := doWebSocket(ws, STMT2Init, &stmt2InitRequest{ReqID: 2})
	require.NoError(t, err)
	var initResp stmt2InitResponse
	require.NoError(t, json.Unmarshal(resp, &initResp))
	require.Equal(t, 0, initResp.Code, initResp.Message)
	return initResp.StmtID
}

func prepareStmt2ForBindExec(t *testing.T, ws *websocket.Conn, stmtID uint64, reqID uint64, sql string) stmt2PrepareResponse {
	resp, err := doWebSocket(ws, STMT2Prepare, &stmt2PrepareRequest{
		ReqID:     reqID,
		StmtID:    stmtID,
		SQL:       sql,
		GetFields: true,
	})
	require.NoError(t, err)
	var prepareResp stmt2PrepareResponse
	require.NoError(t, json.Unmarshal(resp, &prepareResp))
	require.Equal(t, 0, prepareResp.Code, prepareResp.Message)
	return prepareResp
}

func buildStmt2BindExecMessage(reqID uint64, stmtID uint64, payload []byte) []byte {
	req := make([]byte, len(payload)+26)
	binary.LittleEndian.PutUint64(req, reqID)
	binary.LittleEndian.PutUint64(req[8:], stmtID)
	binary.LittleEndian.PutUint64(req[16:], Stmt2BindExecMessage)
	binary.LittleEndian.PutUint16(req[24:], Stmt2BindExecProtocolVersion1)
	copy(req[26:], payload)
	return req
}

func buildInsertColumns(fields []*stmtCommon.Stmt2AllField, tbName string, tagValue int32, ts time.Time, value int32) [][]driver.Value {
	columns := make([][]driver.Value, len(fields))
	for i := range fields {
		switch fields[i].BindType {
		case stmtCommon.TAOS_FIELD_TBNAME:
			columns[i] = []driver.Value{tbName}
		case stmtCommon.TAOS_FIELD_TAG:
			columns[i] = []driver.Value{tagValue}
		case stmtCommon.TAOS_FIELD_COL:
			if fields[i].FieldType == common.TSDB_DATA_TYPE_TIMESTAMP {
				columns[i] = []driver.Value{ts}
			} else {
				columns[i] = []driver.Value{value}
			}
		}
	}
	return columns
}

func requireStmt2BindExecTestEnv(t *testing.T) {
	t.Helper()

	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Skipf("taos test env unavailable: %v", err)
		return
	}
	wrapper.TaosClose(conn)
}

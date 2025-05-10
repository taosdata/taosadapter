package test

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	stmtCommon "github.com/taosdata/taosadapter/v3/driver/common/stmt"
)

const (
	//Deprecated
	//WSWriteRaw                = "write_raw"
	//WSWriteRawBlock           = "write_raw_block"
	//WSWriteRawBlockWithFields = "write_raw_block_with_fields"

	Connect = "conn"
	// websocket
	WSQuery         = "query"
	WSFetch         = "fetch"
	WSFetchBlock    = "fetch_block"
	WSFreeResult    = "free_result"
	WSGetCurrentDB  = "get_current_db"
	WSGetServerInfo = "get_server_info"
	WSNumFields     = "num_fields"

	// schemaless
	SchemalessWrite = "insert"

	// stmt
	STMTInit         = "init"
	STMTPrepare      = "prepare"
	STMTSetTableName = "set_table_name"
	STMTSetTags      = "set_tags"
	STMTBind         = "bind"
	STMTAddBatch     = "add_batch"
	STMTExec         = "exec"
	STMTClose        = "close"
	STMTGetTagFields = "get_tag_fields"
	STMTGetColFields = "get_col_fields"
	STMTUseResult    = "use_result"
	STMTNumParams    = "stmt_num_params"
	STMTGetParam     = "stmt_get_param"

	// stmt2
	STMT2Init    = "stmt2_init"
	STMT2Prepare = "stmt2_prepare"
	STMT2Exec    = "stmt2_exec"
	STMT2Result  = "stmt2_result"
	STMT2Close   = "stmt2_close"

	// options
	OptionsConnection = "options_connection"

	// check_server_status
	CheckServerStatus = "check_server_status"
)

type connRequest struct {
	ReqID    uint64 `json:"req_id"`
	User     string `json:"user"`
	Password string `json:"password"`
	DB       string `json:"db"`
	Mode     *int   `json:"mode"`
	TZ       string `json:"tz"`
	App      string `json:"app"`
	IP       string `json:"ip"`
}

type Request struct {
	Action string          `json:"action"`
	Args   json.RawMessage `json:"args"`
}
type commonResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

const (
	SetTagsMessage            = 1
	BindMessage               = 2
	TMQRawMessage             = 3
	RawBlockMessage           = 4
	RawBlockMessageWithFields = 5
	BinaryQueryMessage        = 6
	FetchRawBlockMessage      = 7
	Stmt2BindMessage          = 9
	ValidateSQL               = 10
)

const (
	BinaryProtocolVersion1    uint16 = 1
	Stmt2BindProtocolVersion1 uint16 = 1
)

func doWebSocket(ws *websocket.Conn, action string, arg interface{}) (resp []byte, err error) {
	var b []byte
	if arg != nil {
		b, err = json.Marshal(arg)
		if err != nil {
			return nil, err
		}
	}
	a, err := json.Marshal(Request{Action: action, Args: b})
	if err != nil {
		return nil, err
	}
	err = ws.WriteMessage(websocket.TextMessage, a)
	if err != nil {
		return nil, err
	}
	_, message, err := ws.ReadMessage()
	return message, err
}

type stmt2InitRequest struct {
	ReqID               uint64 `json:"req_id"`
	SingleStbInsert     bool   `json:"single_stb_insert"`
	SingleTableBindOnce bool   `json:"single_table_bind_once"`
}
type stmt2InitResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}
type stmt2PrepareRequest struct {
	ReqID     uint64 `json:"req_id"`
	StmtID    uint64 `json:"stmt_id"`
	SQL       string `json:"sql"`
	GetFields bool   `json:"get_fields"`
}

type stmt2PrepareResponse struct {
	Code        int                         `json:"code"`
	Message     string                      `json:"message"`
	Action      string                      `json:"action"`
	ReqID       uint64                      `json:"req_id"`
	Timing      int64                       `json:"timing"`
	StmtID      uint64                      `json:"stmt_id"`
	IsInsert    bool                        `json:"is_insert"`
	Fields      []*stmtCommon.Stmt2AllField `json:"fields"`
	FieldsCount int                         `json:"fields_count"`
}

type stmt2BindResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}
type stmt2ExecRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}
type stmt2ExecResponse struct {
	Code     int    `json:"code"`
	Message  string `json:"message"`
	Action   string `json:"action"`
	ReqID    uint64 `json:"req_id"`
	Timing   int64  `json:"timing"`
	StmtID   uint64 `json:"stmt_id"`
	Affected int    `json:"affected"`
}
type stmt2CloseRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}
type stmt2CloseResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func TestWs(t *testing.T) {
	tableCount := 10
	rows := 100
	loopTimes := 100000
	ws, _, err := websocket.DefaultDialer.Dial("ws://172.16.1.43:6041/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	// connect
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata", DB: "test"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp commonResp
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// init
	initReq := stmt2InitRequest{
		ReqID:               0x123,
		SingleStbInsert:     true,
		SingleTableBindOnce: true,
	}
	resp, err = doWebSocket(ws, STMT2Init, &initReq)
	assert.NoError(t, err)
	var initResp stmt2InitResponse
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0x123), initResp.ReqID)
	assert.Equal(t, 0, initResp.Code, initResp.Message)
	sql := "insert into ? using meters tags(?,?) values(?,?,?,?)"
	// prepare
	prepareReq := stmt2PrepareRequest{
		ReqID:     3,
		StmtID:    initResp.StmtID,
		SQL:       sql,
		GetFields: true,
	}
	resp, err = doWebSocket(ws, STMT2Prepare, &prepareReq)
	assert.NoError(t, err)
	var prepareResp stmt2PrepareResponse
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)
	assert.True(t, prepareResp.IsInsert)
	// bind
	binds := make([]*stmtCommon.TaosStmt2BindData, tableCount)
	ts := time.Now().UnixMilli()
	for i := 0; i < tableCount; i++ {
		tbName := fmt.Sprintf("t_%04d", i)
		tag := []driver.Value{int32(i), "location"}
		cols := make([][]driver.Value, 4)
		for j := 0; j < 4; j++ {
			cols[j] = make([]driver.Value, rows)
		}
		for j := 0; j < rows; j++ {
			// ts                             | TIMESTAMP              |           8 |                    | delta-i        | lz4            | medium         |
			// current                        | FLOAT                  |           4 |                    | delta-d        | lz4            | medium         |
			// voltage                        | INT                    |           4 |                    | simple8b       | lz4            | medium         |
			// phase                          | FLOAT                  |           4 |                    | delta-d        | lz4            | medium         |
			cols[0][j] = ts + int64(j)
			cols[1][j] = float32(j)
			cols[2][j] = int32(j)
			cols[3][j] = float32(j)
		}
		bind := &stmtCommon.TaosStmt2BindData{
			TableName: tbName,
			Tags:      tag,
			Cols:      cols,
		}
		binds[i] = bind
	}
	bs, err := stmtCommon.MarshalStmt2Binary(binds, true, prepareResp.Fields)
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
	start := time.Now()
	for i := 0; i < loopTimes; i++ {
		err = ws.WriteMessage(websocket.BinaryMessage, bindReq)
		assert.NoError(t, err)
		_, resp, err = ws.ReadMessage()
		assert.NoError(t, err)
		var bindResp stmt2BindResponse
		err = json.Unmarshal(resp, &bindResp)
		assert.NoError(t, err)
		assert.Equal(t, 0, bindResp.Code, bindResp.Message)

		//exec
		execReq := stmt2ExecRequest{ReqID: 10, StmtID: prepareResp.StmtID}
		resp, err = doWebSocket(ws, STMT2Exec, &execReq)
		assert.NoError(t, err)
		var execResp stmt2ExecResponse
		err = json.Unmarshal(resp, &execResp)
		assert.NoError(t, err)
		assert.Equal(t, uint64(10), execResp.ReqID)
		assert.Equal(t, 0, execResp.Code, execResp.Message)
		//assert.Equal(t, 1000, execResp.Affected)
	}
	t.Log(time.Now().Sub(start))
	// close
	closeReq := stmt2CloseRequest{ReqID: 11, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMT2Close, &closeReq)
	assert.NoError(t, err)
	var closeResp stmt2CloseResponse
	err = json.Unmarshal(resp, &closeResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(11), closeResp.ReqID)
	assert.Equal(t, 0, closeResp.Code, closeResp.Message)
}

func TestWsConcurrent(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(8)
	for i := 0; i < 8; i++ {
		go func() {
			defer wg.Done()
			TestWs(t)
		}()
	}
	wg.Wait()
}

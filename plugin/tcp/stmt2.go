package tcp

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/driver/common/stmt"
	errors2 "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/log"
)

type stmt2InitRequest struct {
	ReqID               uint64 `json:"req_id"`
	SingleStbInsert     bool   `json:"single_stb_insert"`
	SingleTableBindOnce bool   `json:"single_table_bind_once"`
}

func unmarshalStmt2InitRequest(reqID uint64, bytes []byte) (req *stmt2InitRequest, err error) {
	// version 1
	// SingleStbInsert bool
	// SingleTableBindOnce bool
	version := bytes[0]
	if version != 1 {
		return nil, fmt.Errorf("unexpected version:%d", version)
	}
	singleStbInsert := bytes[1] == 1
	singleTableBindOnce := bytes[2] == 1
	req = &stmt2InitRequest{
		ReqID:               reqID,
		SingleStbInsert:     singleStbInsert,
		SingleTableBindOnce: singleTableBindOnce,
	}
	return req, nil
}

func (c *Connection) stmt2Init(ctx context.Context, reqID uint64, innerReqID uint64, payload []byte, logger *logrus.Entry, isDebug bool) {
	req, err := unmarshalStmt2InitRequest(reqID, payload)
	if err != nil {
		logger.Errorf("unmarshal stmt2 init request error, err:%s", err.Error())
		c.sendErrorResponse(ctx, reqID, 0xffff, err.Error(), CmdStmt2Init)
		return
	}
	handle, caller := async.GlobalStmt2CallBackCallerPool.Get()
	stmtInit := syncinterface.TaosStmt2Init(c.conn, int64(innerReqID), req.SingleStbInsert, req.SingleTableBindOnce, handle, logger, isDebug)
	if stmtInit == nil {
		async.GlobalStmt2CallBackCallerPool.Put(handle)
		errStr := wrapper.TaosStmt2Error(stmtInit)
		logger.Errorf("stmt2 init error, err:%s", errStr)
		c.sendErrorResponse(ctx, reqID, 0xffff, errStr, CmdStmt2Init)
		return
	}
	stmtItem := &StmtItem{stmt: stmtInit, handler: handle, caller: caller, isStmt2: true}
	c.stmts.Add(stmtItem)
	logger.Tracef("stmt2 init sucess, stmt_id:%d, stmt pointer:%p", stmtItem.index, stmtInit)
	c.sendStmtCommonResponse(ctx, reqID, stmtItem.index, 0, "", CmdStmt2Init)
}

func (c *Connection) stmt2ValidateAndLock(ctx context.Context, cmd byte, reqID uint64, stmtID uint64, logger *logrus.Entry, isDebug bool) (stmtItem *StmtItem, locked bool) {
	stmtItem = c.stmts.GetStmt2(stmtID)
	if stmtItem == nil {
		logger.Errorf("stmt2 is nil, stmt_id:%d", stmtID)
		c.sendStmtCommonResponse(ctx, reqID, stmtID, 0xffff, "stmt2 is nil", cmd)
		return nil, false
	}
	s := log.GetLogNow(isDebug)
	logger.Trace("get stmt2 lock")
	stmtItem.Lock()
	logger.Debugf("get stmt2 lock cost:%s", log.GetLogDuration(isDebug, s))
	if stmtItem.stmt == nil {
		stmtItem.Unlock()
		logger.Errorf("stmt2 has been freed, stmt_id:%d", stmtID)
		c.sendStmtCommonResponse(ctx, reqID, stmtID, 0xffff, "stmt has been freed", cmd)
		return nil, false
	}
	return stmtItem, true
}

type stmt2PrepareRequest struct {
	ReqID     uint64 `json:"req_id"`
	StmtID    uint64 `json:"stmt_id"`
	SQL       string `json:"sql"`
	GetFields bool   `json:"get_fields"`
}

func unmarshalStmt2PrepareRequest(reqID uint64, bytes []byte) (req *stmt2PrepareRequest, err error) {
	// version 1
	// StmtID uint64
	// SQLLen uint32
	// SQL string
	// GetFields bool
	version := bytes[0]
	if version != 1 {
		return nil, fmt.Errorf("unexpected version:%d", version)
	}
	stmtID := binary.LittleEndian.Uint64(bytes[1:9])
	sqlLen := binary.LittleEndian.Uint32(bytes[9:13])
	if sqlLen == 0 {
		return nil, fmt.Errorf("sql length is 0")
	}
	if sqlLen+13 > uint32(len(bytes)) {
		return nil, fmt.Errorf("sql length is too long, expect:%d, but get:%d", sqlLen, len(bytes))
	}
	sql := string(bytes[13 : 13+sqlLen])
	getFields := bytes[13+sqlLen] == 1
	req = &stmt2PrepareRequest{
		ReqID:     reqID,
		StmtID:    stmtID,
		SQL:       sql,
		GetFields: getFields,
	}
	return req, nil
}

type stmt2PrepareResponse struct {
	Code        int                   `json:"code"`
	Message     string                `json:"message"`
	Action      string                `json:"action"`
	ReqID       uint64                `json:"req_id"`
	Timing      int64                 `json:"timing"`
	StmtID      uint64                `json:"stmt_id"`
	IsInsert    bool                  `json:"is_insert"`
	FieldsCount int32                 `json:"fields_count"`
	Fields      []*stmt.Stmt2AllField `json:"fields"`
}
type Stmt2AllField struct {
	Name      string `json:"name"`
	FieldType int8   `json:"field_type"`
	Precision uint8  `json:"precision"`
	Scale     uint8  `json:"scale"`
	Bytes     int32  `json:"bytes"`
	BindType  int8   `json:"bind_type"`
}

func marshalStmt2PrepareResponse(ctx context.Context, reqID uint64, stmtID uint64, isInsert bool, fieldsCount int32, fields []*stmt.Stmt2AllField) ([]byte, error) {
	totalLen := ResponseHeaderLen + 1 + 8 + 1 + 4 + 4
	for i := 0; i < len(fields); i++ {
		totalLen += 1 + len(fields[i].Name) + 8
	}
	buf := make([]byte, totalLen)
	marshalCommonResponse(buf, ctx, reqID, 0, "", CmdStmt2Prepare)
	payload := buf[ResponseHeaderLen:]
	payload[0] = 1
	binary.LittleEndian.PutUint64(payload[1:], stmtID)
	if isInsert {
		payload[9] = 1
	}
	binary.LittleEndian.PutUint32(payload[10:], uint32(fieldsCount))
	offset := 14
	for i := 0; i < len(fields); i++ {
		field := fields[i]
		payload[offset] = byte(len(field.Name))
		offset++
		copy(payload[offset:], field.Name)
		offset += len(field.Name)
		payload[offset] = byte(field.FieldType)
		offset++
		payload[offset] = field.Precision
		offset++
		payload[offset] = field.Scale
		offset++
		binary.LittleEndian.PutUint32(payload[offset:], uint32(field.Bytes))
		offset += 4
		payload[offset] = byte(field.BindType)
		offset++
	}
	return buf, nil
}

func (c *Connection) stmt2Prepare(ctx context.Context, reqID uint64, payload []byte, logger *logrus.Entry, isDebug bool) {
	req, err := unmarshalStmt2PrepareRequest(reqID, payload)
	if err != nil {
		logger.Errorf("unmarshal stmt2 prepare request error, err:%s", err.Error())
		c.sendErrorResponse(ctx, reqID, 0xffff, err.Error(), CmdStmt2Prepare)
		return
	}
	logger.Debugf("stmt2 prepare, stmt_id:%d, sql:%s", req.StmtID, req.SQL)
	stmtItem, locked := c.stmt2ValidateAndLock(ctx, CmdStmt2Prepare, req.ReqID, req.StmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	stmt2 := stmtItem.stmt
	code := syncinterface.TaosStmt2Prepare(stmt2, req.SQL, logger, isDebug)
	if code != 0 {
		errStr := wrapper.TaosStmt2Error(stmt2)
		logger.Errorf("stmt2 prepare error, err:%s", errStr)
		c.sendStmtCommonResponse(ctx, req.ReqID, req.StmtID, uint32(code), errStr, CmdStmt2Prepare)
		return
	}
	logger.Tracef("stmt2 prepare success, stmt_id:%d", req.StmtID)
	isInsert, code := syncinterface.TaosStmt2IsInsert(stmt2, logger, isDebug)
	if code != 0 {
		errStr := wrapper.TaosStmt2Error(stmt2)
		logger.Errorf("check stmt2 is insert error, err:%s", errStr)
		c.sendStmtCommonResponse(ctx, req.ReqID, req.StmtID, uint32(code), errStr, CmdStmt2Prepare)
		return
	}
	logger.Tracef("stmt2 is insert:%t", isInsert)
	stmtItem.isInsert = isInsert
	prepareResp := &stmt2PrepareResponse{StmtID: req.StmtID, IsInsert: isInsert}
	if req.GetFields {
		code, count, fields := syncinterface.TaosStmt2GetFields(stmt2, logger, isDebug)
		if code != 0 {
			errStr := wrapper.TaosStmt2Error(stmt2)
			logger.Errorf("stmt2 get fields error, code:%d, err:%s", code, errStr)
			c.sendStmtCommonResponse(ctx, req.ReqID, req.StmtID, uint32(code), errStr, CmdStmt2Prepare)
			return
		}
		defer wrapper.TaosStmt2FreeFields(stmt2, fields)
		stbFields := wrapper.Stmt2ParseAllFields(count, fields)
		prepareResp.Fields = stbFields
		prepareResp.FieldsCount = int32(count)

	}
	prepareResp.ReqID = req.ReqID
	prepareResp.Timing = wstool.GetDuration(ctx)
	buf, err := marshalStmt2PrepareResponse(ctx, prepareResp.ReqID, prepareResp.StmtID, prepareResp.IsInsert, prepareResp.FieldsCount, prepareResp.Fields)
	if err != nil {
		logger.Errorf("marshal stmt2 prepare response error, err:%s", err.Error())
		c.sendStmtCommonResponse(ctx, req.ReqID, req.StmtID, uint32(0xffff), err.Error(), CmdStmt2Prepare)
		return
	}
	c.writePacket(buf)
}

type stmt2ExecRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

func unmarshalStmt2ExecRequest(reqID uint64, bytes []byte) (req *stmt2ExecRequest, err error) {
	// version 1
	// StmtID uint64
	version := bytes[0]
	if version != 1 {
		return nil, fmt.Errorf("unexpected version:%d", version)
	}
	stmtID := binary.LittleEndian.Uint64(bytes[1:9])
	req = &stmt2ExecRequest{
		ReqID:  reqID,
		StmtID: stmtID,
	}
	return req, nil
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

func marshalStmt2ExecResponse(ctx context.Context, reqID uint64, stmtID uint64, affected int32) ([]byte, error) {
	totalLen := ResponseHeaderLen + 1 + 8 + 4
	buf := make([]byte, totalLen)
	marshalCommonResponse(buf, ctx, reqID, 0, "", CmdStmt2Execute)
	payload := buf[ResponseHeaderLen:]
	payload[0] = 1
	binary.LittleEndian.PutUint64(payload[1:], stmtID)
	binary.LittleEndian.PutUint32(payload[9:], uint32(affected))
	return buf, nil
}

func (c *Connection) stmt2Exec(ctx context.Context, reqID uint64, payload []byte, logger *logrus.Entry, isDebug bool) {
	req, err := unmarshalStmt2ExecRequest(reqID, payload)
	if err != nil {
		logger.Errorf("unmarshal stmt2 exec request error, err:%s", err.Error())
		c.sendErrorResponse(ctx, reqID, 0xffff, err.Error(), CmdStmt2Execute)
		return
	}
	logger.Tracef("stmt2 execute, stmt_id:%d", req.StmtID)
	stmtItem, locked := c.stmt2ValidateAndLock(ctx, CmdStmt2Execute, req.ReqID, req.StmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	code := syncinterface.TaosStmt2Exec(stmtItem.stmt, logger, isDebug)
	if code != 0 {
		errStr := wrapper.TaosStmt2Error(stmtItem.stmt)
		logger.Errorf("stmt2 execute error,code:%d, err:%s", code, errStr)
		c.sendStmtCommonResponse(ctx, req.ReqID, req.StmtID, uint32(code), errStr, CmdStmt2Execute)
		return
	}
	s := log.GetLogNow(isDebug)
	logger.Tracef("stmt2 execute wait callback, stmt_id:%d", req.StmtID)
	result := <-stmtItem.caller.ExecResult
	logger.Debugf("stmt2 execute wait callback finish, affected:%d, res:%p, n:%d, cost:%s", result.Affected, result.Res, result.N, log.GetLogDuration(isDebug, s))
	if result.N < 0 {
		errStr := wrapper.TaosStmt2Error(stmtItem.stmt)
		logger.Errorf("stmt2 execute callback error, code:%d, err:%s", result.N, errStr)
		c.sendStmtCommonResponse(ctx, req.ReqID, req.StmtID, uint32(code), errStr, CmdStmt2Execute)
		return
	}
	stmtItem.result = result.Res
	buf, _ := marshalStmt2ExecResponse(ctx, req.ReqID, req.StmtID, int32(result.Affected))
	c.writePacket(buf)
}

type stmt2UseResultRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type stmt2CloseRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

func unmarshalStmt2CloseRequest(reqID uint64, bytes []byte) (req *stmt2CloseRequest, err error) {
	// version 1
	// StmtID uint64
	version := bytes[0]
	if version != 1 {
		return nil, fmt.Errorf("unexpected version:%d", version)
	}
	stmtID := binary.LittleEndian.Uint64(bytes[1:9])
	req = &stmt2CloseRequest{
		ReqID:  reqID,
		StmtID: stmtID,
	}
	return req, nil
}

type stmt2CloseResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func marshalStmt2CloseResponse(ctx context.Context, reqID uint64, stmtID uint64) ([]byte, error) {
	totalLen := ResponseHeaderLen + 1 + 8
	buf := make([]byte, totalLen)
	marshalCommonResponse(buf, ctx, reqID, 0, "", CmdStmt2Close)
	payload := buf[ResponseHeaderLen:]
	payload[0] = 1
	binary.LittleEndian.PutUint64(payload[1:], stmtID)
	return buf, nil
}

func (c *Connection) stmt2Close(ctx context.Context, reqID uint64, payload []byte, logger *logrus.Entry, isDebug bool) {
	req, err := unmarshalStmt2CloseRequest(reqID, payload)
	if err != nil {
		logger.Errorf("unmarshal stmt2 close request error, err:%s", err.Error())
		c.sendErrorResponse(ctx, reqID, 0xffff, err.Error(), CmdStmt2Close)
		return
	}
	logger.Tracef("stmt2 close, stmt_id:%d", req.StmtID)
	err = c.stmts.FreeStmtByID(req.StmtID, true, logger)
	if err != nil {
		logger.Errorf("stmt2 close error, err:%s", err.Error())
		c.sendStmtCommonResponse(ctx, reqID, req.StmtID, 0xffff, err.Error(), CmdStmt2Close)
		return
	}
	logger.Tracef("stmt2 close success, stmt_id:%d", req.StmtID)
	buf, _ := marshalStmt2CloseResponse(ctx, reqID, req.StmtID)
	c.writePacket(buf)
}

type stmt2BindResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func unmarshalStmt2BindReq(bytes []byte) (stmtID uint64, colIndex int32, message []byte, err error) {
	// version 1
	// StmtID uint64
	// colIndex int32
	// MessageLen uint64
	// Message []byte
	version := bytes[0]
	if version != 1 {
		return 0, 0, nil, fmt.Errorf("unexpected version:%d", version)
	}
	stmtID = binary.LittleEndian.Uint64(bytes[1:9])
	colIndex = int32(binary.LittleEndian.Uint32(bytes[9:13]))
	MessageLen := binary.LittleEndian.Uint64(bytes[13:21])
	message = bytes[21 : 21+MessageLen]
	return stmtID, colIndex, message, nil
}

func marshalStmt2BindResponse(ctx context.Context, reqID uint64, stmtID uint64) ([]byte, error) {
	totalLen := ResponseHeaderLen + 1 + 8
	buf := make([]byte, totalLen)
	marshalCommonResponse(buf, ctx, reqID, 0, "", CmdStmt2Bind)
	payload := buf[ResponseHeaderLen:]
	payload[0] = 1
	binary.LittleEndian.PutUint64(payload[1:], stmtID)
	return buf, nil
}

func (c *Connection) stmt2BinaryBind(ctx context.Context, reqID uint64, payload []byte, logger *logrus.Entry, isDebug bool) {
	stmtID, colIndex, message, err := unmarshalStmt2BindReq(payload)
	if err != nil {
		logger.Errorf("unmarshal stmt2 bind request error, err:%s", err.Error())
		c.sendErrorResponse(ctx, reqID, 0xffff, err.Error(), CmdStmt2Bind)
		return
	}
	stmtItem, locked := c.stmt2ValidateAndLock(ctx, CmdStmt2Bind, reqID, stmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	err = syncinterface.TaosStmt2BindBinary(stmtItem.stmt, message, colIndex, logger, isDebug)
	if err != nil {
		logger.Errorf("stmt2 bind error, err:%s", err.Error())
		var tError *errors2.TaosError
		if errors.As(err, &tError) {
			c.sendStmtCommonResponse(ctx, reqID, stmtID, uint32(tError.Code), tError.ErrStr, CmdStmt2Bind)
			return
		}
		c.sendStmtCommonResponse(ctx, reqID, stmtID, uint32(0xffff), err.Error(), CmdStmt2Bind)
		return
	}
	logger.Trace("stmt2 bind success")
	buf, _ := marshalStmt2BindResponse(ctx, reqID, stmtID)
	c.writePacket(buf)
}

package ws

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"

	"github.com/sirupsen/logrus"
	stmtCommon "github.com/taosdata/driver-go/v3/common/stmt"
	errors2 "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/jsontype"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

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

func (h *messageHandler) stmt2Init(ctx context.Context, session *melody.Session, action string, req stmt2InitRequest, logger *logrus.Entry, isDebug bool) {
	handle, caller := async.GlobalStmt2CallBackCallerPool.Get()
	stmtInit := syncinterface.TaosStmt2Init(h.conn, int64(req.ReqID), req.SingleStbInsert, req.SingleTableBindOnce, handle, logger, isDebug)
	if stmtInit == nil {
		async.GlobalStmt2CallBackCallerPool.Put(handle)
		errStr := wrapper.TaosStmtErrStr(stmtInit)
		logger.Errorf("stmt2 init error, err:%s", errStr)
		commonErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, errStr)
		return
	}
	stmtItem := &StmtItem{stmt: stmtInit, handler: handle, caller: caller, isStmt2: true}
	h.stmts.Add(stmtItem)
	logger.Tracef("stmt2 init sucess, stmt_id:%d, stmt pointer:%p", stmtItem.index, stmtInit)
	resp := &stmt2InitResponse{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
		StmtID: stmtItem.index,
	}
	wstool.WSWriteJson(session, logger, resp)
}

func (h *messageHandler) stmt2ValidateAndLock(ctx context.Context, session *melody.Session, action string, reqID uint64, stmtID uint64, logger *logrus.Entry, isDebug bool) (stmtItem *StmtItem, locked bool) {
	stmtItem = h.stmts.GetStmt2(stmtID)
	if stmtItem == nil {
		logger.Errorf("stmt2 is nil, stmt_id:%d", stmtID)
		stmtErrorResponse(ctx, session, logger, action, reqID, 0xffff, "stmt2 is nil", stmtID)
		return nil, false
	}
	s := log.GetLogNow(isDebug)
	logger.Trace("get stmt2 lock")
	stmtItem.Lock()
	logger.Debugf("get stmt2 lock cost:%s", log.GetLogDuration(isDebug, s))
	if stmtItem.stmt == nil {
		stmtItem.Unlock()
		logger.Errorf("stmt2 has been freed, stmt_id:%d", stmtID)
		stmtErrorResponse(ctx, session, logger, action, reqID, 0xffff, "stmt has been freed", stmtID)
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

type prepareFields struct {
	stmtCommon.StmtField
	BindType int8 `json:"bind_type"`
}

type stmt2PrepareResponse struct {
	Code        int              `json:"code"`
	Message     string           `json:"message"`
	Action      string           `json:"action"`
	ReqID       uint64           `json:"req_id"`
	Timing      int64            `json:"timing"`
	StmtID      uint64           `json:"stmt_id"`
	IsInsert    bool             `json:"is_insert"`
	Fields      []*prepareFields `json:"fields"`
	FieldsCount int              `json:"fields_count"`
}

func (h *messageHandler) stmt2Prepare(ctx context.Context, session *melody.Session, action string, req stmt2PrepareRequest, logger *logrus.Entry, isDebug bool) {
	logger.Debugf("stmt2 prepare, stmt_id:%d, sql:%s", req.StmtID, req.SQL)
	stmtItem, locked := h.stmt2ValidateAndLock(ctx, session, action, req.ReqID, req.StmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	stmt2 := stmtItem.stmt
	code := syncinterface.TaosStmt2Prepare(stmt2, req.SQL, logger, isDebug)
	if code != 0 {
		errStr := wrapper.TaosStmt2Error(stmt2)
		logger.Errorf("stmt2 prepare error, err:%s", errStr)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr, req.StmtID)
		return
	}
	logger.Tracef("stmt2 prepare success, stmt_id:%d", req.StmtID)
	isInsert, code := syncinterface.TaosStmt2IsInsert(stmt2, logger, isDebug)
	if code != 0 {
		errStr := wrapper.TaosStmt2Error(stmt2)
		logger.Errorf("check stmt2 is insert error, err:%s", errStr)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr, req.StmtID)
		return
	}
	logger.Tracef("stmt2 is insert:%t", isInsert)
	stmtItem.isInsert = isInsert
	prepareResp := &stmt2PrepareResponse{StmtID: req.StmtID, IsInsert: isInsert}
	if req.GetFields {
		if isInsert {
			var fields []*prepareFields
			// get table field
			_, count, code, errStr := getFields(stmt2, stmtCommon.TAOS_FIELD_TBNAME, logger, isDebug)
			if code != 0 {
				logger.Errorf("get table names fields error, code:%d, err:%s", code, errStr)
				stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, fmt.Sprintf("get table names fields error, %s", errStr), req.StmtID)
				return
			}
			if count == 1 {
				tableNameFields := &prepareFields{
					StmtField: stmtCommon.StmtField{},
					BindType:  stmtCommon.TAOS_FIELD_TBNAME,
				}
				fields = append(fields, tableNameFields)
			}
			// get tags field
			tagFields, _, code, errStr := getFields(stmt2, stmtCommon.TAOS_FIELD_TAG, logger, isDebug)
			if code != 0 {
				logger.Errorf("get tag fields error, code:%d, err:%s", code, errStr)
				stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, fmt.Sprintf("get tag fields error, %s", errStr), req.StmtID)
				return
			}
			for i := 0; i < len(tagFields); i++ {
				fields = append(fields, &prepareFields{
					StmtField: *tagFields[i],
					BindType:  stmtCommon.TAOS_FIELD_TAG,
				})
			}
			// get cols field
			colFields, _, code, errStr := getFields(stmt2, stmtCommon.TAOS_FIELD_COL, logger, isDebug)
			if code != 0 {
				logger.Errorf("get col fields error, code:%d, err:%s", code, errStr)
				stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, fmt.Sprintf("get col fields error, %s", errStr), req.StmtID)
				return
			}
			for i := 0; i < len(colFields); i++ {
				fields = append(fields, &prepareFields{
					StmtField: *colFields[i],
					BindType:  stmtCommon.TAOS_FIELD_COL,
				})
			}
			prepareResp.Fields = fields
		} else {
			_, count, code, errStr := getFields(stmt2, stmtCommon.TAOS_FIELD_QUERY, logger, isDebug)
			if code != 0 {
				logger.Errorf("get query fields error, code:%d, err:%s", code, errStr)
				stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, fmt.Sprintf("get query fields error, %s", errStr), req.StmtID)
				return
			}
			prepareResp.FieldsCount = count
		}
	}
	prepareResp.ReqID = req.ReqID
	prepareResp.Action = action
	prepareResp.Timing = wstool.GetDuration(ctx)
	wstool.WSWriteJson(session, logger, prepareResp)
}

func getFields(stmt2 unsafe.Pointer, fieldType int8, logger *logrus.Entry, isDebug bool) (fields []*stmtCommon.StmtField, count int, code int, errSt string) {
	var cFields unsafe.Pointer
	code, count, cFields = syncinterface.TaosStmt2GetFields(stmt2, int(fieldType), logger, isDebug)
	if code != 0 {
		errStr := wrapper.TaosStmt2Error(stmt2)
		logger.Errorf("stmt2 get fields error, field_type:%d, err:%s", fieldType, errStr)
		return nil, count, code, errStr
	}
	defer wrapper.TaosStmt2FreeFields(stmt2, cFields)
	if count > 0 && cFields != nil {
		s := log.GetLogNow(isDebug)
		fields = wrapper.StmtParseFields(count, cFields)
		logger.Debugf("stmt2 parse fields cost:%s", log.GetLogDuration(isDebug, s))
		return fields, count, 0, ""
	}
	return nil, count, 0, ""
}

type stmt2GetFieldsRequest struct {
	ReqID      uint64 `json:"req_id"`
	StmtID     uint64 `json:"stmt_id"`
	FieldTypes []int8 `json:"field_types"`
}

type stmt2GetFieldsResponse struct {
	Code       int                     `json:"code"`
	Message    string                  `json:"message"`
	Action     string                  `json:"action"`
	ReqID      uint64                  `json:"req_id"`
	Timing     int64                   `json:"timing"`
	StmtID     uint64                  `json:"stmt_id"`
	TableCount int32                   `json:"table_count"`
	QueryCount int32                   `json:"query_count"`
	ColFields  []*stmtCommon.StmtField `json:"col_fields"`
	TagFields  []*stmtCommon.StmtField `json:"tag_fields"`
}

func (h *messageHandler) stmt2GetFields(ctx context.Context, session *melody.Session, action string, req stmt2GetFieldsRequest, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("stmt2 get col fields, stmt_id:%d", req.StmtID)
	stmtItem, locked := h.stmt2ValidateAndLock(ctx, session, action, req.ReqID, req.StmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	stmt2GetFieldsResp := &stmt2GetFieldsResponse{StmtID: req.StmtID}
	for i := 0; i < len(req.FieldTypes); i++ {
		switch req.FieldTypes[i] {
		case stmtCommon.TAOS_FIELD_COL:
			colFields, _, code, errStr := getFields(stmtItem.stmt, stmtCommon.TAOS_FIELD_COL, logger, isDebug)
			if code != 0 {
				logger.Errorf("get col fields error, code:%d, err:%s", code, errStr)
				stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, fmt.Sprintf("get col fields error, %s", errStr), req.StmtID)
				return
			}
			stmt2GetFieldsResp.ColFields = colFields
		case stmtCommon.TAOS_FIELD_TAG:
			tagFields, _, code, errStr := getFields(stmtItem.stmt, stmtCommon.TAOS_FIELD_TAG, logger, isDebug)
			if code != 0 {
				logger.Errorf("get tag fields error, code:%d, err:%s", code, errStr)
				stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, fmt.Sprintf("get tag fields error, %s", errStr), req.StmtID)
				return
			}
			stmt2GetFieldsResp.TagFields = tagFields
		case stmtCommon.TAOS_FIELD_TBNAME:
			_, count, code, errStr := getFields(stmtItem.stmt, stmtCommon.TAOS_FIELD_TBNAME, logger, isDebug)
			if code != 0 {
				logger.Errorf("get table names fields error, code:%d, err:%s", code, errStr)
				stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, fmt.Sprintf("get table names fields error, %s", errStr), req.StmtID)
				return
			}
			stmt2GetFieldsResp.TableCount = int32(count)
		case stmtCommon.TAOS_FIELD_QUERY:
			_, count, code, errStr := getFields(stmtItem.stmt, stmtCommon.TAOS_FIELD_QUERY, logger, isDebug)
			if code != 0 {
				logger.Errorf("get query fields error, code:%d, err:%s", code, errStr)
				stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, fmt.Sprintf("get query fields error, %s", errStr), req.StmtID)
				return
			}
			stmt2GetFieldsResp.QueryCount = int32(count)
		}
	}
	stmt2GetFieldsResp.ReqID = req.ReqID
	stmt2GetFieldsResp.Action = action
	stmt2GetFieldsResp.Timing = wstool.GetDuration(ctx)
	wstool.WSWriteJson(session, logger, stmt2GetFieldsResp)
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

func (h *messageHandler) stmt2Exec(ctx context.Context, session *melody.Session, action string, req stmt2ExecRequest, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("stmt2 execute, stmt_id:%d", req.StmtID)
	stmtItem, locked := h.stmt2ValidateAndLock(ctx, session, action, req.ReqID, req.StmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	code := syncinterface.TaosStmt2Exec(stmtItem.stmt, logger, isDebug)
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(stmtItem.stmt)
		logger.Errorf("stmt2 execute error, err:%s", errStr)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr, req.StmtID)
		return
	}
	s := log.GetLogNow(isDebug)
	logger.Tracef("stmt2 execute wait callback, stmt_id:%d", req.StmtID)
	result := <-stmtItem.caller.ExecResult
	logger.Debugf("stmt2 execute wait callback finish, affected:%d, res:%p, n:%d, cost:%s", result.Affected, result.Res, result.N, log.GetLogDuration(isDebug, s))
	stmtItem.result = result.Res
	resp := &stmt2ExecResponse{
		Action:   action,
		ReqID:    req.ReqID,
		Timing:   wstool.GetDuration(ctx),
		StmtID:   req.StmtID,
		Affected: result.Affected,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type stmt2UseResultRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type stmt2UseResultResponse struct {
	Code          int                `json:"code"`
	Message       string             `json:"message"`
	Action        string             `json:"action"`
	ReqID         uint64             `json:"req_id"`
	Timing        int64              `json:"timing"`
	StmtID        uint64             `json:"stmt_id"`
	ResultID      uint64             `json:"result_id"`
	FieldsCount   int                `json:"fields_count"`
	FieldsNames   []string           `json:"fields_names"`
	FieldsTypes   jsontype.JsonUint8 `json:"fields_types"`
	FieldsLengths []int64            `json:"fields_lengths"`
	Precision     int                `json:"precision"`
}

func (h *messageHandler) stmt2UseResult(ctx context.Context, session *melody.Session, action string, req stmt2UseResultRequest, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("stmt2 use result, stmt_id:%d", req.StmtID)
	stmtItem, locked := h.stmt2ValidateAndLock(ctx, session, action, req.ReqID, req.StmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	result := stmtItem.result
	fieldsCount := wrapper.TaosNumFields(result)
	rowsHeader, _ := wrapper.ReadColumn(result, fieldsCount)
	precision := wrapper.TaosResultPrecision(result)
	logger.Tracef("stmt use result success, stmt_id:%d, fields_count:%d, precision:%d", req.StmtID, fieldsCount, precision)
	queryResult := QueryResult{TaosResult: result, FieldsCount: fieldsCount, Header: rowsHeader, precision: precision, inStmt: true}
	idx := h.queryResults.Add(&queryResult)
	resp := &stmt2UseResultResponse{
		Action:        action,
		ReqID:         req.ReqID,
		Timing:        wstool.GetDuration(ctx),
		StmtID:        req.StmtID,
		ResultID:      idx,
		FieldsCount:   fieldsCount,
		FieldsNames:   rowsHeader.ColNames,
		FieldsTypes:   rowsHeader.ColTypes,
		FieldsLengths: rowsHeader.ColLength,
		Precision:     precision,
	}
	wstool.WSWriteJson(session, logger, resp)
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

func (h *messageHandler) stmt2Close(ctx context.Context, session *melody.Session, action string, req stmt2CloseRequest, logger *logrus.Entry) {
	logger.Tracef("stmt2 close, stmt_id:%d", req.StmtID)
	err := h.stmts.FreeStmtByID(req.StmtID, true, logger)
	if err != nil {
		logger.Errorf("stmt2 close error, err:%s", err.Error())
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, err.Error(), req.StmtID)
		return
	}
	logger.Tracef("stmt2 close success, stmt_id:%d", req.StmtID)
	resp := &stmt2CloseResponse{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
		StmtID: req.StmtID,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type stmt2BindResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (h *messageHandler) stmt2BinaryBind(ctx context.Context, session *melody.Session, action string, reqID uint64, stmtID uint64, message []byte, logger *logrus.Entry, isDebug bool) {
	if len(message) < 30 {
		logger.Errorf("message length is too short, len:%d, stmt_id:%d", len(message), stmtID)
		stmtErrorResponse(ctx, session, logger, action, reqID, 0xffff, "message length is too short", stmtID)
		return
	}
	v := binary.LittleEndian.Uint16(message[24:])
	if v != Stmt2BindProtocolVersion1 {
		logger.Errorf("unknown stmt2 bind version, version:%d, stmt_id:%d", v, stmtID)
		stmtErrorResponse(ctx, session, logger, action, reqID, 0xffff, "unknown stmt2 bind version", stmtID)
		return
	}
	colIndex := int32(binary.LittleEndian.Uint32(message[26:]))
	stmtItem, locked := h.stmt2ValidateAndLock(ctx, session, action, reqID, stmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	bindData := message[30:]
	err := syncinterface.TaosStmt2BindBinary(stmtItem.stmt, bindData, colIndex, logger, isDebug)
	if err != nil {
		logger.Errorf("stmt2 bind error, err:%s", err.Error())
		var tError *errors2.TaosError
		if errors.As(err, &tError) {
			stmtErrorResponse(ctx, session, logger, action, reqID, int(tError.Code), tError.ErrStr, stmtID)
			return
		}
		stmtErrorResponse(ctx, session, logger, action, reqID, 0xffff, err.Error(), stmtID)
		return
	}
	logger.Trace("stmt2 bind success")
	resp := &stmt2BindResponse{
		Action: action,
		ReqID:  reqID,
		Timing: wstool.GetDuration(ctx),
		StmtID: stmtID,
	}
	wstool.WSWriteJson(session, logger, resp)
}

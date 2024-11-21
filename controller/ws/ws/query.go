package ws

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v3/common"
	errors2 "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/tools/bytesutil"
	"github.com/taosdata/taosadapter/v3/tools/jsontype"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

type connRequest struct {
	ReqID    uint64 `json:"req_id"`
	User     string `json:"user"`
	Password string `json:"password"`
	DB       string `json:"db"`
	Mode     *int   `json:"mode"`
}

func (h *messageHandler) connect(ctx context.Context, session *melody.Session, action string, req *connRequest, logger *logrus.Entry, isDebug bool) {
	h.lock(logger, isDebug)
	defer h.Unlock()
	if h.closed {
		logger.Trace("server closed")
		return
	}
	if h.conn != nil {
		logger.Trace("duplicate connections")
		commonErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, "duplicate connections")
		return
	}

	conn, err := syncinterface.TaosConnect("", req.User, req.Password, req.DB, 0, logger, isDebug)

	if err != nil {
		logger.Errorf("connect to TDengine error, err:%s", err)
		var taosErr *errors2.TaosError
		errors.As(err, &taosErr)
		commonErrorResponse(ctx, session, logger, action, req.ReqID, int(taosErr.Code), taosErr.ErrStr)
		return
	}
	logger.Trace("get whitelist")
	s := log.GetLogNow(isDebug)
	whitelist, err := tool.GetWhitelist(conn)
	logger.Debugf("get whitelist cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.Errorf("get whitelist error, err:%s", err)
		syncinterface.TaosClose(conn, logger, isDebug)
		var taosErr *errors2.TaosError
		errors.As(err, &taosErr)
		commonErrorResponse(ctx, session, logger, action, req.ReqID, int(taosErr.Code), taosErr.ErrStr)
		return
	}
	logger.Tracef("check whitelist, ip:%s, whitelist:%s", h.ipStr, tool.IpNetSliceToString(whitelist))
	valid := tool.CheckWhitelist(whitelist, h.ip)
	if !valid {
		logger.Errorf("ip not in whitelist, ip:%s, whitelist:%s", h.ipStr, tool.IpNetSliceToString(whitelist))
		syncinterface.TaosClose(conn, logger, isDebug)
		commonErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, "whitelist prohibits current IP access")
		return
	}
	s = log.GetLogNow(isDebug)
	logger.Trace("register whitelist change")
	err = tool.RegisterChangeWhitelist(conn, h.whitelistChangeHandle)
	logger.Debugf("register whitelist change cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.Errorf("register whitelist change error, err:%s", err)
		syncinterface.TaosClose(conn, logger, isDebug)
		var taosErr *errors2.TaosError
		errors.As(err, &taosErr)
		commonErrorResponse(ctx, session, logger, action, req.ReqID, int(taosErr.Code), taosErr.ErrStr)
		return
	}
	s = log.GetLogNow(isDebug)
	logger.Trace("register drop user")
	err = tool.RegisterDropUser(conn, h.dropUserHandle)
	logger.Debugf("register drop user cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.Errorf("register drop user error, err:%s", err)
		syncinterface.TaosClose(conn, logger, isDebug)
		var taosErr *errors2.TaosError
		errors.As(err, &taosErr)
		commonErrorResponse(ctx, session, logger, action, req.ReqID, int(taosErr.Code), taosErr.ErrStr)
		return
	}
	if req.Mode != nil {
		switch *req.Mode {
		case common.TAOS_CONN_MODE_BI:
			// BI mode
			logger.Trace("set connection mode to BI")
			code := wrapper.TaosSetConnMode(conn, common.TAOS_CONN_MODE_BI, 1)
			logger.Trace("set connection mode to BI done")
			if code != 0 {
				logger.Errorf("set connection mode to BI error, err:%s", wrapper.TaosErrorStr(nil))
				syncinterface.TaosClose(conn, logger, isDebug)
				commonErrorResponse(ctx, session, logger, action, req.ReqID, code, wrapper.TaosErrorStr(nil))
				return
			}
		default:
			syncinterface.TaosClose(conn, logger, isDebug)
			logger.Tracef("unexpected mode:%d", *req.Mode)
			commonErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, fmt.Sprintf("unexpected mode:%d", *req.Mode))
			return
		}
	}
	h.conn = conn
	logger.Trace("start wait signal goroutine")
	go h.waitSignal(h.logger)
	commonSuccessResponse(ctx, session, logger, action, req.ReqID)
}

type queryRequest struct {
	ReqID uint64 `json:"req_id"`
	Sql   string `json:"sql"`
}

type queryResponse struct {
	Code          int                `json:"code"`
	Message       string             `json:"message"`
	Action        string             `json:"action"`
	ReqID         uint64             `json:"req_id"`
	Timing        int64              `json:"timing"`
	ID            uint64             `json:"id"`
	IsUpdate      bool               `json:"is_update"`
	AffectedRows  int                `json:"affected_rows"`
	FieldsCount   int                `json:"fields_count"`
	FieldsNames   []string           `json:"fields_names"`
	FieldsTypes   jsontype.JsonUint8 `json:"fields_types"`
	FieldsLengths []int64            `json:"fields_lengths"`
	Precision     int                `json:"precision"`
}

func (h *messageHandler) query(ctx context.Context, session *melody.Session, action string, req *queryRequest, logger *logrus.Entry, isDebug bool) {
	sqlType := monitor.WSRecordRequest(req.Sql)
	logger.Debugf("get query request, sql:%s", req.Sql)
	s := log.GetLogNow(isDebug)
	handler := async.GlobalAsync.HandlerPool.Get()
	defer async.GlobalAsync.HandlerPool.Put(handler)
	logger.Debugf("get handler cost:%s", log.GetLogDuration(isDebug, s))
	result := async.GlobalAsync.TaosQuery(h.conn, logger, isDebug, req.Sql, handler, int64(req.ReqID))
	code := wrapper.TaosError(result.Res)
	if code != 0 {
		monitor.WSRecordResult(sqlType, false)
		errStr := wrapper.TaosErrorStr(result.Res)
		logger.Errorf("query error, code:%d, message:%s", code, errStr)
		syncinterface.FreeResult(result.Res, logger, isDebug)
		commonErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr)
		return
	}

	monitor.WSRecordResult(sqlType, true)
	logger.Trace("check is_update_query")
	s = log.GetLogNow(isDebug)
	isUpdate := wrapper.TaosIsUpdateQuery(result.Res)
	logger.Debugf("get is_update_query %t, cost:%s", isUpdate, log.GetLogDuration(isDebug, s))
	if isUpdate {
		s = log.GetLogNow(isDebug)
		affectRows := wrapper.TaosAffectedRows(result.Res)
		logger.Debugf("affected_rows %d cost:%s", affectRows, log.GetLogDuration(isDebug, s))
		syncinterface.FreeResult(result.Res, logger, isDebug)
		resp := queryResponse{
			Action:       action,
			ReqID:        req.ReqID,
			Timing:       wstool.GetDuration(ctx),
			IsUpdate:     true,
			AffectedRows: affectRows,
		}
		wstool.WSWriteJson(session, logger, resp)
		return
	}
	s = log.GetLogNow(isDebug)
	fieldsCount := wrapper.TaosNumFields(result.Res)
	logger.Debugf("get num_fields:%d, cost:%s", fieldsCount, log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	rowsHeader, _ := wrapper.ReadColumn(result.Res, fieldsCount)
	logger.Debugf("read column cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	precision := wrapper.TaosResultPrecision(result.Res)
	logger.Debugf("get result_precision:%d, cost:%s", precision, log.GetLogDuration(isDebug, s))
	queryResult := QueryResult{TaosResult: result.Res, FieldsCount: fieldsCount, Header: rowsHeader, precision: precision}
	idx := h.queryResults.Add(&queryResult)
	logger.Trace("add result to list finished")
	resp := queryResponse{
		Action:        action,
		ReqID:         req.ReqID,
		Timing:        wstool.GetDuration(ctx),
		ID:            idx,
		FieldsCount:   fieldsCount,
		FieldsNames:   rowsHeader.ColNames,
		FieldsLengths: rowsHeader.ColLength,
		FieldsTypes:   rowsHeader.ColTypes,
		Precision:     precision,
	}
	wstool.WSWriteJson(session, logger, resp)
}

func (h *messageHandler) binaryQuery(ctx context.Context, session *melody.Session, action string, reqID uint64, message []byte, logger *logrus.Entry, isDebug bool) {
	if len(message) < 31 {
		commonErrorResponse(ctx, session, logger, action, reqID, 0xffff, "message length is too short")
		return
	}
	v := binary.LittleEndian.Uint16(message[24:])
	var sql []byte
	if v == BinaryProtocolVersion1 {
		sqlLen := binary.LittleEndian.Uint32(message[26:])
		remainMessageLength := len(message) - 30
		if remainMessageLength < int(sqlLen) {
			commonErrorResponse(ctx, session, logger, action, reqID, 0xffff, fmt.Sprintf("uncompleted message, sql length:%d, remainMessageLength:%d", sqlLen, remainMessageLength))
			return
		}
		sql = message[30 : 30+sqlLen]
	} else {
		logger.Errorf("unknown binary query version:%d", v)
		commonErrorResponse(ctx, session, logger, action, reqID, 0xffff, fmt.Sprintf("unknown binary query version:%d", v))
		return
	}
	logger.Debugf("binary query, sql:%s", log.GetLogSql(bytesutil.ToUnsafeString(sql)))
	sqlType := monitor.WSRecordRequest(bytesutil.ToUnsafeString(sql))
	s := log.GetLogNow(isDebug)
	handler := async.GlobalAsync.HandlerPool.Get()
	defer async.GlobalAsync.HandlerPool.Put(handler)
	logger.Debugf("get handler cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	result := async.GlobalAsync.TaosQuery(h.conn, logger, isDebug, bytesutil.ToUnsafeString(sql), handler, int64(reqID))
	logger.Debugf("query cost:%s", log.GetLogDuration(isDebug, s))
	code := wrapper.TaosError(result.Res)
	if code != 0 {
		monitor.WSRecordResult(sqlType, false)
		errStr := wrapper.TaosErrorStr(result.Res)
		logger.Errorf("taos query error, code:%d, msg:%s, sql:%s", code, errStr, log.GetLogSql(bytesutil.ToUnsafeString(sql)))
		syncinterface.FreeResult(result.Res, logger, isDebug)
		commonErrorResponse(ctx, session, logger, action, reqID, code, errStr)
		return
	}
	monitor.WSRecordResult(sqlType, true)
	s = log.GetLogNow(isDebug)
	isUpdate := wrapper.TaosIsUpdateQuery(result.Res)
	logger.Debugf("get is_update_query %t, cost:%s", isUpdate, log.GetLogDuration(isDebug, s))
	if isUpdate {
		affectRows := wrapper.TaosAffectedRows(result.Res)
		logger.Debugf("affected_rows %d cost:%s", affectRows, log.GetLogDuration(isDebug, s))
		syncinterface.FreeResult(result.Res, logger, isDebug)
		resp := &queryResponse{
			Action:       action,
			ReqID:        reqID,
			Timing:       wstool.GetDuration(ctx),
			IsUpdate:     true,
			AffectedRows: affectRows,
		}
		wstool.WSWriteJson(session, logger, resp)
		return
	}
	s = log.GetLogNow(isDebug)
	fieldsCount := wrapper.TaosNumFields(result.Res)
	logger.Debugf("num_fields cost:%s", log.GetLogDuration(isDebug, s))
	rowsHeader, _ := wrapper.ReadColumn(result.Res, fieldsCount)
	s = log.GetLogNow(isDebug)
	logger.Debugf("read column cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	precision := wrapper.TaosResultPrecision(result.Res)
	logger.Debugf("result_precision cost:%s", log.GetLogDuration(isDebug, s))
	queryResult := QueryResult{TaosResult: result.Res, FieldsCount: fieldsCount, Header: rowsHeader, precision: precision}
	idx := h.queryResults.Add(&queryResult)
	logger.Trace("query success")
	resp := &queryResponse{
		Action:        action,
		ReqID:         reqID,
		Timing:        wstool.GetDuration(ctx),
		ID:            idx,
		FieldsCount:   fieldsCount,
		FieldsNames:   rowsHeader.ColNames,
		FieldsLengths: rowsHeader.ColLength,
		FieldsTypes:   rowsHeader.ColTypes,
		Precision:     precision,
	}
	wstool.WSWriteJson(session, logger, resp)
}

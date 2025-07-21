package ws

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	taoserrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/monitor/recordsql"
	"github.com/taosdata/taosadapter/v3/tools/bytesutil"
	"github.com/taosdata/taosadapter/v3/tools/jsontype"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

func handleConnectError(ctx context.Context, conn unsafe.Pointer, session *melody.Session, logger *logrus.Entry, isDebug bool, action string, reqID uint64, err error, errorExt string) {
	var code int
	var errStr string
	taosError, ok := err.(*taoserrors.TaosError)
	if ok {
		code = int(taosError.Code)
		errStr = taosError.ErrStr
	} else {
		code = 0xffff
		errStr = err.Error()
	}
	logger.Errorf("%s, code:%d, message:%s", errorExt, code, errStr)
	syncinterface.TaosClose(conn, logger, isDebug)
	commonErrorResponse(ctx, session, logger, action, reqID, code, errStr)
}

type queryRequest struct {
	ReqID uint64 `json:"req_id"`
	Sql   string `json:"sql"`
}

type queryResponse struct {
	Code             int                `json:"code"`
	Message          string             `json:"message"`
	Action           string             `json:"action"`
	ReqID            uint64             `json:"req_id"`
	Timing           int64              `json:"timing"`
	ID               uint64             `json:"id"`
	IsUpdate         bool               `json:"is_update"`
	AffectedRows     int                `json:"affected_rows"`
	FieldsCount      int                `json:"fields_count"`
	FieldsNames      []string           `json:"fields_names"`
	FieldsTypes      jsontype.JsonUint8 `json:"fields_types"`
	FieldsLengths    []int64            `json:"fields_lengths"`
	Precision        int                `json:"precision"`
	FieldsPrecisions []int64            `json:"fields_precisions"`
	FieldsScales     []int64            `json:"fields_scales"`
}

func (h *messageHandler) query(ctx context.Context, session *melody.Session, action string, req queryRequest, innerReqID uint64, logger *logrus.Entry, isDebug bool) {
	record, recordSql := recordsql.GetSQLRecord()
	var recordTime time.Time
	if recordSql {
		record.Init(req.Sql, h.ipStr, h.user, recordsql.WSType, innerReqID, time.Now())
	}
	sqlType := monitor.WSRecordRequest(req.Sql)
	logger.Debugf("get query request, sql:%s", req.Sql)
	s := log.GetLogNow(isDebug)
	logger.Trace("get handler from pool")
	handler := async.GlobalAsync.HandlerPool.Get()
	logger.Tracef("get handler cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		async.GlobalAsync.HandlerPool.Put(handler)
		logger.Trace("put handler back to pool")
	}()
	if recordSql {
		recordTime = time.Now()
	}
	result := async.GlobalAsync.TaosQuery(h.conn, logger, isDebug, req.Sql, handler, int64(innerReqID))
	if recordSql {
		record.SetQueryDuration(time.Since(recordTime))
	}
	code := syncinterface.TaosError(result.Res, logger, isDebug)
	if code != 0 {
		if recordSql {
			record.SetFreeTime(time.Now())
			recordsql.PutSQLRecord(record)
		}
		monitor.WSRecordResult(sqlType, false)
		errStr := syncinterface.TaosErrorStr(result.Res, logger, isDebug)
		logger.Errorf("query error, code:%d, message:%s", code, errStr)
		async.FreeResultAsync(result.Res, logger, isDebug)
		commonErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr)
		return
	}

	monitor.WSRecordResult(sqlType, true)
	logger.Trace("check is_update_query")
	s = log.GetLogNow(isDebug)
	isUpdate := syncinterface.TaosIsUpdateQuery(result.Res, logger, isDebug)
	logger.Tracef("get is_update_query %t, cost:%s", isUpdate, log.GetLogDuration(isDebug, s))
	if isUpdate {
		if recordSql {
			record.SetFreeTime(time.Now())
			recordsql.PutSQLRecord(record)
		}
		s = log.GetLogNow(isDebug)
		affectRows := syncinterface.TaosAffectedRows(result.Res, logger, isDebug)
		logger.Debugf("affected_rows %d, cost:%s", affectRows, log.GetLogDuration(isDebug, s))
		async.FreeResultAsync(result.Res, logger, isDebug)
		resp := &queryResponse{
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
	fieldsCount := syncinterface.TaosNumFields(result.Res, logger, isDebug)
	logger.Tracef("get num_fields:%d, cost:%s", fieldsCount, log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	rowsHeader, _ := syncinterface.ReadColumn(result.Res, fieldsCount, logger, isDebug)
	logger.Tracef("read column cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	precision := syncinterface.TaosResultPrecision(result.Res, logger, isDebug)
	logger.Tracef("get result_precision:%d, cost:%s", precision, log.GetLogDuration(isDebug, s))
	queryResult := QueryResult{TaosResult: result.Res, FieldsCount: fieldsCount, Header: rowsHeader, precision: precision}
	if recordSql {
		queryResult.record = record
	}
	idx := h.queryResults.Add(&queryResult)
	logger.Debug("query success")
	resp := &queryResponse{
		Action:           action,
		ReqID:            req.ReqID,
		Timing:           wstool.GetDuration(ctx),
		ID:               idx,
		FieldsCount:      fieldsCount,
		FieldsNames:      rowsHeader.ColNames,
		FieldsLengths:    rowsHeader.ColLength,
		FieldsTypes:      rowsHeader.ColTypes,
		Precision:        precision,
		FieldsPrecisions: rowsHeader.Precisions,
		FieldsScales:     rowsHeader.Scales,
	}
	wstool.WSWriteJson(session, logger, resp)
}

func (h *messageHandler) binaryQuery(ctx context.Context, session *melody.Session, action string, reqID uint64, message []byte, innerReqID uint64, logger *logrus.Entry, isDebug bool) {
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
	var reqSql = bytesutil.ToUnsafeString(sql)
	record, recordSql := recordsql.GetSQLRecord()
	var recordTime time.Time
	if recordSql {
		// copy sql to record, can not use reqSql directly, because it is only alive in this function scope
		sqlStr := string(sql)
		record.Init(sqlStr, h.ipStr, h.user, recordsql.WSType, innerReqID, time.Now())
	}
	logger.Debugf("binary query, sql:%s", log.GetLogSql(reqSql))
	sqlType := monitor.WSRecordRequest(reqSql)
	s := log.GetLogNow(isDebug)
	logger.Trace("get handler from pool")
	handler := async.GlobalAsync.HandlerPool.Get()
	logger.Tracef("get handler cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		async.GlobalAsync.HandlerPool.Put(handler)
		logger.Trace("put handler back to pool")
	}()
	s = log.GetLogNow(isDebug)
	if recordSql {
		recordTime = time.Now()
	}
	result := async.GlobalAsync.TaosQuery(h.conn, logger, isDebug, reqSql, handler, int64(innerReqID))
	if recordSql {
		record.SetQueryDuration(time.Since(recordTime))
	}
	logger.Tracef("query cost:%s", log.GetLogDuration(isDebug, s))
	code := syncinterface.TaosError(result.Res, logger, isDebug)
	if code != 0 {
		if recordSql {
			record.SetFreeTime(time.Now())
			recordsql.PutSQLRecord(record)
		}
		monitor.WSRecordResult(sqlType, false)
		errStr := syncinterface.TaosErrorStr(result.Res, logger, isDebug)
		logger.Errorf("taos query error, code:%d, msg:%s, sql:%s", code, errStr, log.GetLogSql(reqSql))
		async.FreeResultAsync(result.Res, logger, isDebug)
		commonErrorResponse(ctx, session, logger, action, reqID, code, errStr)
		return
	}
	monitor.WSRecordResult(sqlType, true)
	s = log.GetLogNow(isDebug)
	isUpdate := syncinterface.TaosIsUpdateQuery(result.Res, logger, isDebug)
	logger.Tracef("get is_update_query %t, cost:%s", isUpdate, log.GetLogDuration(isDebug, s))
	if isUpdate {
		if recordSql {
			record.SetFreeTime(time.Now())
			recordsql.PutSQLRecord(record)
		}
		affectRows := syncinterface.TaosAffectedRows(result.Res, logger, isDebug)
		logger.Debugf("affected_rows %d cost:%s", affectRows, log.GetLogDuration(isDebug, s))
		async.FreeResultAsync(result.Res, logger, isDebug)
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
	fieldsCount := syncinterface.TaosNumFields(result.Res, logger, isDebug)
	logger.Tracef("num_fields cost:%s", log.GetLogDuration(isDebug, s))
	rowsHeader, _ := syncinterface.ReadColumn(result.Res, fieldsCount, logger, isDebug)
	s = log.GetLogNow(isDebug)
	logger.Tracef("read column cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	precision := syncinterface.TaosResultPrecision(result.Res, logger, isDebug)
	logger.Tracef("result_precision cost:%s", log.GetLogDuration(isDebug, s))
	queryResult := QueryResult{TaosResult: result.Res, FieldsCount: fieldsCount, Header: rowsHeader, precision: precision}
	if recordSql {
		queryResult.record = record
	}
	idx := h.queryResults.Add(&queryResult)
	logger.Debug("query success")
	resp := &queryResponse{
		Action:           action,
		ReqID:            reqID,
		Timing:           wstool.GetDuration(ctx),
		ID:               idx,
		FieldsCount:      fieldsCount,
		FieldsNames:      rowsHeader.ColNames,
		FieldsLengths:    rowsHeader.ColLength,
		FieldsTypes:      rowsHeader.ColTypes,
		Precision:        precision,
		FieldsPrecisions: rowsHeader.Precisions,
		FieldsScales:     rowsHeader.Scales,
	}
	wstool.WSWriteJson(session, logger, resp)
}

package ws

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/driver/common"
	taoserrors "github.com/taosdata/taosadapter/v3/driver/errors"
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
	TZ       string `json:"tz"`
	App      string `json:"app"`
	IP       string `json:"ip"`
}

func (h *messageHandler) connect(ctx context.Context, session *melody.Session, action string, req connRequest, innerReqID uint64, logger *logrus.Entry, isDebug bool) {
	h.lock(logger, isDebug)
	defer h.Unlock()
	if h.isClosed() {
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
		handleConnectError(ctx, conn, session, logger, isDebug, action, req.ReqID, err, "connect to TDengine error")
		return
	}
	logger.Trace("get whitelist")
	s := log.GetLogNow(isDebug)
	whitelist, err := tool.GetWhitelist(conn, logger, isDebug)
	logger.Debugf("get whitelist cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		handleConnectError(ctx, conn, session, logger, isDebug, action, req.ReqID, err, "get whitelist error")
		return
	}
	logger.Tracef("check whitelist, ip:%s, whitelist:%s", h.ipStr, tool.IpNetSliceToString(whitelist))
	valid := tool.CheckWhitelist(whitelist, h.ip)
	if !valid {
		err = errors.New("ip not in whitelist")
		handleConnectError(ctx, conn, session, logger, isDebug, action, req.ReqID, err, "ip not in whitelist")
		return
	}
	s = log.GetLogNow(isDebug)
	logger.Trace("register whitelist change")
	err = tool.RegisterChangeWhitelist(conn, h.whitelistChangeHandle, logger, isDebug)
	logger.Debugf("register whitelist change cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		handleConnectError(ctx, conn, session, logger, isDebug, action, req.ReqID, err, "register whitelist change error")
		return
	}
	s = log.GetLogNow(isDebug)
	logger.Trace("register drop user")
	err = tool.RegisterDropUser(conn, h.dropUserHandle, logger, isDebug)
	logger.Debugf("register drop user cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		handleConnectError(ctx, conn, session, logger, isDebug, action, req.ReqID, err, "register drop user error")
		return
	}
	if req.Mode != nil {
		switch *req.Mode {
		case common.TAOS_CONN_MODE_BI:
			// BI mode
			logger.Trace("set connection mode to BI")
			code := syncinterface.TaosSetConnMode(conn, common.TAOS_CONN_MODE_BI, 1, logger, isDebug)
			logger.Trace("set connection mode to BI done")
			if code != 0 {
				handleConnectError(ctx, conn, session, logger, isDebug, action, req.ReqID, taoserrors.NewError(code, syncinterface.TaosErrorStr(nil, logger, isDebug)), "set connection mode to BI error")
				return
			}
		default:
			err = fmt.Errorf("unexpected mode:%d", *req.Mode)
			handleConnectError(ctx, conn, session, logger, isDebug, action, req.ReqID, err, err.Error())
			return
		}
	}
	// set connection ip
	clientIP := h.ipStr
	if req.IP != "" {
		clientIP = req.IP
	}
	logger.Tracef("set connection ip, ip:%s", clientIP)
	code := syncinterface.TaosOptionsConnection(conn, common.TSDB_OPTION_CONNECTION_USER_IP, &clientIP, logger, isDebug)
	logger.Trace("set connection ip done")
	if code != 0 {
		handleConnectError(ctx, conn, session, logger, isDebug, action, req.ReqID, taoserrors.NewError(code, syncinterface.TaosErrorStr(nil, logger, isDebug)), "set connection ip error")
		return
	}
	// set timezone
	if req.TZ != "" {
		logger.Tracef("set timezone, tz:%s", req.TZ)
		code = syncinterface.TaosOptionsConnection(conn, common.TSDB_OPTION_CONNECTION_TIMEZONE, &req.TZ, logger, isDebug)
		logger.Trace("set timezone done")
		if code != 0 {
			handleConnectError(ctx, conn, session, logger, isDebug, action, req.ReqID, taoserrors.NewError(code, syncinterface.TaosErrorStr(nil, logger, isDebug)), "set timezone error")
			return
		}
	}
	// set connection app
	if req.App != "" {
		logger.Tracef("set app, app:%s", req.App)
		code = syncinterface.TaosOptionsConnection(conn, common.TSDB_OPTION_CONNECTION_USER_APP, &req.App, logger, isDebug)
		logger.Trace("set app done")
		if code != 0 {
			handleConnectError(ctx, conn, session, logger, isDebug, action, req.ReqID, taoserrors.NewError(code, syncinterface.TaosErrorStr(nil, logger, isDebug)), "set app error")
			return
		}
	}
	h.conn = conn
	logger.Trace("start wait signal goroutine")
	go h.waitSignal(h.logger)
	commonSuccessResponse(ctx, session, logger, action, req.ReqID)
}

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
	result := async.GlobalAsync.TaosQuery(h.conn, logger, isDebug, req.Sql, handler, int64(innerReqID))
	code := syncinterface.TaosError(result.Res, logger, isDebug)
	if code != 0 {
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
	logger.Debugf("binary query, sql:%s", log.GetLogSql(bytesutil.ToUnsafeString(sql)))
	sqlType := monitor.WSRecordRequest(bytesutil.ToUnsafeString(sql))
	s := log.GetLogNow(isDebug)
	logger.Trace("get handler from pool")
	handler := async.GlobalAsync.HandlerPool.Get()
	logger.Tracef("get handler cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		async.GlobalAsync.HandlerPool.Put(handler)
		logger.Trace("put handler back to pool")
	}()
	s = log.GetLogNow(isDebug)
	result := async.GlobalAsync.TaosQuery(h.conn, logger, isDebug, bytesutil.ToUnsafeString(sql), handler, int64(innerReqID))
	logger.Tracef("query cost:%s", log.GetLogDuration(isDebug, s))
	code := syncinterface.TaosError(result.Res, logger, isDebug)
	if code != 0 {
		monitor.WSRecordResult(sqlType, false)
		errStr := syncinterface.TaosErrorStr(result.Res, logger, isDebug)
		logger.Errorf("taos query error, code:%d, msg:%s, sql:%s", code, errStr, log.GetLogSql(bytesutil.ToUnsafeString(sql)))
		async.FreeResultAsync(result.Res, logger, isDebug)
		commonErrorResponse(ctx, session, logger, action, reqID, code, errStr)
		return
	}
	monitor.WSRecordResult(sqlType, true)
	s = log.GetLogNow(isDebug)
	isUpdate := syncinterface.TaosIsUpdateQuery(result.Res, logger, isDebug)
	logger.Tracef("get is_update_query %t, cost:%s", isUpdate, log.GetLogDuration(isDebug, s))
	if isUpdate {
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

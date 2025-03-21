package syncinterface

import (
	"database/sql/driver"
	"strings"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/driver/types"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/thread"
)

func FreeResult(res unsafe.Pointer, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("call taos_free_result, res:%p", res)
	if res == nil {
		logger.Trace("result is nil")
		return
	}
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for free result cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	wrapper.TaosFreeResult(res)
	logger.Debugf("taos_free_result finish, cost:%s", log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
}

func TaosClose(conn unsafe.Pointer, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("call taos_close, conn:%p", conn)
	if conn == nil {
		logger.Trace("connection is nil")
		return
	}
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_close cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	wrapper.TaosClose(conn)
	logger.Debugf("taos_close finish, cost:%s", log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
}

func TaosSelectDB(conn unsafe.Pointer, db string, logger *logrus.Entry, isDebug bool) int {
	logger.Tracef("call taos_select_db, conn:%p, db:%s", conn, db)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_select_db cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code := wrapper.TaosSelectDB(conn, db)
	logger.Debugf("taos_select_db finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return code
}

func TaosConnect(host, user, pass, db string, port int, logger *logrus.Entry, isDebug bool) (unsafe.Pointer, error) {
	logger.Tracef("call taos_connect, host:%s, user:%s, pass:%s, db:%s, port:%d", host, user, pass, db, port)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_connect cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	conn, err := wrapper.TaosConnect(host, user, pass, db, port)
	logger.Debugf("taos_connect finish, conn:%p, err:%v, cost:%s", conn, err, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return conn, err
}

func TaosGetTablesVgID(conn unsafe.Pointer, db string, tables []string, logger *logrus.Entry, isDebug bool) ([]int, int) {
	logger.Tracef("call taos_get_tables_vgId, conn:%p, db:%s, tables:%s", conn, db, strings.Join(tables, ", "))
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_get_tables_vgId cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	vgIDs, code := wrapper.TaosGetTablesVgID(conn, db, tables)
	logger.Debugf("taos_get_tables_vgId finish, vgid:%v, code:%d, cost:%s", vgIDs, code, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return vgIDs, code
}

func TaosStmtInitWithReqID(conn unsafe.Pointer, reqID int64, logger *logrus.Entry, isDebug bool) unsafe.Pointer {
	logger.Tracef("call taos_stmt_init_with_reqid, conn:%p, QID:0x%x", conn, reqID)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_stmt_init_with_reqid cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	stmtInit := wrapper.TaosStmtInitWithReqID(conn, reqID)
	logger.Debugf("taos_stmt_init_with_reqid result:%p, cost:%s", stmtInit, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return stmtInit
}

func TaosStmtPrepare(stmt unsafe.Pointer, sql string, logger *logrus.Entry, isDebug bool) int {
	logger.Tracef("call taos_stmt_init_with_reqid, stmt:%p,  sql:%s", stmt, log.GetLogSql(sql))
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_stmt_prepare cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code := wrapper.TaosStmtPrepare(stmt, sql)
	logger.Debugf("taos_stmt_prepare code:%d cost:%s", code, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return code
}

func TaosStmtIsInsert(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) (bool, int) {
	logger.Tracef("call taos_stmt_is_insert, stmt:%p", stmt)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_stmt_is_insert cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	isInsert, code := wrapper.TaosStmtIsInsert(stmt)
	logger.Debugf("taos_stmt_is_insert isInsert finish, insert:%t, code:%d, cost:%s", isInsert, code, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return isInsert, code
}

func TaosStmtSetTBName(stmt unsafe.Pointer, tbname string, logger *logrus.Entry, isDebug bool) int {
	logger.Tracef("call taos_stmt_set_tbname, stmt:%p, tbname:%s", stmt, tbname)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_stmt_set_tbname cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code := wrapper.TaosStmtSetTBName(stmt, tbname)
	logger.Debugf("taos_stmt_set_tbname finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return code
}

func TaosStmtGetTagFields(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) (int, int, unsafe.Pointer) {
	logger.Tracef("call taos_stmt_get_tag_fields, stmt:%p", stmt)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_stmt_get_tag_fields cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code, num, fields := wrapper.TaosStmtGetTagFields(stmt)
	logger.Debugf("taos_stmt_get_tag_fields finish, code:%d, num:%d, fields:%p, cost:%s", code, num, fields, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return code, num, fields
}

func TaosStmtSetTags(stmt unsafe.Pointer, tags []driver.Value, logger *logrus.Entry, isDebug bool) int {
	logger.Tracef("call taos_stmt_set_tags, stmt:%p, tags:%v", stmt, tags)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_stmt_set_tags cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code := wrapper.TaosStmtSetTags(stmt, tags)
	logger.Debugf("taos_stmt_set_tags finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return code
}

func TaosStmtGetColFields(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) (int, int, unsafe.Pointer) {
	logger.Tracef("call taos_stmt_get_col_fields, stmt:%p", stmt)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_stmt_get_col_fields cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code, num, fields := wrapper.TaosStmtGetColFields(stmt)
	logger.Debugf("taos_stmt_get_col_fields code:%d, num:%d, fields:%p, cost:%s", code, num, fields, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return code, num, fields
}

func TaosStmtBindParamBatch(stmt unsafe.Pointer, multiBind [][]driver.Value, bindType []*types.ColumnType, logger *logrus.Entry, isDebug bool) int {
	logger.Tracef("call taos_stmt_bind_param_batch, stmt:%p, multiBind:%v, bindType:%v", stmt, multiBind, bindType)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_stmt_bind_param_batch cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code := wrapper.TaosStmtBindParamBatch(stmt, multiBind, bindType)
	logger.Debugf("taos_stmt_bind_param_batch code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return code
}

func TaosStmtAddBatch(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) int {
	logger.Tracef("call taos_stmt_add_batch, stmt:%p", stmt)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_stmt_add_batch cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code := wrapper.TaosStmtAddBatch(stmt)
	logger.Debugf("taos_stmt_add_batch code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return code
}

func TaosStmtExecute(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) int {
	logger.Tracef("call taos_stmt_execute, stmt:%p", stmt)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_stmt_execute cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code := wrapper.TaosStmtExecute(stmt)
	logger.Debugf("taos_stmt_execute code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return code
}

func TaosStmtClose(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) int {
	logger.Tracef("call taos_stmt_close, stmt:%p", stmt)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_stmt_close cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code := wrapper.TaosStmtClose(stmt)
	logger.Debugf("taos_stmt_close code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return code
}

func TMQWriteRaw(conn unsafe.Pointer, raw unsafe.Pointer, logger *logrus.Entry, isDebug bool) int32 {
	logger.Tracef("call tmq_write_raw, conn:%p, raw:%p", conn, raw)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for tmq_write_raw cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code := wrapper.TMQWriteRaw(conn, raw)
	logger.Debugf("tmq_write_raw finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return code
}

func TaosWriteRawBlockWithReqID(conn unsafe.Pointer, numOfRows int, pData unsafe.Pointer, tableName string, reqID int64, logger *logrus.Entry, isDebug bool) int {
	logger.Tracef("call taos_write_raw_block_with_reqid, conn:%p, numOfRows:%d, pData:%p, tableName:%s, reqID:%d", conn, numOfRows, pData, tableName, reqID)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_write_raw_block_with_reqid cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code := wrapper.TaosWriteRawBlockWithReqID(conn, numOfRows, pData, tableName, reqID)
	logger.Debugf("taos_write_raw_block_with_reqid finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return code
}

func TaosWriteRawBlockWithFieldsWithReqID(
	conn unsafe.Pointer,
	numOfRows int,
	pData unsafe.Pointer,
	tableName string,
	fields unsafe.Pointer,
	numFields int,
	reqID int64,
	logger *logrus.Entry,
	isDebug bool,
) int {
	logger.Tracef(
		"call taos_write_raw_block_with_fields_with_reqid, conn:%p, numOfRows:%d, pData:%p, tableName:%s, fields:%p, numFields:%d, reqID:%d",
		conn,
		numOfRows,
		pData,
		tableName,
		fields,
		numFields,
		reqID,
	)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_write_raw_block_with_fields_with_reqid cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code := wrapper.TaosWriteRawBlockWithFieldsWithReqID(conn, numOfRows, pData, tableName, fields, numFields, reqID)
	logger.Debugf("taos_write_raw_block_with_fields_with_reqid finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return code
}

func TaosGetCurrentDB(conn unsafe.Pointer, logger *logrus.Entry, isDebug bool) (string, error) {
	logger.Tracef("call taos_get_current_db, conn:%p", conn)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_get_current_db cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	db, err := wrapper.TaosGetCurrentDB(conn)
	logger.Debugf("taos_get_current_db finish, db:%s, err:%v, cost:%s", db, err, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return db, err
}

func TaosGetServerInfo(conn unsafe.Pointer, logger *logrus.Entry, isDebug bool) string {
	logger.Tracef("call taos_get_server_info, conn:%p", conn)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_get_server_info cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	info := wrapper.TaosGetServerInfo(conn)
	logger.Debugf("taos_get_server_info finish, info:%s, cost:%s", info, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return info
}

func TaosStmtNumParams(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) (int, int) {
	logger.Tracef("call taos_stmt_num_params, stmt:%p", stmt)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_stmt_num_params cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	num, errCode := wrapper.TaosStmtNumParams(stmt)
	logger.Debugf("taos_stmt_num_params finish, num:%d, code:%d, cost:%s", num, errCode, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return num, errCode
}

func TaosStmtGetParam(stmt unsafe.Pointer, index int, logger *logrus.Entry, isDebug bool) (int, int, error) {
	logger.Tracef("call taos_stmt_get_param, stmt:%p,  index:%d", stmt, index)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_stmt_get_param cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	dataType, dataLength, err := wrapper.TaosStmtGetParam(stmt, index)
	logger.Debugf("taos_stmt_get_param finish, type:%d, len:%d, err:%v, cost:%s", dataType, dataLength, err, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return dataType, dataLength, err
}

func TaosSchemalessInsertRawTTLWithReqIDTBNameKey(conn unsafe.Pointer, lines string, protocol int, precision string, ttl int, reqID int64, tbNameKey string, logger *logrus.Entry, isDebug bool) (int32, unsafe.Pointer) {
	logger.Tracef("call taos_schemaless_insert_raw_ttl_with_reqid_tbname_key, conn:%p, lines:%s, protocol:%d, precision:%s, ttl:%d, reqID:%d, tbnameKey:%s", conn, lines, protocol, precision, ttl, reqID, tbNameKey)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_schemaless_insert_raw_ttl_with_reqid_tbname_key cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	rows, result := wrapper.TaosSchemalessInsertRawTTLWithReqIDTBNameKey(conn, lines, protocol, precision, ttl, reqID, tbNameKey)
	logger.Debugf("taos_schemaless_insert_raw_ttl_with_reqid_tbname_key finish, rows:%d, result:%p, cost:%s", rows, result, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return rows, result
}

func TaosStmt2Init(taosConnect unsafe.Pointer, reqID int64, singleStbInsert bool, singleTableBindOnce bool, handle cgo.Handle, logger *logrus.Entry, isDebug bool) unsafe.Pointer {
	logger.Tracef("call taos_stmt2_init, taosConnect:%p, reqID:%d, singleStbInsert:%t, singleTableBindOnce:%t, handle:%p", taosConnect, reqID, singleStbInsert, singleTableBindOnce, handle.Pointer())
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_stmt2_init cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	stmt2 := wrapper.TaosStmt2Init(taosConnect, reqID, singleStbInsert, singleTableBindOnce, handle)
	logger.Debugf("taos_stmt2_init finish, stmt2:%p, cost:%s", stmt2, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return stmt2
}

func TaosStmt2Prepare(stmt2 unsafe.Pointer, sql string, logger *logrus.Entry, isDebug bool) int {
	logger.Tracef("call taos_stmt2_prepare, stmt2:%p, sql:%s", stmt2, log.GetLogSql(sql))
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_stmt2_prepare cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code := wrapper.TaosStmt2Prepare(stmt2, sql)
	logger.Debugf("taos_stmt2_prepare finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return code
}

func TaosStmt2IsInsert(stmt2 unsafe.Pointer, logger *logrus.Entry, isDebug bool) (bool, int) {
	logger.Tracef("call taos_stmt2_is_insert, stmt2:%p", stmt2)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_stmt2_is_insert cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	isInsert, code := wrapper.TaosStmt2IsInsert(stmt2)
	logger.Debugf("taos_stmt2_is_insert finish, isInsert:%t, code:%d, cost:%s", isInsert, code, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return isInsert, code
}

func TaosStmt2GetFields(stmt2 unsafe.Pointer, logger *logrus.Entry, isDebug bool) (code, count int, fields unsafe.Pointer) {
	logger.Tracef("call taos_stmt2_get_fields, stmt2:%p", stmt2)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_stmt2_get_fields cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code, count, fields = wrapper.TaosStmt2GetFields(stmt2)
	logger.Debugf("taos_stmt2_get_fields finish, code:%d, count:%d, fields:%p, cost:%s", code, count, fields, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return code, count, fields
}

func TaosStmt2Exec(stmt2 unsafe.Pointer, logger *logrus.Entry, isDebug bool) int {
	logger.Tracef("call taos_stmt2_exec, stmt2:%p", stmt2)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_stmt2_exec cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code := wrapper.TaosStmt2Exec(stmt2)
	logger.Debugf("taos_stmt2_exec finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return code
}

func TaosStmt2Close(stmt2 unsafe.Pointer, logger *logrus.Entry, isDebug bool) int {
	logger.Tracef("call taos_stmt2_close, stmt2:%p", stmt2)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_stmt2_close cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code := wrapper.TaosStmt2Close(stmt2)
	logger.Debugf("taos_stmt2_close finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return code
}

func TaosStmt2BindBinary(stmt2 unsafe.Pointer, data []byte, colIdx int32, logger *logrus.Entry, isDebug bool) error {
	logger.Tracef("call taos_stmt_bind_binary, stmt2:%p, colIdx:%d, data:%v", stmt2, colIdx, data)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_stmt_bind_binary cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	err := wrapper.TaosStmt2BindBinary(stmt2, data, colIdx)
	logger.Debugf("taos_stmt_bind_binary finish, err:%v, cost:%s", err, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return err
}

func TaosOptionsConnection(conn unsafe.Pointer, option int, value *string, logger *logrus.Entry, isDebug bool) int {
	if value == nil {
		logger.Tracef("call taos_options_connection, conn:%p, option:%d, value:<nil>", conn, option)
	} else {
		logger.Tracef("call taos_options_connection, conn:%p, option:%d, value:%s", conn, option, *value)
	}
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosOptionsConnection(conn, option, value)
	logger.Debugf("taos_options_connection finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	return code
}

func TaosValidateSql(taosConnect unsafe.Pointer, sql string, logger *logrus.Entry, isDebug bool) int {
	logger.Tracef("call taos_validate_sql, taosConnect:%p, sql:%s", taosConnect, sql)
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_validate_sql cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code := wrapper.TaosValidateSql(taosConnect, sql)
	logger.Debugf("taos_validate_sql finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	thread.SyncLocker.Unlock()
	return code
}

func TaosCheckServerStatus(fqdn *string, port int32, logger *logrus.Entry, isDebug bool) (int32, string) {
	if fqdn == nil {
		logger.Tracef("call taos_check_server_status, fqdn: nil, port:%d", port)
	} else {
		logger.Tracef("call taos_check_server_status, fqdn:%s, port:%d", *fqdn, port)
	}
	s := log.GetLogNow(isDebug)
	thread.SyncLocker.Lock()
	logger.Debugf("get thread lock for taos_check_server_status cost:%s", log.GetLogDuration(isDebug, s))
	status, details := wrapper.TaosCheckServerStatus(fqdn, port)
	logger.Debugf("taos_check_server_status finish, status:%d, detail:%s", status, details)
	thread.SyncLocker.Unlock()
	return status, details
}

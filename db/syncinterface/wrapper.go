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
	if res == nil {
		logger.Trace("result is nil")
		return
	}
	logger.Trace("sync semaphore acquire for taos_free_result")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_free_result")
	}()
	logger.Debugf("call taos_free_result, res:%p", res)
	s := log.GetLogNow(isDebug)
	wrapper.TaosFreeResult(res)
	logger.Debugf("taos_free_result finish, cost:%s", log.GetLogDuration(isDebug, s))
}

func TaosClose(conn unsafe.Pointer, logger *logrus.Entry, isDebug bool) {
	if conn == nil {
		logger.Trace("connection is nil")
		return
	}
	logger.Trace("sync semaphore acquire for taos_close")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_close")
	}()
	logger.Debugf("call taos_close, conn:%p", conn)
	s := log.GetLogNow(isDebug)
	wrapper.TaosClose(conn)
	logger.Debugf("taos_close finish, cost:%s", log.GetLogDuration(isDebug, s))
}

func TaosSelectDB(conn unsafe.Pointer, db string, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_select_db")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_select_db")
	}()
	logger.Debugf("call taos_select_db, conn:%p, db:%s", conn, db)
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosSelectDB(conn, db)
	logger.Debugf("taos_select_db finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	return code
}

func TaosConnect(host, user, pass, db string, port int, logger *logrus.Entry, isDebug bool) (unsafe.Pointer, error) {
	logger.Trace("sync semaphore acquire for taos_connect")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_connect")
	}()
	logger.Debugf("call taos_connect, host:%s, user:%s, pass:%s, db:%s, port:%d", host, user, pass, db, port)
	s := log.GetLogNow(isDebug)
	conn, err := wrapper.TaosConnect(host, user, pass, db, port)
	logger.Debugf("taos_connect finish, conn:%p, err:%v, cost:%s", conn, err, log.GetLogDuration(isDebug, s))
	return conn, err
}

func TaosGetTablesVgID(conn unsafe.Pointer, db string, tables []string, logger *logrus.Entry, isDebug bool) ([]int, int) {
	logger.Trace("sync semaphore acquire for taos_get_tables_vgId")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_get_tables_vgId")
	}()
	logger.Debugf("call taos_get_tables_vgId, conn:%p, db:%s, tables:%s", conn, db, strings.Join(tables, ", "))
	s := log.GetLogNow(isDebug)
	vgIDs, code := wrapper.TaosGetTablesVgID(conn, db, tables)
	logger.Debugf("taos_get_tables_vgId finish, vgid:%v, code:%d, cost:%s", vgIDs, code, log.GetLogDuration(isDebug, s))
	return vgIDs, code
}

func TaosStmtInitWithReqID(conn unsafe.Pointer, reqID int64, logger *logrus.Entry, isDebug bool) unsafe.Pointer {
	logger.Trace("sync semaphore acquire for taos_stmt_init_with_reqid")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_stmt_init_with_reqid")
	}()
	logger.Debugf("call taos_stmt_init_with_reqid, conn:%p, QID:0x%x", conn, reqID)
	s := log.GetLogNow(isDebug)
	stmtInit := wrapper.TaosStmtInitWithReqID(conn, reqID)
	logger.Debugf("taos_stmt_init_with_reqid finish, result:%p, cost:%s", stmtInit, log.GetLogDuration(isDebug, s))
	return stmtInit
}

func TaosStmtPrepare(stmt unsafe.Pointer, sql string, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_stmt_prepare")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_stmt_prepare")
	}()
	logger.Debugf("call taos_stmt_prepare, stmt:%p,  sql:%s", stmt, log.GetLogSql(sql))
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosStmtPrepare(stmt, sql)
	logger.Debugf("taos_stmt_prepare finish, code:%d cost:%s", code, log.GetLogDuration(isDebug, s))
	return code
}

func TaosStmtIsInsert(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) (bool, int) {
	logger.Trace("sync semaphore acquire for taos_stmt_is_insert")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_stmt_is_insert")
	}()
	logger.Debugf("call taos_stmt_is_insert, stmt:%p", stmt)
	s := log.GetLogNow(isDebug)
	isInsert, code := wrapper.TaosStmtIsInsert(stmt)
	logger.Debugf("taos_stmt_is_insert isInsert finish, insert:%t, code:%d, cost:%s", isInsert, code, log.GetLogDuration(isDebug, s))
	return isInsert, code
}

func TaosStmtSetTBName(stmt unsafe.Pointer, tbname string, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_stmt_set_tbname")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_stmt_set_tbname")
	}()
	logger.Debugf("call taos_stmt_set_tbname, stmt:%p, tbname:%s", stmt, tbname)
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosStmtSetTBName(stmt, tbname)
	logger.Debugf("taos_stmt_set_tbname finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	return code
}

func TaosStmtGetTagFields(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) (int, int, unsafe.Pointer) {
	logger.Trace("sync semaphore acquire for taos_stmt_get_tag_fields")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_stmt_get_tag_fields")
	}()
	logger.Debugf("call taos_stmt_get_tag_fields, stmt:%p", stmt)
	s := log.GetLogNow(isDebug)
	code, num, fields := wrapper.TaosStmtGetTagFields(stmt)
	logger.Debugf("taos_stmt_get_tag_fields finish, code:%d, num:%d, fields:%p, cost:%s", code, num, fields, log.GetLogDuration(isDebug, s))
	return code, num, fields
}

func TaosStmtSetTags(stmt unsafe.Pointer, tags []driver.Value, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_stmt_set_tags")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_stmt_set_tags")
	}()
	logger.Debugf("call taos_stmt_set_tags, stmt:%p, tags:%v", stmt, tags)
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosStmtSetTags(stmt, tags)
	logger.Debugf("taos_stmt_set_tags finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	return code
}

func TaosStmtGetColFields(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) (int, int, unsafe.Pointer) {
	logger.Trace("sync semaphore acquire for taos_stmt_get_col_fields")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_stmt_get_col_fields")
	}()
	logger.Debugf("call taos_stmt_get_col_fields, stmt:%p", stmt)
	s := log.GetLogNow(isDebug)
	code, num, fields := wrapper.TaosStmtGetColFields(stmt)
	logger.Debugf("taos_stmt_get_col_fields finish, code:%d, num:%d, fields:%p, cost:%s", code, num, fields, log.GetLogDuration(isDebug, s))
	return code, num, fields
}

func TaosStmtBindParamBatch(stmt unsafe.Pointer, multiBind [][]driver.Value, bindType []*types.ColumnType, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_stmt_bind_param_batch")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_stmt_bind_param_batch")
	}()
	logger.Debugf("call taos_stmt_bind_param_batch, stmt:%p, multiBind:%v, bindType:%v", stmt, multiBind, bindType)
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosStmtBindParamBatch(stmt, multiBind, bindType)
	logger.Debugf("taos_stmt_bind_param_batch finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	return code
}

func TaosStmtAddBatch(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_stmt_add_batch")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_stmt_add_batch")
	}()
	logger.Debugf("call taos_stmt_add_batch, stmt:%p", stmt)
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosStmtAddBatch(stmt)
	logger.Debugf("taos_stmt_add_batch finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	return code
}

func TaosStmtExecute(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_stmt_execute")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_stmt_execute")
	}()
	logger.Debugf("call taos_stmt_execute, stmt:%p", stmt)
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosStmtExecute(stmt)
	logger.Debugf("taos_stmt_execute finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	return code
}

func TaosStmtClose(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_stmt_close")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_stmt_close")
	}()
	logger.Debugf("call taos_stmt_close, stmt:%p", stmt)
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosStmtClose(stmt)
	logger.Debugf("taos_stmt_close finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	return code
}

func TMQWriteRaw(conn unsafe.Pointer, length uint32, metaType uint16, data unsafe.Pointer, logger *logrus.Entry, isDebug bool) int32 {
	logger.Trace("sync semaphore acquire for tmq_write_raw")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for tmq_write_raw")
	}()
	logger.Debugf("call tmq_write_raw, conn:%p, length:%d, metaType:%d, data:%p", conn, length, metaType, data)
	s := log.GetLogNow(isDebug)
	code := wrapper.TMQWriteRaw(conn, length, metaType, data)
	logger.Debugf("tmq_write_raw finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	return code
}

func TaosWriteRawBlockWithReqID(conn unsafe.Pointer, numOfRows int, pData unsafe.Pointer, tableName string, reqID int64, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_write_raw_block_with_reqid")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_write_raw_block_with_reqid")
	}()
	logger.Debugf("call taos_write_raw_block_with_reqid, conn:%p, numOfRows:%d, pData:%p, tableName:%s, reqID:%d", conn, numOfRows, pData, tableName, reqID)
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosWriteRawBlockWithReqID(conn, numOfRows, pData, tableName, reqID)
	logger.Debugf("taos_write_raw_block_with_reqid finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
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
	logger.Trace("sync semaphore acquire for taos_write_raw_block_with_fields_with_reqid")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_write_raw_block_with_fields_with_reqid")
	}()
	logger.Debugf(
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
	code := wrapper.TaosWriteRawBlockWithFieldsWithReqID(conn, numOfRows, pData, tableName, fields, numFields, reqID)
	logger.Debugf("taos_write_raw_block_with_fields_with_reqid finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	return code
}

func TaosGetCurrentDB(conn unsafe.Pointer, logger *logrus.Entry, isDebug bool) (string, error) {
	logger.Trace("sync semaphore acquire for taos_get_current_db")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_get_current_db")
	}()
	logger.Debugf("call taos_get_current_db, conn:%p", conn)
	s := log.GetLogNow(isDebug)
	db, err := wrapper.TaosGetCurrentDB(conn)
	logger.Debugf("taos_get_current_db finish, db:%s, err:%v, cost:%s", db, err, log.GetLogDuration(isDebug, s))
	return db, err
}

func TaosGetServerInfo(conn unsafe.Pointer, logger *logrus.Entry, isDebug bool) string {
	logger.Trace("sync semaphore acquire for taos_get_server_info")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_get_server_info")
	}()
	logger.Debugf("call taos_get_server_info, conn:%p", conn)
	s := log.GetLogNow(isDebug)
	info := wrapper.TaosGetServerInfo(conn)
	logger.Debugf("taos_get_server_info finish, info:%s, cost:%s", info, log.GetLogDuration(isDebug, s))
	return info
}

func TaosStmtNumParams(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) (int, int) {
	logger.Trace("sync semaphore acquire for taos_stmt_num_params")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_stmt_num_params")
	}()
	logger.Debugf("call taos_stmt_num_params, stmt:%p", stmt)
	s := log.GetLogNow(isDebug)
	num, errCode := wrapper.TaosStmtNumParams(stmt)
	logger.Debugf("taos_stmt_num_params finish, num:%d, code:%d, cost:%s", num, errCode, log.GetLogDuration(isDebug, s))
	return num, errCode
}

func TaosStmtGetParam(stmt unsafe.Pointer, index int, logger *logrus.Entry, isDebug bool) (int, int, error) {
	logger.Trace("sync semaphore acquire for taos_stmt_get_param")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_stmt_get_param")
	}()
	logger.Debugf("call taos_stmt_get_param, stmt:%p, index:%d", stmt, index)
	s := log.GetLogNow(isDebug)
	dataType, dataLength, err := wrapper.TaosStmtGetParam(stmt, index)
	logger.Debugf("taos_stmt_get_param finish, type:%d, len:%d, err:%v, cost:%s", dataType, dataLength, err, log.GetLogDuration(isDebug, s))
	return dataType, dataLength, err
}

func TaosSchemalessInsertRawTTLWithReqIDTBNameKey(conn unsafe.Pointer, lines string, protocol int, precision string, ttl int, reqID int64, tbNameKey string, logger *logrus.Entry, isDebug bool) (int32, unsafe.Pointer) {
	logger.Trace("sync semaphore acquire for taos_schemaless_insert_raw_ttl_with_reqid_tbname_key")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_schemaless_insert_raw_ttl_with_reqid_tbname_key")
	}()
	logger.Debugf("call taos_schemaless_insert_raw_ttl_with_reqid_tbname_key, conn:%p, lines:%s, protocol:%d, precision:%s, ttl:%d, reqID:%d, tbnameKey:%s", conn, lines, protocol, precision, ttl, reqID, tbNameKey)
	s := log.GetLogNow(isDebug)
	rows, result := wrapper.TaosSchemalessInsertRawTTLWithReqIDTBNameKey(conn, lines, protocol, precision, ttl, reqID, tbNameKey)
	logger.Debugf("taos_schemaless_insert_raw_ttl_with_reqid_tbname_key finish, rows:%d, result:%p, cost:%s", rows, result, log.GetLogDuration(isDebug, s))
	return rows, result
}

func TaosStmt2Init(taosConnect unsafe.Pointer, reqID int64, singleStbInsert bool, singleTableBindOnce bool, handle cgo.Handle, logger *logrus.Entry, isDebug bool) unsafe.Pointer {
	logger.Trace("sync semaphore acquire for taos_stmt2_init")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_stmt2_init")
	}()
	logger.Debugf("call taos_stmt2_init, taosConnect:%p, reqID:%d, singleStbInsert:%t, singleTableBindOnce:%t, handle:%p", taosConnect, reqID, singleStbInsert, singleTableBindOnce, handle.Pointer())
	s := log.GetLogNow(isDebug)
	stmt2 := wrapper.TaosStmt2Init(taosConnect, reqID, singleStbInsert, singleTableBindOnce, handle)
	logger.Debugf("taos_stmt2_init finish, stmt2:%p, cost:%s", stmt2, log.GetLogDuration(isDebug, s))
	return stmt2
}

func TaosStmt2Prepare(stmt2 unsafe.Pointer, sql string, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_stmt2_prepare")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_stmt2_prepare")
	}()
	logger.Debugf("call taos_stmt2_prepare, stmt2:%p, sql:%s", stmt2, log.GetLogSql(sql))
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosStmt2Prepare(stmt2, sql)
	logger.Debugf("taos_stmt2_prepare finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	return code
}

func TaosStmt2IsInsert(stmt2 unsafe.Pointer, logger *logrus.Entry, isDebug bool) (bool, int) {
	logger.Trace("sync semaphore acquire for taos_stmt2_is_insert")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_stmt2_is_insert")
	}()
	logger.Debugf("call taos_stmt2_is_insert, stmt2:%p", stmt2)
	s := log.GetLogNow(isDebug)
	isInsert, code := wrapper.TaosStmt2IsInsert(stmt2)
	logger.Debugf("taos_stmt2_is_insert finish, isInsert:%t, code:%d, cost:%s", isInsert, code, log.GetLogDuration(isDebug, s))
	return isInsert, code
}

func TaosStmt2GetFields(stmt2 unsafe.Pointer, logger *logrus.Entry, isDebug bool) (code, count int, fields unsafe.Pointer) {
	logger.Trace("sync semaphore acquire for taos_stmt2_get_fields")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_stmt2_get_fields")
	}()
	logger.Debugf("call taos_stmt2_get_fields, stmt2:%p", stmt2)
	s := log.GetLogNow(isDebug)
	code, count, fields = wrapper.TaosStmt2GetFields(stmt2)
	logger.Debugf("taos_stmt2_get_fields finish, code:%d, count:%d, fields:%p, cost:%s", code, count, fields, log.GetLogDuration(isDebug, s))
	return code, count, fields
}

func TaosStmt2Exec(stmt2 unsafe.Pointer, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_stmt2_exec")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_stmt2_exec")
	}()
	logger.Debugf("call taos_stmt2_exec, stmt2:%p", stmt2)
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosStmt2Exec(stmt2)
	logger.Debugf("taos_stmt2_exec finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	return code
}

func TaosStmt2Close(stmt2 unsafe.Pointer, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_stmt2_close")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_stmt2_close")
	}()
	logger.Debugf("call taos_stmt2_close, stmt2:%p", stmt2)
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosStmt2Close(stmt2)
	logger.Debugf("taos_stmt2_close finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	return code
}

func TaosStmt2BindBinary(stmt2 unsafe.Pointer, data []byte, colIdx int32, logger *logrus.Entry, isDebug bool) error {
	logger.Trace("sync semaphore acquire for taos_stmt2_bind_binary")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_stmt2_bind_binary")
	}()
	logger.Debugf("call taos_stmt2_bind_binary, stmt2:%p, colIdx:%d, data:%v", stmt2, colIdx, data)
	s := log.GetLogNow(isDebug)
	err := wrapper.TaosStmt2BindBinary(stmt2, data, colIdx)
	logger.Debugf("taos_stmt2_bind_binary finish, err:%v, cost:%s", err, log.GetLogDuration(isDebug, s))
	return err
}

func TaosOptionsConnection(conn unsafe.Pointer, option int, value *string, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_options_connection")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_options_connection")
	}()
	if value == nil {
		logger.Debugf("call taos_options_connection, conn:%p, option:%d, value:<nil>", conn, option)
	} else {
		logger.Debugf("call taos_options_connection, conn:%p, option:%d, value:%s", conn, option, *value)
	}
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosOptionsConnection(conn, option, value)
	logger.Debugf("taos_options_connection finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	return code
}

func TaosValidateSql(taosConnect unsafe.Pointer, sql string, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_validate_sql")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_validate_sql")
	}()
	logger.Debugf("call taos_validate_sql, taosConnect:%p, sql:%s", taosConnect, sql)
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosValidateSql(taosConnect, sql)
	logger.Debugf("taos_validate_sql finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	return code
}

func TaosCheckServerStatus(fqdn *string, port int32, logger *logrus.Entry, isDebug bool) (int32, string) {
	logger.Trace("sync semaphore acquire for taos_check_server_status")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore delete for taos_check_server_status")
	}()
	if fqdn == nil {
		logger.Debugf("call taos_check_server_status, fqdn: nil, port:%d", port)
	} else {
		logger.Debugf("call taos_check_server_status, fqdn:%s, port:%d", *fqdn, port)
	}
	s := log.GetLogNow(isDebug)
	status, details := wrapper.TaosCheckServerStatus(fqdn, port)
	logger.Debugf("taos_check_server_status finish, status:%d, detail:%s, cost:%s", status, details, log.GetLogDuration(isDebug, s))
	return status, details
}

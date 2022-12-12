package rest

import (
	"container/list"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/huskar-t/melody"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v3/common/parser"
	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/types"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/thread"
	"github.com/taosdata/taosadapter/v3/tools/web"
)

type TaosStmt struct {
	conn            unsafe.Pointer
	stmtIndexLocker sync.RWMutex
	StmtList        *list.List
	stmtIndex       uint64
	closed          bool
	sync.Mutex
}

type StmtItem struct {
	index uint64
	stmt  unsafe.Pointer
	sync.Mutex
}

func (s *StmtItem) clean() {
	s.Lock()
	if s.stmt != nil {
		thread.Lock()
		wrapper.TaosStmtClose(s.stmt)
		thread.Unlock()
	}
	s.Unlock()
}

func NewTaosStmt() *TaosStmt {
	return &TaosStmt{StmtList: list.New()}
}

func (t *TaosStmt) addStmtItem(stmt *StmtItem) {
	index := atomic.AddUint64(&t.stmtIndex, 1)
	stmt.index = index
	t.stmtIndexLocker.Lock()
	t.StmtList.PushBack(stmt)
	t.stmtIndexLocker.Unlock()
}

func (t *TaosStmt) getStmtItem(index uint64) *list.Element {
	t.stmtIndexLocker.RLock()
	defer t.stmtIndexLocker.RUnlock()
	root := t.StmtList.Front()
	if root == nil {
		return nil
	}
	rootIndex := root.Value.(*StmtItem).index
	if rootIndex == index {
		return root
	}
	item := root.Next()
	for {
		if item == nil || item == root {
			return nil
		}
		if item.Value.(*StmtItem).index == index {
			return item
		}
		item = item.Next()
	}
}

func (t *TaosStmt) removeStmtItem(item *list.Element) {
	t.stmtIndexLocker.Lock()
	t.StmtList.Remove(item)
	t.stmtIndexLocker.Unlock()
}

type StmtConnectReq struct {
	ReqID    uint64 `json:"req_id"`
	User     string `json:"user"`
	Password string `json:"password"`
	DB       string `json:"db"`
}

type StmtConnectResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func (t *TaosStmt) connect(ctx context.Context, session *melody.Session, req *StmtConnectReq) {
	logger := getLogger(session).WithField("action", STMTConnect)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.Lock()
	logger.Debugln("get global lock cost:", log.GetLogDuration(isDebug, s))
	defer t.Unlock()
	if t.conn != nil {
		wsStmtErrorMsg(ctx, session, 0xffff, "taos duplicate connections", STMTConnect, req.ReqID, nil)
		return
	}
	s = log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	conn, err := wrapper.TaosConnect("", req.User, req.Password, req.DB, 0)
	logger.Debugln("taos connect cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if err != nil {
		wsStmtError(ctx, session, err, STMTConnect, req.ReqID, nil)
		return
	}
	t.conn = conn
	wsWriteJson(session, &StmtConnectResp{
		Action: STMTConnect,
		ReqID:  req.ReqID,
		Timing: getDuration(ctx),
	})
}

type StmtInitReq struct {
	ReqID uint64 `json:"req_id"`
}
type StmtInitResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (t *TaosStmt) init(ctx context.Context, session *melody.Session, req *StmtInitReq) {
	if t.conn == nil {
		wsStmtErrorMsg(ctx, session, 0xffff, "taos not connected", STMTInit, req.ReqID, nil)
		return
	}
	logger := getLogger(session).WithField("action", STMTInit)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	stmt := wrapper.TaosStmtInit(t.conn)
	logger.Debugln("taos_stmt_init cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if stmt == nil {
		errStr := wrapper.TaosStmtErrStr(stmt)
		wsStmtErrorMsg(ctx, session, 0xffff, errStr, STMTInit, req.ReqID, nil)
		return
	}
	stmtItem := &StmtItem{
		stmt: stmt,
	}
	t.addStmtItem(stmtItem)
	resp := &StmtInitResp{Action: STMTInit, ReqID: req.ReqID, StmtID: stmtItem.index, Timing: getDuration(ctx)}
	wsWriteJson(session, resp)
}

type StmtPrepareReq struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
	SQL    string `json:"sql"`
}
type StmtPrepareResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (t *TaosStmt) prepare(ctx context.Context, session *melody.Session, req *StmtPrepareReq) {
	if t.conn == nil {
		wsStmtErrorMsg(ctx, session, 0xffff, "taos not connected", STMTPrepare, req.ReqID, &req.StmtID)
		return
	}

	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		wsStmtErrorMsg(ctx, session, 0xffff, "stmt is nil", STMTPrepare, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	logger := getLogger(session).WithField("action", STMTPrepare)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code := wrapper.TaosStmtPrepare(stmt.stmt, req.SQL)
	logger.Debugln("taos_stmt_prepare cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		wsStmtErrorMsg(ctx, session, code, errStr, STMTPrepare, req.ReqID, &req.StmtID)
		return
	}
	resp := &StmtPrepareResp{
		Action: STMTPrepare,
		ReqID:  req.ReqID,
		StmtID: req.StmtID,
		Timing: getDuration(ctx),
	}
	wsWriteJson(session, resp)
}

type StmtSetTableNameReq struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
	Name   string `json:"name"`
}
type StmtSetTableNameResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (t *TaosStmt) setTableName(ctx context.Context, session *melody.Session, req *StmtSetTableNameReq) {
	if t.conn == nil {
		wsStmtErrorMsg(ctx, session, 0xffff, "taos not connected", STMTSetTableName, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		wsStmtErrorMsg(ctx, session, 0xffff, "stmt is nil", STMTSetTableName, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	logger := getLogger(session).WithField("action", STMTSetTableName)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code := wrapper.TaosStmtSetTBName(stmt.stmt, req.Name)
	logger.Debugln("taos_stmt_set_tbname cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		wsStmtErrorMsg(ctx, session, code, errStr, STMTSetTableName, req.ReqID, &req.StmtID)
		return
	}
	resp := &StmtSetTableNameResp{
		Action: STMTSetTableName,
		ReqID:  req.ReqID,
		StmtID: req.StmtID,
		Timing: getDuration(ctx),
	}
	wsWriteJson(session, resp)
}

type StmtSetTagsReq struct {
	ReqID  uint64          `json:"req_id"`
	StmtID uint64          `json:"stmt_id"`
	Tags   json.RawMessage `json:"tags"`
}

type StmtSetTagsResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (t *TaosStmt) setTags(ctx context.Context, session *melody.Session, req *StmtSetTagsReq) {
	if t.conn == nil {
		wsStmtErrorMsg(ctx, session, 0xffff, "taos not connected", STMTSetTags, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		wsStmtErrorMsg(ctx, session, 0xffff, "stmt is nil", STMTSetTags, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	logger := getLogger(session).WithField("action", STMTSetTags)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("taos_stmt_get_tag_fields get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code, tagNums, tagFields := wrapper.TaosStmtGetTagFields(stmt.stmt)
	logger.Debugln("taos_stmt_get_tag_fields cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		wsStmtErrorMsg(ctx, session, code, errStr, STMTSetTags, req.ReqID, &req.StmtID)
		return
	}
	resp := &StmtSetTagsResp{
		Action: STMTSetTags,
		ReqID:  req.ReqID,
		StmtID: req.StmtID,
	}
	if tagNums == 0 {
		wsWriteJson(session, resp)
		return
	}
	s = log.GetLogNow(isDebug)
	fields := wrapper.StmtParseFields(tagNums, tagFields)
	logger.Debugln("stmt parse fields cost:", log.GetLogDuration(isDebug, s))
	tags := make([][]driver.Value, tagNums)
	for i := 0; i < tagNums; i++ {
		tags[i] = []driver.Value{req.Tags[i]}
	}
	s = log.GetLogNow(isDebug)
	data, err := stmtParseTag(req.Tags, fields)
	logger.Debugln("stmt parse tag json cost:", log.GetLogDuration(isDebug, s))
	if err != nil {
		wsStmtErrorMsg(ctx, session, 0xffff, fmt.Sprintf("stmt parse tag json:%s", err.Error()), STMTSetTags, req.ReqID, &req.StmtID)
		return
	}
	s = log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("taos_stmt_set_tags get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code = wrapper.TaosStmtSetTags(stmt.stmt, data)
	logger.Debugln("taos_stmt_set_tags cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		wsStmtErrorMsg(ctx, session, code, errStr, STMTSetTags, req.ReqID, &req.StmtID)
		return
	}
	resp.Timing = getDuration(ctx)
	wsWriteJson(session, resp)
}

type StmtBindReq struct {
	ReqID   uint64          `json:"req_id"`
	StmtID  uint64          `json:"stmt_id"`
	Columns json.RawMessage `json:"columns"`
}
type StmtBindResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (t *TaosStmt) bind(ctx context.Context, session *melody.Session, req *StmtBindReq) {
	if t.conn == nil {
		wsStmtErrorMsg(ctx, session, 0xffff, "taos not connected", STMTBind, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		wsStmtErrorMsg(ctx, session, 0xffff, "stmt is nil", STMTBind, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	logger := getLogger(session).WithField("action", STMTBind)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("taos_stmt_get_col_fields get thread lock cost:", log.GetLogDuration(isDebug, s))
	code, colNums, colFields := wrapper.TaosStmtGetColFields(stmt.stmt)
	logger.Debugln("taos_stmt_get_col_fields cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		wsStmtErrorMsg(ctx, session, code, errStr, STMTBind, req.ReqID, &req.StmtID)
		return
	}
	resp := &StmtBindResp{
		Action: STMTBind,
		ReqID:  req.ReqID,
		StmtID: req.StmtID,
	}
	if colNums == 0 {
		resp.Timing = getDuration(ctx)
		wsWriteJson(session, resp)
		return
	}
	s = log.GetLogNow(isDebug)
	fields := wrapper.StmtParseFields(colNums, colFields)
	logger.Debugln("stmt parse fields cost:", log.GetLogDuration(isDebug, s))
	fieldTypes := make([]*types.ColumnType, colNums)
	var err error

	for i := 0; i < colNums; i++ {
		fieldTypes[i], err = fields[i].GetType()
		if err != nil {
			wsStmtErrorMsg(ctx, session, 0xffff, fmt.Sprintf("stmt get column type error:%s", err.Error()), STMTBind, req.ReqID, &req.StmtID)
			return
		}
	}
	s = log.GetLogNow(isDebug)
	data, err := stmtParseColumn(req.Columns, fields, fieldTypes)
	logger.Debugln("stmt parse column json cost:", log.GetLogDuration(isDebug, s))
	if err != nil {
		wsStmtErrorMsg(ctx, session, 0xffff, fmt.Sprintf("stmt parse column json:%s", err.Error()), STMTBind, req.ReqID, &req.StmtID)
		return
	}
	s = log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("taos_stmt_bind_param_batch get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	wrapper.TaosStmtBindParamBatch(stmt.stmt, data, fieldTypes)
	logger.Debugln("taos_stmt_bind_param_batch cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	resp.Timing = getDuration(ctx)
	wsWriteJson(session, resp)
}

type StmtAddBatchReq struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}
type StmtAddBatchResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (t *TaosStmt) addBatch(ctx context.Context, session *melody.Session, req *StmtAddBatchReq) {
	if t.conn == nil {
		wsStmtErrorMsg(ctx, session, 0xffff, "taos not connected", STMTAddBatch, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		wsStmtErrorMsg(ctx, session, 0xffff, "stmt is nil", STMTAddBatch, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	logger := getLogger(session).WithField("action", STMTAddBatch)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code := wrapper.TaosStmtAddBatch(stmt.stmt)
	logger.Debugln("taos_stmt_add_batch cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		wsStmtErrorMsg(ctx, session, code, errStr, STMTAddBatch, req.ReqID, &req.StmtID)
		return
	}
	resp := &StmtAddBatchResp{
		Action: STMTAddBatch,
		ReqID:  req.ReqID,
		StmtID: req.StmtID,
		Timing: getDuration(ctx),
	}
	wsWriteJson(session, resp)
}

type StmtExecReq struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}
type StmtExecResp struct {
	Code     int    `json:"code"`
	Message  string `json:"message"`
	Action   string `json:"action"`
	ReqID    uint64 `json:"req_id"`
	Timing   int64  `json:"timing"`
	StmtID   uint64 `json:"stmt_id"`
	Affected int    `json:"affected"`
}

func (t *TaosStmt) exec(ctx context.Context, session *melody.Session, req *StmtExecReq) {
	if t.conn == nil {
		wsStmtErrorMsg(ctx, session, 0xffff, "taos not connected", STMTExec, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		wsStmtErrorMsg(ctx, session, 0xffff, "stmt is nil", STMTExec, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	logger := getLogger(session).WithField("action", STMTExec)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("taos_stmt_execute get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code := wrapper.TaosStmtExecute(stmt.stmt)
	logger.Debugln("taos_stmt_execute cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		wsStmtErrorMsg(ctx, session, code, errStr, STMTExec, req.ReqID, &req.StmtID)
		return
	}
	s = log.GetLogNow(isDebug)
	affected := wrapper.TaosStmtAffectedRowsOnce(stmt.stmt)
	logger.Debugln("taos_stmt_affected_rows_once cost:", log.GetLogDuration(isDebug, s))
	resp := &StmtExecResp{
		Action:   STMTExec,
		ReqID:    req.ReqID,
		StmtID:   req.StmtID,
		Timing:   getDuration(ctx),
		Affected: affected,
	}
	wsWriteJson(session, resp)
}

type StmtClose struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

func (t *TaosStmt) close(ctx context.Context, session *melody.Session, req *StmtClose) {
	if t.conn == nil {
		wsStmtErrorMsg(ctx, session, 0xffff, "taos not connected", STMTClose, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		wsStmtErrorMsg(ctx, session, 0xffff, "stmt is nil", STMTClose, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	t.removeStmtItem(stmtItem)
	stmt.clean()
}

func (t *TaosStmt) setTagsBlock(ctx context.Context, session *melody.Session, reqID, stmtID uint64, rows, columns int, block unsafe.Pointer) {
	if rows != 1 {
		wsStmtErrorMsg(ctx, session, 0xffff, "rows not equal 1", STMTSetTags, reqID, &stmtID)
		return
	}
	if t.conn == nil {
		wsStmtErrorMsg(ctx, session, 0xffff, "taos not connected", STMTSetTags, reqID, &stmtID)
		return
	}
	stmtItem := t.getStmtItem(stmtID)
	if stmtItem == nil {
		wsStmtErrorMsg(ctx, session, 0xffff, "stmt is nil", STMTSetTags, reqID, &stmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	logger := getLogger(session).WithField("action", STMTSetTags)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("taos_stmt_get_tag_fields get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code, tagNums, tagFields := wrapper.TaosStmtGetTagFields(stmt.stmt)
	logger.Debugln("taos_stmt_get_tag_fields cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		wsStmtErrorMsg(ctx, session, code, errStr, STMTSetTags, reqID, &stmtID)
		return
	}
	resp := &StmtSetTagsResp{
		Action: STMTSetTags,
		ReqID:  reqID,
		StmtID: stmtID,
	}
	if tagNums == 0 {
		wsWriteJson(session, resp)
		return
	}
	if columns != tagNums {
		wsStmtErrorMsg(ctx, session, 0xffff, "stmt tags count not match", STMTSetTags, reqID, &stmtID)
		return
	}
	s = log.GetLogNow(isDebug)
	fields := wrapper.StmtParseFields(tagNums, tagFields)
	logger.Debugln("stmt parse fields cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	tags := blockConvert(block, int(rows), fields)
	logger.Debugln("block concert cost:", log.GetLogDuration(isDebug, s))
	reTags := make([]driver.Value, tagNums)
	for i := 0; i < tagNums; i++ {
		reTags[i] = tags[i][0]
	}
	s = log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("taos_stmt_set_tags get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code = wrapper.TaosStmtSetTags(stmt.stmt, reTags)
	logger.Debugln("taos_stmt_set_tags cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		wsStmtErrorMsg(ctx, session, code, errStr, STMTSetTags, reqID, &stmtID)
		return
	}
	resp.Timing = getDuration(ctx)
	wsWriteJson(session, resp)
}

func (t *TaosStmt) bindBlock(ctx context.Context, session *melody.Session, reqID, stmtID uint64, rows, columns int, block unsafe.Pointer) {
	if t.conn == nil {
		wsStmtErrorMsg(ctx, session, 0xffff, "taos not connected", STMTBind, reqID, &stmtID)
		return
	}
	stmtItem := t.getStmtItem(stmtID)
	if stmtItem == nil {
		wsStmtErrorMsg(ctx, session, 0xffff, "stmt is nil", STMTBind, reqID, &stmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	logger := getLogger(session).WithField("action", STMTBind)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("taos_stmt_get_col_fields get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	code, colNums, colFields := wrapper.TaosStmtGetColFields(stmt.stmt)
	logger.Debugln("taos_stmt_get_col_fields cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		wsStmtErrorMsg(ctx, session, code, errStr, STMTBind, reqID, &stmtID)
		return
	}
	resp := &StmtBindResp{
		Action: STMTBind,
		ReqID:  reqID,
		StmtID: stmtID,
	}
	if colNums == 0 {
		resp.Timing = getDuration(ctx)
		wsWriteJson(session, resp)
		return
	}
	s = log.GetLogNow(isDebug)
	fields := wrapper.StmtParseFields(colNums, colFields)
	logger.Debugln("stmt parse fields cost:", log.GetLogDuration(isDebug, s))
	fieldTypes := make([]*types.ColumnType, colNums)
	var err error
	for i := 0; i < colNums; i++ {
		fieldTypes[i], err = fields[i].GetType()
		if err != nil {
			wsStmtErrorMsg(ctx, session, 0xffff, fmt.Sprintf("stmt get column type error:%s", err.Error()), STMTBind, reqID, &stmtID)
			return
		}
	}
	if columns != colNums {
		wsStmtErrorMsg(ctx, session, 0xffff, "stmt column count not match", STMTBind, reqID, &stmtID)
		return
	}
	s = log.GetLogNow(isDebug)
	data := blockConvert(block, rows, fields)
	logger.Debugln("block convert cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("taos_stmt_bind_param_batch get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	wrapper.TaosStmtBindParamBatch(stmt.stmt, data, fieldTypes)
	logger.Debugln("taos_stmt_bind_param_batch cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	resp.Timing = getDuration(ctx)
	wsWriteJson(session, resp)
}

func (t *TaosStmt) Close() {
	t.Lock()
	defer t.Unlock()
	if t.closed {
		return
	}
	t.closed = true
	t.cleanUp()
	if t.conn != nil {
		thread.Lock()
		wrapper.TaosClose(t.conn)
		thread.Unlock()
		t.conn = nil
	}
}

func (t *TaosStmt) cleanUp() {
	t.stmtIndexLocker.Lock()
	defer t.stmtIndexLocker.Unlock()
	root := t.StmtList.Front()
	if root == nil {
		return
	}
	root.Value.(*StmtItem).clean()
	item := root.Next()
	for {
		if item == nil || item == root {
			return
		}
		item.Value.(*StmtItem).clean()
		item = item.Next()
	}
}

func (ctl *Restful) InitStmt() {
	ctl.stmtM = melody.New()
	ctl.stmtM.Config.MaxMessageSize = 4 * 1024 * 1024

	ctl.stmtM.HandleConnect(func(session *melody.Session) {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws connect")
		session.Set(TaosStmtKey, NewTaosStmt())
	})

	ctl.stmtM.HandleMessage(func(session *melody.Session, data []byte) {
		if ctl.stmtM.IsClosed() {
			return
		}
		ctx := context.WithValue(context.Background(), StartTimeKey, time.Now().UnixNano())
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("get ws message data:", string(data))
		var action WSAction
		err := json.Unmarshal(data, &action)
		if err != nil {
			logger.WithError(err).Errorln("unmarshal ws request")
			return
		}
		switch action.Action {
		case ClientVersion:
			session.Write(ctl.wsVersionResp)
		case STMTConnect:
			var req StmtConnectReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal connect request args")
				return
			}
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).connect(ctx, session, &req)
		case STMTInit:
			var req StmtInitReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal init args")
				return
			}
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).init(ctx, session, &req)
		case STMTPrepare:
			var req StmtPrepareReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal prepare args")
				return
			}
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).prepare(ctx, session, &req)
		case STMTSetTableName:
			var req StmtSetTableNameReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal set table name args")
				return
			}
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).setTableName(ctx, session, &req)
		case STMTSetTags:
			var req StmtSetTagsReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal set tags args")
				return
			}
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).setTags(ctx, session, &req)
		case STMTBind:
			var req StmtBindReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal bind args")
				return
			}
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).bind(ctx, session, &req)
		case STMTAddBatch:
			var req StmtAddBatchReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal add batch args")
				return
			}
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).addBatch(ctx, session, &req)
		case STMTExec:
			var req StmtExecReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal exec args")
				return
			}
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).exec(ctx, session, &req)
		case STMTClose:
			var req StmtClose
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal close args")
				return
			}
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).close(ctx, session, &req)
		default:
			logger.WithError(err).Errorln("unknown action: " + action.Action)
			return
		}
	})

	ctl.stmtM.HandleMessageBinary(func(session *melody.Session, data []byte) {
		ctx := context.WithValue(context.Background(), StartTimeKey, time.Now().UnixNano())
		//p0 uin64 代表 req_id
		//p0+8 uint64 代表 stmt_id
		//p0+16 uint64 代表 操作类型(1 (set tag) 2 (bind))
		//p0+24 raw block
		p0 := *(*uintptr)(unsafe.Pointer(&data))
		reqID := *(*uint64)(unsafe.Pointer(p0))
		stmtID := *(*uint64)(unsafe.Pointer(p0 + uintptr(8)))
		action := *(*uint64)(unsafe.Pointer(p0 + uintptr(16)))
		block := unsafe.Pointer(p0 + uintptr(24))
		columns := parser.RawBlockGetNumOfCols(block)
		rows := parser.RawBlockGetNumOfRows(block)
		if ctl.stmtM.IsClosed() {
			return
		}
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("get ws stmt block message data:", data)
		switch action {
		case BindMessage:
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).bindBlock(ctx, session, reqID, stmtID, int(rows), int(columns), block)
		case SetTagsMessage:
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).setTagsBlock(ctx, session, reqID, stmtID, int(rows), int(columns), block)
		}
	})

	ctl.stmtM.HandleClose(func(session *melody.Session, i int, s string) error {
		//message := melody.FormatCloseMessage(i, "")
		//session.WriteControl(websocket.CloseMessage, message, time.Now().Add(time.Second))
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws close", i, s)
		t, exist := session.Get(TaosStmtKey)
		if exist && t != nil {
			t.(*TaosStmt).Close()
		}
		return nil
	})

	ctl.stmtM.HandleError(func(session *melody.Session, err error) {
		logger := session.MustGet("logger").(*logrus.Entry)
		_, is := err.(*websocket.CloseError)
		if is {
			logger.WithError(err).Debugln("ws close in error")
		} else {
			logger.WithError(err).Errorln("ws error")
		}
		t, exist := session.Get(TaosStmtKey)
		if exist && t != nil {
			t.(*TaosStmt).Close()
		}
	})

	ctl.stmtM.HandleDisconnect(func(session *melody.Session) {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws disconnect")
		t, exist := session.Get(TaosStmtKey)
		if exist && t != nil {
			t.(*TaosStmt).Close()
		}
	})
}

func (ctl *Restful) stmt(c *gin.Context) {
	id := web.GetRequestID(c)
	loggerWithID := logger.WithField("sessionID", id).WithField("wsType", "stmt")
	_ = ctl.stmtM.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": loggerWithID})
}

type WSStmtErrorResp struct {
	Code    int     `json:"code"`
	Message string  `json:"message"`
	Action  string  `json:"action"`
	ReqID   uint64  `json:"req_id"`
	Timing  int64   `json:"timing"`
	StmtID  *uint64 `json:"stmt_id,omitempty"`
}

func wsStmtErrorMsg(ctx context.Context, session *melody.Session, code int, message string, action string, reqID uint64, stmtID *uint64) {
	b, _ := json.Marshal(&WSStmtErrorResp{
		Code:    code & 0xffff,
		Message: message,
		Action:  action,
		ReqID:   reqID,
		Timing:  getDuration(ctx),
		StmtID:  stmtID,
	})
	session.Write(b)
}
func wsStmtError(ctx context.Context, session *melody.Session, err error, action string, reqID uint64, stmtID *uint64) {
	e, is := err.(*tErrors.TaosError)
	if is {
		wsStmtErrorMsg(ctx, session, int(e.Code)&0xffff, e.ErrStr, action, reqID, stmtID)
	} else {
		wsStmtErrorMsg(ctx, session, 0xffff, err.Error(), action, reqID, stmtID)
	}
}

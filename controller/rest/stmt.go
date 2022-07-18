package rest

import (
	"container/list"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/huskar-t/melody"
	"github.com/sirupsen/logrus"
	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/types"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/db/async"
	"github.com/taosdata/taosadapter/db/commonpool"
	"github.com/taosdata/taosadapter/httperror"
	"github.com/taosdata/taosadapter/thread"
	"github.com/taosdata/taosadapter/tools/pool"
	"github.com/taosdata/taosadapter/tools/web"
)

const TaosStmtKey = "taos_stmt"
const (
	STMTConnect      = "conn"
	STMTInit         = "init"
	STMTPrepare      = "prepare"
	STMTSetTableName = "set_table_name"
	STMTSetTags      = "set_tags"
	STMTBind         = "bind"
	STMTAddBatch     = "add_batch"
	STMTExec         = "exec"
	STMTClose        = "close"
)

type TaosStmt struct {
	Conn            *commonpool.Conn
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
}

func (t *TaosStmt) connect(session *melody.Session, req *StmtConnectReq) {
	t.Lock()
	defer t.Unlock()
	if t.Conn != nil {
		wsStmtErrorMsg(session, 0xffff, "taos duplicate connections", WSConnect, req.ReqID, nil)
		return
	}
	conn, err := commonpool.GetConnection(req.User, req.Password)
	if err != nil {
		wsStmtError(session, err, STMTConnect, req.ReqID, nil)
		return
	}
	if len(req.DB) != 0 {
		b := pool.StringBuilderPoolGet()
		defer pool.StringBuilderPoolPut(b)
		b.WriteString("use ")
		b.WriteString(req.DB)
		err := async.GlobalAsync.TaosExecWithoutResult(conn.TaosConnection, b.String())
		if err != nil {
			conn.Put()
			wsStmtError(session, err, STMTConnect, req.ReqID, nil)
			return
		}
	}
	t.Conn = conn
	wsWriteJson(session, &StmtConnectResp{
		Action: STMTConnect,
		ReqID:  req.ReqID,
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
	StmtID  uint64 `json:"stmt_id"`
}

func (t *TaosStmt) init(session *melody.Session, req *StmtInitReq) {
	if t.Conn == nil {
		wsStmtErrorMsg(session, 0xffff, "taos not connected", STMTInit, req.ReqID, nil)
		return
	}
	thread.Lock()
	stmt := wrapper.TaosStmtInit(t.Conn.TaosConnection)
	thread.Unlock()
	if stmt == nil {
		errStr := wrapper.TaosStmtErrStr(stmt)
		wsStmtErrorMsg(session, 0xffff, errStr, STMTInit, req.ReqID, nil)
		return
	}
	stmtItem := &StmtItem{
		stmt: stmt,
	}
	t.addStmtItem(stmtItem)
	resp := &StmtInitResp{Action: STMTInit, ReqID: req.ReqID, StmtID: stmtItem.index}
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
	StmtID  uint64 `json:"stmt_id"`
}

func (t *TaosStmt) prepare(session *melody.Session, req *StmtPrepareReq) {
	if t.Conn == nil {
		wsStmtErrorMsg(session, 0xffff, "taos not connected", STMTPrepare, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		wsStmtErrorMsg(session, 0xffff, "stmt is nil", STMTPrepare, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	thread.Lock()
	code := wrapper.TaosStmtPrepare(stmt.stmt, req.SQL)
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		wsStmtErrorMsg(session, code, errStr, STMTPrepare, req.ReqID, &req.StmtID)
		return
	}
	resp := &StmtSetTableNameResp{
		Action: STMTPrepare,
		ReqID:  req.ReqID,
		StmtID: req.StmtID,
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
	StmtID  uint64 `json:"stmt_id"`
}

func (t *TaosStmt) setTableName(session *melody.Session, req *StmtSetTableNameReq) {
	if t.Conn == nil {
		wsStmtErrorMsg(session, 0xffff, "taos not connected", STMTSetTableName, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		wsStmtErrorMsg(session, 0xffff, "stmt is nil", STMTSetTableName, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	thread.Lock()
	code := wrapper.TaosStmtSetTBName(stmt.stmt, req.Name)
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		wsStmtErrorMsg(session, code, errStr, STMTSetTableName, req.ReqID, &req.StmtID)
		return
	}
	resp := &StmtSetTableNameResp{
		Action: STMTSetTableName,
		ReqID:  req.ReqID,
		StmtID: req.StmtID,
	}
	wsWriteJson(session, resp)
}

type StmtSetTagsReq struct {
	ReqID  uint64         `json:"req_id"`
	StmtID uint64         `json:"stmt_id"`
	Tags   []driver.Value `json:"tags"`
}

type StmtSetTagsResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	StmtID  uint64 `json:"stmt_id"`
}

func (t *TaosStmt) setTags(session *melody.Session, req *StmtSetTagsReq) {
	if t.Conn == nil {
		wsStmtErrorMsg(session, 0xffff, "taos not connected", STMTSetTags, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		wsStmtErrorMsg(session, 0xffff, "stmt is nil", STMTSetTags, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	thread.Lock()
	code, tagNums, tagFields := wrapper.TaosStmtGetTagFields(stmt.stmt)
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		wsStmtErrorMsg(session, code, errStr, STMTSetTags, req.ReqID, &req.StmtID)
		return
	}
	resp := &StmtSetTableNameResp{
		Action: STMTSetTags,
		ReqID:  req.ReqID,
		StmtID: req.StmtID,
	}
	if tagNums == 0 {
		wsWriteJson(session, resp)
		return
	}
	if len(req.Tags) != tagNums {
		wsStmtErrorMsg(session, 0xffff, "stmt tags count not match", STMTSetTags, req.ReqID, &req.StmtID)
		return
	}
	fields := wrapper.StmtParseFields(tagNums, tagFields)
	tags := make([][]driver.Value, tagNums)
	for i := 0; i < tagNums; i++ {
		tags[i] = []driver.Value{req.Tags[i]}
	}
	err := stmtConvert(tags, fields, nil)
	if err != nil {
		wsStmtErrorMsg(session, 0xffff, fmt.Sprintf("stmt convert error:%s", err.Error()), STMTSetTags, req.ReqID, &req.StmtID)
		return
	}
	for i := 0; i < tagNums; i++ {
		req.Tags[i] = tags[i][0]
	}
	thread.Lock()
	code = wrapper.TaosStmtSetTags(stmt.stmt, req.Tags)
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		wsStmtErrorMsg(session, code, errStr, STMTSetTags, req.ReqID, &req.StmtID)
		return
	}
	wsWriteJson(session, resp)
}

type StmtBindReq struct {
	ReqID   uint64           `json:"req_id"`
	StmtID  uint64           `json:"stmt_id"`
	Columns [][]driver.Value `json:"columns"`
}
type StmtBindResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	StmtID  uint64 `json:"stmt_id"`
}

func (t *TaosStmt) bind(session *melody.Session, req *StmtBindReq) {
	if t.Conn == nil {
		wsStmtErrorMsg(session, 0xffff, "taos not connected", STMTBind, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		wsStmtErrorMsg(session, 0xffff, "stmt is nil", STMTBind, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	thread.Lock()
	code, colNums, colFields := wrapper.TaosStmtGetColFields(stmt.stmt)
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		wsStmtErrorMsg(session, code, errStr, STMTBind, req.ReqID, &req.StmtID)
		return
	}
	resp := &StmtBindResp{
		Action: STMTBind,
		ReqID:  req.ReqID,
		StmtID: req.StmtID,
	}
	if colNums == 0 {
		wsWriteJson(session, resp)
		return
	}
	fields := wrapper.StmtParseFields(colNums, colFields)
	fieldTypes := make([]*types.ColumnType, colNums)
	var err error
	for i := 0; i < colNums; i++ {
		fieldTypes[i], err = fields[i].GetType()
		if err != nil {
			wsStmtErrorMsg(session, 0xffff, fmt.Sprintf("stmt get column type error:%s", err.Error()), STMTBind, req.ReqID, &req.StmtID)
			return
		}
	}
	if len(req.Columns) != colNums {
		wsStmtErrorMsg(session, 0xffff, "stmt column count not match", STMTBind, req.ReqID, &req.StmtID)
		return
	}
	err = stmtConvert(req.Columns, fields, fieldTypes)
	if err != nil {
		wsStmtErrorMsg(session, 0xffff, fmt.Sprintf("stmt convert error:%s", err.Error()), STMTBind, req.ReqID, &req.StmtID)
		return
	}
	thread.Lock()
	wrapper.TaosStmtBindParamBatch(stmt.stmt, req.Columns, fieldTypes)
	thread.Unlock()
	wsWriteJson(session, resp)
	return
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
	StmtID  uint64 `json:"stmt_id"`
}

func (t *TaosStmt) addBatch(session *melody.Session, req *StmtAddBatchReq) {
	if t.Conn == nil {
		wsStmtErrorMsg(session, 0xffff, "taos not connected", STMTAddBatch, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		wsStmtErrorMsg(session, 0xffff, "stmt is nil", STMTAddBatch, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	thread.Lock()
	code := wrapper.TaosStmtAddBatch(stmt.stmt)
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		wsStmtErrorMsg(session, code, errStr, STMTAddBatch, req.ReqID, &req.StmtID)
		return
	}
	resp := &StmtAddBatchResp{
		Action: STMTAddBatch,
		ReqID:  req.ReqID,
		StmtID: req.StmtID,
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
	StmtID   uint64 `json:"stmt_id"`
	Affected int    `json:"affected"`
}

func (t *TaosStmt) exec(session *melody.Session, req *StmtExecReq) {
	if t.Conn == nil {
		wsStmtErrorMsg(session, 0xffff, "taos not connected", STMTExec, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		wsStmtErrorMsg(session, 0xffff, "stmt is nil", STMTExec, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	thread.Lock()
	code := wrapper.TaosStmtExecute(stmt.stmt)
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		wsStmtErrorMsg(session, code, errStr, STMTExec, req.ReqID, &req.StmtID)
		return
	}
	affected := wrapper.TaosStmtAffectedRowsOnce(stmt.stmt)
	resp := &StmtExecResp{
		Action:   STMTExec,
		ReqID:    req.ReqID,
		StmtID:   req.StmtID,
		Affected: affected,
	}
	wsWriteJson(session, resp)
}

type StmtClose struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

func (t *TaosStmt) close(session *melody.Session, req *StmtClose) {
	if t.Conn == nil {
		wsStmtErrorMsg(session, 0xffff, "taos not connected", STMTExec, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		wsStmtErrorMsg(session, 0xffff, "stmt is nil", STMTExec, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	t.removeStmtItem(stmtItem)
	stmt.clean()
}

func (t *TaosStmt) setTagsBlock(session *melody.Session, reqID, stmtID, rows, columns uint64, block unsafe.Pointer) {
	if rows != 1 {
		wsStmtErrorMsg(session, 0xffff, "rows not equal 1", STMTSetTags, reqID, &stmtID)
		return
	}
	if t.Conn == nil {
		wsStmtErrorMsg(session, 0xffff, "taos not connected", STMTSetTags, reqID, &stmtID)
		return
	}
	stmtItem := t.getStmtItem(stmtID)
	if stmtItem == nil {
		wsStmtErrorMsg(session, 0xffff, "stmt is nil", STMTSetTags, reqID, &stmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	thread.Lock()
	code, tagNums, tagFields := wrapper.TaosStmtGetTagFields(stmt.stmt)
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		wsStmtErrorMsg(session, code, errStr, STMTSetTags, reqID, &stmtID)
		return
	}
	resp := &StmtSetTableNameResp{
		Action: STMTSetTags,
		ReqID:  reqID,
		StmtID: stmtID,
	}
	if tagNums == 0 {
		wsWriteJson(session, resp)
		return
	}
	if int(columns) != tagNums {
		wsStmtErrorMsg(session, 0xffff, "stmt tags count not match", STMTSetTags, reqID, &stmtID)
		return
	}
	fields := wrapper.StmtParseFields(tagNums, tagFields)
	tags := blockConvert(block, int(rows), fields)
	thread.Lock()
	reTags := make([]driver.Value, tagNums)
	for i := 0; i < tagNums; i++ {
		reTags[i] = tags[i][0]
	}
	code = wrapper.TaosStmtSetTags(stmt.stmt, reTags)
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		wsStmtErrorMsg(session, code, errStr, STMTSetTags, reqID, &stmtID)
		return
	}
	wsWriteJson(session, resp)
}

func (t *TaosStmt) bindBlock(session *melody.Session, reqID, stmtID, rows, columns uint64, block unsafe.Pointer) {
	if t.Conn == nil {
		wsStmtErrorMsg(session, 0xffff, "taos not connected", STMTBind, reqID, &stmtID)
		return
	}
	stmtItem := t.getStmtItem(stmtID)
	if stmtItem == nil {
		wsStmtErrorMsg(session, 0xffff, "stmt is nil", STMTBind, reqID, &stmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	thread.Lock()
	code, colNums, colFields := wrapper.TaosStmtGetColFields(stmt.stmt)
	thread.Unlock()
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		wsStmtErrorMsg(session, code, errStr, STMTBind, reqID, &stmtID)
		return
	}
	resp := &StmtBindResp{
		Action: STMTBind,
		ReqID:  reqID,
		StmtID: stmtID,
	}
	if colNums == 0 {
		wsWriteJson(session, resp)
		return
	}
	fields := wrapper.StmtParseFields(colNums, colFields)
	fieldTypes := make([]*types.ColumnType, colNums)
	var err error
	for i := 0; i < colNums; i++ {
		fieldTypes[i], err = fields[i].GetType()
		if err != nil {
			wsStmtErrorMsg(session, 0xffff, fmt.Sprintf("stmt get column type error:%s", err.Error()), STMTBind, reqID, &stmtID)
			return
		}
	}
	if int(columns) != colNums {
		wsStmtErrorMsg(session, 0xffff, "stmt column count not match", STMTBind, reqID, &stmtID)
		return
	}
	data := blockConvert(block, int(rows), fields)
	thread.Lock()
	wrapper.TaosStmtBindParamBatch(stmt.stmt, data, fieldTypes)
	thread.Unlock()
	wsWriteJson(session, resp)
	return
}

func (t *TaosStmt) Close() {
	t.Lock()
	defer t.Unlock()
	if t.closed {
		return
	}
	t.closed = true
	t.cleanUp()
	if t.Conn != nil {
		t.Conn.Put()
		t.Conn = nil
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
			t.(*TaosStmt).connect(session, &req)
		case STMTInit:
			var req StmtInitReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal query args")
				return
			}
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).init(session, &req)
		case STMTPrepare:
			var req StmtPrepareReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal fetch args")
				return
			}
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).prepare(session, &req)
		case STMTSetTableName:
			var req StmtSetTableNameReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal fetch args")
				return
			}
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).setTableName(session, &req)
		case STMTSetTags:
			var req StmtSetTagsReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal fetch args")
				return
			}
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).setTags(session, &req)
		case STMTBind:
			var req StmtBindReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal fetch args")
				return
			}
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).bind(session, &req)
		case STMTAddBatch:
			var req StmtAddBatchReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal fetch args")
				return
			}
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).addBatch(session, &req)
		case STMTExec:
			var req StmtExecReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal fetch args")
				return
			}
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).exec(session, &req)
		case STMTClose:
			var req StmtClose
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal fetch args")
				return
			}
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).close(session, &req)
		}
	})
	const (
		SetTagsAction = 1
		BindAction    = 2
	)

	ctl.stmtM.HandleMessageBinary(func(session *melody.Session, data []byte) {
		//p0 uin64 代表 req_id
		//p0+8 uint64 代表 stmt_id
		//p0+16 uint64 代表 操作类型(1 (set tag) 2 (bind))
		//p0+24 uint64 代表 列数
		//p0+32 uint64 代表 行数
		//p0+40 raw block
		p0 := *(*uintptr)(unsafe.Pointer(&data))
		reqID := *(*uint64)(unsafe.Pointer(p0))
		stmtID := *(*uint64)(unsafe.Pointer(p0 + uintptr(8)))
		action := *(*uint64)(unsafe.Pointer(p0 + uintptr(16)))
		columns := *(*uint64)(unsafe.Pointer(p0 + uintptr(24)))
		counts := *(*uint64)(unsafe.Pointer(p0 + uintptr(32)))
		block := unsafe.Pointer(p0 + uintptr(40))
		if ctl.stmtM.IsClosed() {
			return
		}
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("get ws stmt block message data:", data)
		switch action {
		case BindAction:
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).bindBlock(session, reqID, stmtID, counts, columns, block)
		case SetTagsAction:
			t := session.MustGet(TaosStmtKey)
			t.(*TaosStmt).setTagsBlock(session, reqID, stmtID, counts, columns, block)
		}
	})

	ctl.stmtM.HandleClose(func(session *melody.Session, i int, s string) error {
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
	loggerWithID := logger.WithField("sessionID", id)
	_ = ctl.stmtM.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": loggerWithID})
}

type WSStmtErrorResp struct {
	Code    int     `json:"code"`
	Message string  `json:"message"`
	Action  string  `json:"action"`
	ReqID   uint64  `json:"req_id"`
	StmtID  *uint64 `json:"stmt_id,omitempty"`
}

func wsStmtErrorMsg(session *melody.Session, code int, message string, action string, reqID uint64, stmtID *uint64) {
	b, _ := json.Marshal(&WSStmtErrorResp{
		Code:    code & 0xffff,
		Message: message,
		Action:  action,
		ReqID:   reqID,
		StmtID:  stmtID,
	})
	session.Write(b)
}
func wsStmtError(session *melody.Session, err error, action string, reqID uint64, stmtID *uint64) {
	e, is := err.(*tErrors.TaosError)
	if is {
		wsStmtErrorMsg(session, int(e.Code)&0xffff, e.ErrStr, action, reqID, stmtID)
	} else {
		wsStmtErrorMsg(session, 0xffff, err.Error(), action, reqID, stmtID)
	}
}

package syncinterface

import (
	"database/sql/driver"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	stmtCommon "github.com/taosdata/taosadapter/v3/driver/common/stmt"
	taoserrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/types"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/generator"
)

var logger = log.GetLogger("test")

const isDebug = true

func TestMain(m *testing.M) {
	config.Init()
	log.ConfigLog()
	_ = log.SetLevel("trace")
	db.PrepareConnection()
	m.Run()
}
func TestTaosConnect(t *testing.T) {
	reqID := generator.GetReqID()
	var logger = logger.WithField("test", "TestTaosConnect").WithField(config.ReqIDKey, reqID)
	conn, err := TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if !assert.NoError(t, err) {
		return
	}
	defer TaosClose(conn, logger, isDebug)
	wrongConn, err := TaosConnect("", "root", "wrong", "", 0, logger, isDebug)
	assert.Error(t, err)
	assert.Nil(t, wrongConn)
}

func TestTaosSelectDB(t *testing.T) {
	reqID := generator.GetReqID()
	var logger = logger.WithField("test", "TaosSelectDB").WithField(config.ReqIDKey, reqID)
	conn, err := TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if !assert.NoError(t, err) {
		return
	}
	defer TaosClose(conn, logger, isDebug)
	code := TaosSelectDB(conn, "information_schema", logger, isDebug)
	assert.Equal(t, 0, code)
	code = TaosSelectDB(conn, "wrongdb", logger, isDebug)
	assert.NotEqual(t, 0, code)
}

func TestTaosSchemalessInsertRawTTLWithReqIDTBNameKey(t *testing.T) {
	reqID := generator.GetReqID()
	var logger = logger.WithField("test", "TestTaosSchemalessInsertRawTTLWithReqIDTBNameKey").WithField(config.ReqIDKey, reqID)
	conn, err := TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if !assert.NoError(t, err) {
		return
	}
	defer TaosClose(conn, logger, isDebug)
	err = exec(conn, "create database if not exists `syncinterface_test_sml`")
	assert.NoError(t, err)
	defer func() {
		err = exec(conn, "drop database if exists `syncinterface_test_sml`")
		assert.NoError(t, err)
	}()
	code := TaosSelectDB(conn, "syncinterface_test_sml", logger, isDebug)
	assert.Equal(t, 0, code)
	errCode, result := TaosSchemalessInsertRawTTLWithReqIDTBNameKey(conn, "measurement,host=host1 field1=2i,field2=2.0 1577836800000000000", wrapper.InfluxDBLineProtocol, "", 0, reqID, "", logger, isDebug)
	assert.Equal(t, int32(1), errCode)
	assert.NotNil(t, result)
	FreeResult(result, logger, isDebug)
}

func TestTaosGetTablesVgID(t *testing.T) {
	reqID := generator.GetReqID()
	var logger = logger.WithField("test", "TaosSchemalessInsertRawTTLWithReqID").WithField(config.ReqIDKey, reqID)
	conn, err := TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if !assert.NoError(t, err) {
		return
	}
	defer TaosClose(conn, logger, isDebug)
	err = exec(conn, "create database if not exists `syncinterface_test_vgid`")
	assert.NoError(t, err)
	defer func() {
		err = exec(conn, "drop database if exists `syncinterface_test_vgid`")
		assert.NoError(t, err)
	}()
	err = exec(conn, "create table if not exists `syncinterface_test_vgid`.`tb1` (ts timestamp,v int)")
	assert.NoError(t, err)
	vgids, errCode := TaosGetTablesVgID(conn, "syncinterface_test_vgid", []string{"tb1"}, logger, isDebug)
	assert.Equal(t, 0, errCode)
	assert.Equal(t, 1, len(vgids))
}

func TestTaosStmt(t *testing.T) {
	reqID := generator.GetReqID()
	var logger = logger.WithField("test", "TestTaosStmt").WithField(config.ReqIDKey, reqID)
	conn, err := TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if !assert.NoError(t, err) {
		return
	}
	defer TaosClose(conn, logger, isDebug)
	err = exec(conn, "create database if not exists `syncinterface_test_stmt`")
	assert.NoError(t, err)
	defer func() {
		err = exec(conn, "drop database if exists `syncinterface_test_stmt`")
		assert.NoError(t, err)
	}()
	err = exec(conn, "create table if not exists `syncinterface_test_stmt`.`stb1` (ts timestamp,v int) tags (id int)")
	assert.NoError(t, err)
	code := TaosSelectDB(conn, "syncinterface_test_stmt", logger, isDebug)
	assert.Equal(t, 0, code)
	stmt := TaosStmtInitWithReqID(conn, reqID, logger, isDebug)
	if !assert.NotNil(t, stmt, wrapper.TaosStmtErrStr(stmt)) {
		return
	}
	defer func() {
		code = TaosStmtClose(stmt, logger, isDebug)
		assert.Equal(t, 0, code, wrapper.TaosStmtErrStr(stmt))
	}()
	code = TaosStmtPrepare(stmt, "insert into ? using `syncinterface_test_stmt`.`stb1` tags(?) values(?,?)", logger, isDebug)
	if !assert.Equal(t, 0, code, wrapper.TaosStmtErrStr(stmt)) {
		return
	}
	isInsert, code := TaosStmtIsInsert(stmt, logger, isDebug)
	if !assert.Equal(t, 0, code, wrapper.TaosStmtErrStr(stmt)) {
		return
	}
	assert.True(t, isInsert)
	code = TaosStmtSetTBName(stmt, "db1", logger, isDebug)
	if !assert.Equal(t, 0, code, wrapper.TaosStmtErrStr(stmt)) {
		return
	}
	code, num, fields := TaosStmtGetColFields(stmt, logger, isDebug)
	if !assert.Equal(t, 0, code, wrapper.TaosStmtErrStr(stmt)) {
		return
	}
	assert.Equal(t, 2, num)
	assert.NotNil(t, fields)
	defer func() {
		wrapper.TaosStmtReclaimFields(stmt, fields)
	}()
	colFields := wrapper.StmtParseFields(num, fields)
	assert.Equal(t, 2, len(colFields))
	assert.Equal(t, "ts", colFields[0].Name)
	assert.Equal(t, int8(common.TSDB_DATA_TYPE_TIMESTAMP), colFields[0].FieldType)
	assert.Equal(t, "v", colFields[1].Name)
	assert.Equal(t, int8(common.TSDB_DATA_TYPE_INT), colFields[1].FieldType)
	code, num, tags := TaosStmtGetTagFields(stmt, logger, isDebug)
	if !assert.Equal(t, 0, code, wrapper.TaosStmtErrStr(stmt)) {
		return
	}
	assert.Equal(t, 1, num)
	assert.NotNil(t, tags)
	defer func() {
		wrapper.TaosStmtReclaimFields(stmt, tags)
	}()
	tagFields := wrapper.StmtParseFields(num, tags)
	assert.Equal(t, 1, len(tagFields))
	assert.Equal(t, "id", tagFields[0].Name)
	assert.Equal(t, int8(common.TSDB_DATA_TYPE_INT), tagFields[0].FieldType)

	num, code = TaosStmtNumParams(stmt, logger, isDebug)
	if !assert.Equal(t, 0, code, wrapper.TaosStmtErrStr(stmt)) {
		return
	}
	assert.Equal(t, 2, num)

	paramType, paramLen, err := TaosStmtGetParam(stmt, 0, logger, isDebug)
	assert.NoError(t, err)
	assert.Equal(t, common.TSDB_DATA_TYPE_TIMESTAMP, paramType)
	assert.Equal(t, 8, paramLen)

	code = TaosStmtSetTags(stmt, []driver.Value{types.TaosInt(1)}, logger, isDebug)
	if !assert.Equal(t, 0, code, wrapper.TaosStmtErrStr(stmt)) {
		return
	}
	now := time.Now()
	data := [][]driver.Value{
		{types.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, types.TaosTimestamp{T: now.Add(time.Second), Precision: common.PrecisionMilliSecond}}, // ts
		{types.TaosInt(100), types.TaosInt(101)}, // v
	}
	dataType := []*types.ColumnType{{Type: types.TaosTimestampType}, {Type: types.TaosIntType}}
	code = TaosStmtBindParamBatch(stmt, data, dataType, logger, isDebug)
	if !assert.Equal(t, 0, code, wrapper.TaosStmtErrStr(stmt)) {
		return
	}
	code = TaosStmtAddBatch(stmt, logger, isDebug)
	if !assert.Equal(t, 0, code, wrapper.TaosStmtErrStr(stmt)) {
		return
	}
	code = TaosStmtExecute(stmt, logger, isDebug)
	if !assert.Equal(t, 0, code, wrapper.TaosStmtErrStr(stmt)) {
		return
	}
	affected := wrapper.TaosStmtAffectedRowsOnce(stmt)
	assert.Equal(t, 2, affected)
}

func TestTaosGetCurrentDB(t *testing.T) {
	reqID := generator.GetReqID()
	var logger = logger.WithField("test", "TaosGetCurrentDB").WithField(config.ReqIDKey, reqID)
	conn, err := TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if !assert.NoError(t, err) {
		return
	}
	defer TaosClose(conn, logger, isDebug)
	currentDb, err := TaosGetCurrentDB(conn, logger, isDebug)
	assert.NoError(t, err)
	assert.Equal(t, "", currentDb)
	err = exec(conn, "create database if not exists `syncinterface_test_current_db`")
	assert.NoError(t, err)
	defer func() {
		err = exec(conn, "drop database if exists `syncinterface_test_current_db`")
		assert.NoError(t, err)
	}()
	code := TaosSelectDB(conn, "syncinterface_test_current_db", logger, isDebug)
	assert.Equal(t, 0, code)
	currentDb, err = TaosGetCurrentDB(conn, logger, isDebug)
	assert.NoError(t, err)
	assert.Equal(t, "syncinterface_test_current_db", currentDb)
}

func TestTaosGetServerInfo(t *testing.T) {
	reqID := generator.GetReqID()
	var logger = logger.WithField("test", "TestTaosGetServerInfo").WithField(config.ReqIDKey, reqID)
	conn, err := TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if !assert.NoError(t, err) {
		return
	}
	defer TaosClose(conn, logger, isDebug)
	serverInfo := TaosGetServerInfo(conn, logger, isDebug)
	assert.NotEmpty(t, serverInfo)
}

func TestTMQWriteRaw(t *testing.T) {
	data := []byte{
		0x64, 0x01, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x58, 0x01, 0x00, 0x00, 0x04, 0x73, 0x74, 0x62,
		0x00, 0xd5, 0xf0, 0xed, 0x8a, 0xe0, 0x23, 0xf3, 0x45, 0x00, 0x1c, 0x02, 0x09, 0x01, 0x10, 0x02,
		0x03, 0x74, 0x73, 0x00, 0x01, 0x01, 0x02, 0x04, 0x03, 0x63, 0x31, 0x00, 0x02, 0x01, 0x02, 0x06,
		0x03, 0x63, 0x32, 0x00, 0x03, 0x01, 0x04, 0x08, 0x03, 0x63, 0x33, 0x00, 0x04, 0x01, 0x08, 0x0a,
		0x03, 0x63, 0x34, 0x00, 0x05, 0x01, 0x10, 0x0c, 0x03, 0x63, 0x35, 0x00, 0x0b, 0x01, 0x02, 0x0e,
		0x03, 0x63, 0x36, 0x00, 0x0c, 0x01, 0x04, 0x10, 0x03, 0x63, 0x37, 0x00, 0x0d, 0x01, 0x08, 0x12,
		0x03, 0x63, 0x38, 0x00, 0x0e, 0x01, 0x10, 0x14, 0x03, 0x63, 0x39, 0x00, 0x06, 0x01, 0x08, 0x16,
		0x04, 0x63, 0x31, 0x30, 0x00, 0x07, 0x01, 0x10, 0x18, 0x04, 0x63, 0x31, 0x31, 0x00, 0x08, 0x01,
		0x2c, 0x1a, 0x04, 0x63, 0x31, 0x32, 0x00, 0x0a, 0x01, 0xa4, 0x01, 0x1c, 0x04, 0x63, 0x31, 0x33,
		0x00, 0x1c, 0x02, 0x09, 0x02, 0x10, 0x1e, 0x04, 0x74, 0x74, 0x73, 0x00, 0x01, 0x00, 0x02, 0x20,
		0x04, 0x74, 0x63, 0x31, 0x00, 0x02, 0x00, 0x02, 0x22, 0x04, 0x74, 0x63, 0x32, 0x00, 0x03, 0x00,
		0x04, 0x24, 0x04, 0x74, 0x63, 0x33, 0x00, 0x04, 0x00, 0x08, 0x26, 0x04, 0x74, 0x63, 0x34, 0x00,
		0x05, 0x00, 0x10, 0x28, 0x04, 0x74, 0x63, 0x35, 0x00, 0x0b, 0x00, 0x02, 0x2a, 0x04, 0x74, 0x63,
		0x36, 0x00, 0x0c, 0x00, 0x04, 0x2c, 0x04, 0x74, 0x63, 0x37, 0x00, 0x0d, 0x00, 0x08, 0x2e, 0x04,
		0x74, 0x63, 0x38, 0x00, 0x0e, 0x00, 0x10, 0x30, 0x04, 0x74, 0x63, 0x39, 0x00, 0x06, 0x00, 0x08,
		0x32, 0x05, 0x74, 0x63, 0x31, 0x30, 0x00, 0x07, 0x00, 0x10, 0x34, 0x05, 0x74, 0x63, 0x31, 0x31,
		0x00, 0x08, 0x00, 0x2c, 0x36, 0x05, 0x74, 0x63, 0x31, 0x32, 0x00, 0x0a, 0x00, 0xa4, 0x01, 0x38,
		0x05, 0x74, 0x63, 0x31, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x1c, 0x02, 0x02, 0x02,
		0x01, 0x00, 0x02, 0x04, 0x02, 0x01, 0x00, 0x03, 0x06, 0x02, 0x01, 0x00, 0x01, 0x08, 0x02, 0x01,
		0x00, 0x01, 0x0a, 0x02, 0x01, 0x00, 0x01, 0x0c, 0x02, 0x01, 0x00, 0x01, 0x0e, 0x02, 0x01, 0x00,
		0x01, 0x10, 0x02, 0x01, 0x00, 0x01, 0x12, 0x02, 0x01, 0x00, 0x01, 0x14, 0x02, 0x01, 0x00, 0x01,
		0x16, 0x02, 0x01, 0x00, 0x04, 0x18, 0x02, 0x01, 0x00, 0x04, 0x1a, 0x02, 0x01, 0x00, 0xff, 0x1c,
		0x02, 0x01, 0x00, 0xff,
	}
	length := uint32(356)
	metaType := uint16(531)
	reqID := generator.GetReqID()
	var logger = logger.WithField("test", "TestTMQWriteRaw").WithField(config.ReqIDKey, reqID)
	conn, err := TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if !assert.NoError(t, err) {
		return
	}
	defer TaosClose(conn, logger, isDebug)
	err = exec(conn, "create database if not exists `syncinterface_test_write_raw`")
	assert.NoError(t, err)
	defer func() {
		err = exec(conn, "drop database if exists `syncinterface_test_write_raw`")
		assert.NoError(t, err)
	}()
	errCode := TaosSelectDB(conn, "syncinterface_test_write_raw", logger, isDebug)
	assert.Equal(t, 0, errCode)
	code := TMQWriteRaw(conn, length, metaType, unsafe.Pointer(&data[0]), logger, isDebug)
	assert.Equal(t, int32(0), code)
	d, err := query(conn, "describe stb")
	assert.NoError(t, err)
	expect := [][]driver.Value{
		{"ts", "TIMESTAMP", int32(8), ""},
		{"c1", "BOOL", int32(1), ""},
		{"c2", "TINYINT", int32(1), ""},
		{"c3", "SMALLINT", int32(2), ""},
		{"c4", "INT", int32(4), ""},
		{"c5", "BIGINT", int32(8), ""},
		{"c6", "TINYINT UNSIGNED", int32(1), ""},
		{"c7", "SMALLINT UNSIGNED", int32(2), ""},
		{"c8", "INT UNSIGNED", int32(4), ""},
		{"c9", "BIGINT UNSIGNED", int32(8), ""},
		{"c10", "FLOAT", int32(4), ""},
		{"c11", "DOUBLE", int32(8), ""},
		{"c12", "VARCHAR", int32(20), ""},
		{"c13", "NCHAR", int32(20), ""},
		{"tts", "TIMESTAMP", int32(8), "TAG"},
		{"tc1", "BOOL", int32(1), "TAG"},
		{"tc2", "TINYINT", int32(1), "TAG"},
		{"tc3", "SMALLINT", int32(2), "TAG"},
		{"tc4", "INT", int32(4), "TAG"},
		{"tc5", "BIGINT", int32(8), "TAG"},
		{"tc6", "TINYINT UNSIGNED", int32(1), "TAG"},
		{"tc7", "SMALLINT UNSIGNED", int32(2), "TAG"},
		{"tc8", "INT UNSIGNED", int32(4), "TAG"},
		{"tc9", "BIGINT UNSIGNED", int32(8), "TAG"},
		{"tc10", "FLOAT", int32(4), "TAG"},
		{"tc11", "DOUBLE", int32(8), "TAG"},
		{"tc12", "VARCHAR", int32(20), "TAG"},
		{"tc13", "NCHAR", int32(20), "TAG"},
	}
	for rowIndex, values := range d {
		for i := 0; i < 4; i++ {
			assert.Equal(t, expect[rowIndex][i], values[i])
		}
	}
}

func TestTaosWriteRawBlockWithReqID(t *testing.T) {
	data := []byte{
		0x01, 0x00, 0x00, 0x00, 0x54, 0x01, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x08, 0x00, 0x00,
		0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x03, 0x02, 0x00, 0x00, 0x00,
		0x04, 0x04, 0x00, 0x00, 0x00, 0x05, 0x08, 0x00, 0x00, 0x00, 0x0b, 0x01, 0x00, 0x00, 0x00, 0x0c,
		0x02, 0x00, 0x00, 0x00, 0x0d, 0x04, 0x00, 0x00, 0x00, 0x0e, 0x08, 0x00, 0x00, 0x00, 0x06, 0x04,
		0x00, 0x00, 0x00, 0x07, 0x08, 0x00, 0x00, 0x00, 0x08, 0x16, 0x00, 0x00, 0x00, 0x0a, 0x52, 0x00,
		0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x04, 0x00,
		0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x04, 0x00,
		0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x10, 0x00,
		0x00, 0x00, 0x0d, 0x00, 0x00, 0x00, 0x2a, 0x00, 0x00, 0x00, 0x00, 0x60, 0x75, 0x28, 0x98, 0x91,
		0x01, 0x00, 0x00, 0x48, 0x79, 0x28, 0x98, 0x91, 0x01, 0x00, 0x00, 0x40, 0x01, 0x00, 0x40, 0x01,
		0x00, 0x40, 0x01, 0x00, 0x00, 0x00, 0x40, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
		0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x40, 0x01, 0x00, 0x40, 0x01, 0x00, 0x00, 0x00, 0x40, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x40, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x40, 0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0xff, 0xff, 0xff, 0xff, 0x0b, 0x00, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x62, 0x69, 0x6e, 0x61, 0x72,
		0x79, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x28, 0x00, 0x74, 0x00, 0x00, 0x00, 0x65,
		0x00, 0x00, 0x00, 0x73, 0x00, 0x00, 0x00, 0x74, 0x00, 0x00, 0x00, 0x5f, 0x00, 0x00, 0x00, 0x6e,
		0x00, 0x00, 0x00, 0x63, 0x00, 0x00, 0x00, 0x68, 0x00, 0x00, 0x00, 0x61, 0x00, 0x00, 0x00, 0x72,
		0x00, 0x00, 0x00, 0x00,
	}
	reqID := generator.GetReqID()
	var logger = logger.WithField("test", "TestTaosWriteRawBlockWithReqID").WithField(config.ReqIDKey, reqID)
	conn, err := TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if !assert.NoError(t, err) {
		return
	}
	defer TaosClose(conn, logger, isDebug)
	err = exec(conn, "create database if not exists `syncinterface_test_write_raw_block`")
	assert.NoError(t, err)
	defer func() {
		err = exec(conn, "drop database if exists `syncinterface_test_write_raw_block`")
		assert.NoError(t, err)
	}()
	errCode := TaosSelectDB(conn, "syncinterface_test_write_raw_block", logger, isDebug)
	assert.Equal(t, 0, errCode)
	err = exec(conn, "create table if not exists all_type (ts timestamp,"+
		"c1 bool,"+
		"c2 tinyint,"+
		"c3 smallint,"+
		"c4 int,"+
		"c5 bigint,"+
		"c6 tinyint unsigned,"+
		"c7 smallint unsigned,"+
		"c8 int unsigned,"+
		"c9 bigint unsigned,"+
		"c10 float,"+
		"c11 double,"+
		"c12 binary(20),"+
		"c13 nchar(20)"+
		") tags (info json)")
	assert.NoError(t, err)
	err = exec(conn, "create table t1 using all_type tags('{\"a\":2}')")
	assert.NoError(t, err)
	code := TaosWriteRawBlockWithReqID(conn, 2, unsafe.Pointer(&data[0]), "t1", reqID, logger, isDebug)
	assert.Equal(t, 0, code)
	d, err := query(conn, "select count(*) from t1")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), d[0][0])
}

func TestTaosWriteRawBlockWithFieldsWithReqID(t *testing.T) {
	reqID := generator.GetReqID()
	var logger = logger.WithField("test", "TestTaosWriteRawBlockWithFieldsWithReqID").WithField(config.ReqIDKey, reqID)
	conn, err := TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if !assert.NoError(t, err) {
		return
	}
	defer TaosClose(conn, logger, isDebug)
	err = exec(conn, "create database if not exists `syncinterface_test_write_raw_block_with_fields`")
	assert.NoError(t, err)
	defer func() {
		err = exec(conn, "drop database if exists `syncinterface_test_write_raw_block_with_fields`")
		assert.NoError(t, err)
	}()
	errCode := TaosSelectDB(conn, "syncinterface_test_write_raw_block_with_fields", logger, isDebug)
	assert.Equal(t, 0, errCode)
	err = exec(conn, "create table if not exists all_type (ts timestamp,"+
		"c1 bool,"+
		"c2 tinyint,"+
		"c3 smallint,"+
		"c4 int,"+
		"c5 bigint,"+
		"c6 tinyint unsigned,"+
		"c7 smallint unsigned,"+
		"c8 int unsigned,"+
		"c9 bigint unsigned,"+
		"c10 float,"+
		"c11 double,"+
		"c12 binary(20),"+
		"c13 nchar(20)"+
		") tags (info json)")
	assert.NoError(t, err)
	err = exec(conn, "create table t1 using all_type tags('{\"a\":2}')")
	assert.NoError(t, err)
	rows := 2
	data := []byte{
		0x01, 0x00, 0x00, 0x00, 0x43, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x08, 0x00, 0x00,
		0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x47,
		0x2a, 0x38, 0x98, 0x91, 0x01, 0x00, 0x00, 0x2f, 0x2e, 0x38, 0x98, 0x91, 0x01, 0x00, 0x00, 0x40,
		0x01, 0x00, 0x00,
	}
	fields := []byte{
		0x74, 0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x09, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x63, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
	}
	code := TaosWriteRawBlockWithFieldsWithReqID(conn, rows, unsafe.Pointer(&data[0]), "t1", unsafe.Pointer(&fields[0]), 2, reqID, logger, isDebug)
	assert.Equal(t, 0, code)
	d, err := query(conn, "select count(*) from t1")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), d[0][0])
}

type TestStmt2Result struct {
	Res      unsafe.Pointer
	Affected int
	N        int
}

type Stmt2CallBackCaller struct {
	ExecResult chan *TestStmt2Result
}

func (s *Stmt2CallBackCaller) ExecCall(res unsafe.Pointer, affected int, code int) {
	s.ExecResult <- &TestStmt2Result{
		Res:      res,
		Affected: affected,
		N:        code,
	}
}

func TestTaosStmt2(t *testing.T) {
	reqID := generator.GetReqID()
	var logger = logger.WithField("test", "TestTaosStmt2").WithField(config.ReqIDKey, reqID)
	conn, err := TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if !assert.NoError(t, err) {
		return
	}
	defer TaosClose(conn, logger, isDebug)
	err = exec(conn, "create database if not exists `syncinterface_test_stmt2`")
	assert.NoError(t, err)
	defer func() {
		err = exec(conn, "drop database if exists `syncinterface_test_stmt2`")
		assert.NoError(t, err)
	}()
	err = exec(conn, "create table if not exists `syncinterface_test_stmt2`.`stb1` (ts timestamp,v int) tags (id int)")
	assert.NoError(t, err)
	code := TaosSelectDB(conn, "syncinterface_test_stmt2", logger, isDebug)
	assert.Equal(t, 0, code)
	caller := &Stmt2CallBackCaller{
		ExecResult: make(chan *TestStmt2Result, 1),
	}
	handle := cgo.NewHandle(caller)
	stmt := TaosStmt2Init(conn, reqID, false, false, handle, logger, isDebug)
	if !assert.NotNil(t, stmt, wrapper.TaosStmt2Error(stmt)) {
		return
	}
	defer func() {
		code = TaosStmt2Close(stmt, logger, isDebug)
		assert.Equal(t, 0, code, wrapper.TaosStmt2Error(stmt))
	}()
	code = TaosStmt2Prepare(stmt, "insert into ? using `syncinterface_test_stmt2`.`stb1` tags(?) values(?,?)", logger, isDebug)
	if !assert.Equal(t, 0, code, wrapper.TaosStmt2Error(stmt)) {
		return
	}
	isInsert, code := TaosStmt2IsInsert(stmt, logger, isDebug)
	if !assert.Equal(t, 0, code, wrapper.TaosStmt2Error(stmt)) {
		return
	}
	assert.True(t, isInsert)
	code, count, fields := TaosStmt2GetFields(stmt, logger, isDebug)
	if !assert.Equal(t, 0, code, wrapper.TaosStmt2Error(stmt)) {
		return
	}
	assert.Equal(t, 4, count)
	assert.NotNil(t, fields)
	defer func() {
		wrapper.TaosStmt2FreeFields(stmt, fields)
	}()
	fs := wrapper.Stmt2ParseAllFields(count, fields)
	assert.Equal(t, 4, len(fs))
	assert.Equal(t, "tbname", fs[0].Name)
	assert.Equal(t, int8(common.TSDB_DATA_TYPE_BINARY), fs[0].FieldType)
	assert.Equal(t, int8(stmtCommon.TAOS_FIELD_TBNAME), fs[0].BindType)
	assert.Equal(t, "id", fs[1].Name)
	assert.Equal(t, int8(common.TSDB_DATA_TYPE_INT), fs[1].FieldType)
	assert.Equal(t, int8(stmtCommon.TAOS_FIELD_TAG), fs[1].BindType)
	assert.Equal(t, "ts", fs[2].Name)
	assert.Equal(t, int8(common.TSDB_DATA_TYPE_TIMESTAMP), fs[2].FieldType)
	assert.Equal(t, int8(stmtCommon.TAOS_FIELD_COL), fs[2].BindType)
	assert.Equal(t, uint8(common.PrecisionMilliSecond), fs[2].Precision)
	assert.Equal(t, "v", fs[3].Name)
	assert.Equal(t, int8(common.TSDB_DATA_TYPE_INT), fs[3].FieldType)
	assert.Equal(t, int8(stmtCommon.TAOS_FIELD_COL), fs[3].BindType)
	tableName := "tb1"
	binds := &stmtCommon.TaosStmt2BindData{
		TableName: tableName,
	}
	bs, err := stmtCommon.MarshalStmt2Binary([]*stmtCommon.TaosStmt2BindData{binds}, true, nil)
	assert.NoError(t, err)
	err = TaosStmt2BindBinary(stmt, bs, -1, logger, isDebug)
	assert.NoError(t, err)

	code, num, fields2 := TaosStmt2GetFields(stmt, logger, isDebug)
	if !assert.Equal(t, 0, code, wrapper.TaosStmt2Error(stmt)) {
		return
	}
	assert.Equal(t, 4, num)
	assert.NotNil(t, fields)
	defer func() {
		wrapper.TaosStmt2FreeFields(stmt, fields2)
	}()
	fsAfterBindTableName := wrapper.Stmt2ParseAllFields(num, fields2)
	assert.Equal(t, 4, len(fsAfterBindTableName))
	assert.Equal(t, "tbname", fsAfterBindTableName[0].Name)
	assert.Equal(t, int8(common.TSDB_DATA_TYPE_BINARY), fsAfterBindTableName[0].FieldType)
	assert.Equal(t, int8(stmtCommon.TAOS_FIELD_TBNAME), fsAfterBindTableName[0].BindType)
	assert.Equal(t, "id", fsAfterBindTableName[1].Name)
	assert.Equal(t, int8(common.TSDB_DATA_TYPE_INT), fsAfterBindTableName[1].FieldType)
	assert.Equal(t, int8(stmtCommon.TAOS_FIELD_TAG), fsAfterBindTableName[1].BindType)
	assert.Equal(t, "ts", fsAfterBindTableName[2].Name)
	assert.Equal(t, int8(common.TSDB_DATA_TYPE_TIMESTAMP), fsAfterBindTableName[2].FieldType)
	assert.Equal(t, int8(stmtCommon.TAOS_FIELD_COL), fsAfterBindTableName[2].BindType)
	assert.Equal(t, uint8(common.PrecisionMilliSecond), fsAfterBindTableName[2].Precision)
	assert.Equal(t, "v", fsAfterBindTableName[3].Name)
	assert.Equal(t, int8(common.TSDB_DATA_TYPE_INT), fsAfterBindTableName[3].FieldType)
	assert.Equal(t, int8(stmtCommon.TAOS_FIELD_COL), fsAfterBindTableName[3].BindType)
	binds = &stmtCommon.TaosStmt2BindData{
		Tags: []driver.Value{int32(1)},
	}

	bs, err = stmtCommon.MarshalStmt2Binary([]*stmtCommon.TaosStmt2BindData{binds}, true, fsAfterBindTableName[1:2])
	assert.NoError(t, err)
	err = TaosStmt2BindBinary(stmt, bs, -1, logger, isDebug)
	assert.NoError(t, err)

	now := time.Now()
	binds = &stmtCommon.TaosStmt2BindData{
		Cols: [][]driver.Value{
			{now, now.Add(time.Second)},
			{int32(100), int32(101)},
		},
	}
	bs, err = stmtCommon.MarshalStmt2Binary([]*stmtCommon.TaosStmt2BindData{binds}, true, fsAfterBindTableName[2:])
	assert.NoError(t, err)
	err = TaosStmt2BindBinary(stmt, bs, -1, logger, isDebug)
	assert.NoError(t, err)

	code = TaosStmt2Exec(stmt, logger, isDebug)
	if !assert.Equal(t, 0, code, wrapper.TaosStmt2Error(stmt)) {
		return
	}
	result := <-caller.ExecResult
	assert.NotNil(t, result)
	assert.Equal(t, 0, result.N)
	assert.Equal(t, 2, result.Affected)
}

func exec(conn unsafe.Pointer, sql string) error {
	result := wrapper.TaosQuery(conn, sql)
	defer wrapper.TaosFreeResult(result)
	code := wrapper.TaosError(result)
	if code != 0 {
		return taoserrors.NewError(code, wrapper.TaosErrorStr(result))
	}
	return nil
}

func query(conn unsafe.Pointer, sql string) ([][]driver.Value, error) {
	res := wrapper.TaosQuery(conn, sql)
	defer wrapper.TaosFreeResult(res)
	code := wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		return nil, taoserrors.NewError(code, errStr)
	}
	fileCount := wrapper.TaosNumFields(res)
	rh, err := wrapper.ReadColumn(res, fileCount)
	if err != nil {
		return nil, err
	}
	precision := wrapper.TaosResultPrecision(res)
	var result [][]driver.Value
	for {
		columns, errCode, block := wrapper.TaosFetchRawBlock(res)
		if errCode != 0 {
			errStr := wrapper.TaosErrorStr(res)
			return nil, taoserrors.NewError(errCode, errStr)
		}
		if columns == 0 {
			break
		}
		r, err := parser.ReadBlock(block, columns, rh.ColTypes, precision)
		if err != nil {
			return nil, err
		}
		result = append(result, r...)
	}
	return result, nil
}

func TestTaosOptionsConnection(t *testing.T) {
	reqID := generator.GetReqID()
	var logger = logger.WithField("test", "TestTaosOptionsConnection").WithField(config.ReqIDKey, reqID)
	conn, err := TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if !assert.NoError(t, err) {
		return
	}
	defer TaosClose(conn, logger, isDebug)
	app := "test_sync_interface"
	code := TaosOptionsConnection(conn, common.TSDB_OPTION_CONNECTION_USER_APP, &app, logger, isDebug)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(nil)
		t.Error(t, taoserrors.NewError(code, errStr))
		return
	}
	code = TaosOptionsConnection(conn, common.TSDB_OPTION_CONNECTION_USER_APP, nil, logger, isDebug)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(nil)
		t.Error(t, taoserrors.NewError(code, errStr))
		return
	}
}

func TestTaosValidateSql(t *testing.T) {
	reqID := generator.GetReqID()
	var logger = logger.WithField("test", "TestTaosValidateSql").WithField(config.ReqIDKey, reqID)
	conn, err := TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if !assert.NoError(t, err) {
		return
	}
	defer TaosClose(conn, logger, isDebug)
	code := TaosValidateSql(conn, "create database if not exists `syncinterface_test_validate`", logger, isDebug)
	assert.Equal(t, 0, code)
	code = TaosValidateSql(conn, "create table syncinterface_test_validate.t(ts timestamp,v int)", logger, isDebug)
	assert.NotEqual(t, 0, code)
}

func TestTaosCheckServerStatus(t *testing.T) {
	reqID := generator.GetReqID()
	var logger = logger.WithField("test", "TestTaosCheckServerStatus").WithField(config.ReqIDKey, reqID)
	localhost := "localhost"
	status, detail := TaosCheckServerStatus(&localhost, 0, logger, isDebug)
	assert.Equal(t, int32(2), status)
	assert.Equal(t, "", detail)
	status, detail = TaosCheckServerStatus(nil, 0, logger, isDebug)
	assert.Equal(t, int32(2), status)
	assert.Equal(t, "", detail)
}

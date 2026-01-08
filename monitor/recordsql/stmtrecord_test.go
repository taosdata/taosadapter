package recordsql

import (
	"encoding/csv"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetStmtRecord(t *testing.T) {
	t.Run("no mission", func(t *testing.T) {
		setMission(RecordTypeStmt, nil)
		record, running := GetStmtRecord()
		assert.Nil(t, record)
		assert.False(t, running)
	})

	t.Run("mission not running", func(t *testing.T) {
		mission := &RecordMission{recordType: RecordTypeStmt, running: false}
		setMission(RecordTypeStmt, mission)
		record, running := GetStmtRecord()
		assert.Nil(t, record)
		assert.False(t, running)
	})

	t.Run("mission running", func(t *testing.T) {
		mission := &RecordMission{
			recordType: RecordTypeStmt,
			running:    true,
			recordList: NewRecordList(),
		}
		setMission(RecordTypeStmt, mission)
		defer setMission(RecordTypeStmt, nil)

		record, running := GetStmtRecord()
		require.NotNil(t, record)
		assert.True(t, running)
		assert.Equal(t, mission, record.mission)
		assert.NotNil(t, record.ele)
	})
}

func TestPutStmtRecord(t *testing.T) {
	mission := &RecordMission{
		recordType: RecordTypeStmt,
		running:    true,
		recordList: NewRecordList(),
		logger:     logrus.NewEntry(logrus.New()),
		csvWriter:  csv.NewWriter(&mockWriter{}),
	}
	setMission(RecordTypeStmt, mission)
	defer setMission(RecordTypeStmt, nil)

	record, _ := GetStmtRecord()
	require.NotNil(t, record)
	record.SQL = "SELECT 1"

	PutStmtRecord(record)

	// Verify record was reset and returned to pool
	assert.Empty(t, record.SQL)
	assert.Nil(t, record.mission)
	assert.Nil(t, record.ele)

	record2, _ := GetStmtRecord()
	assert.NotNil(t, record2)
	assert.Equal(t, record, record2, "Should return the same record from pool")
}

func TestStmtRecordInit(t *testing.T) {
	r := &StmtRecord{}
	sql := "SELECT * FROM test where id = ?"
	ip := "127.0.0.1"
	user := "testuser"
	connType := HTTPType
	qid := uint64(12345)
	startTime := time.Now()
	port := "38000"
	appName := "testapp"

	bindData := []byte{0x01, 0x02, 0x03}

	r.InitPrepare(1, ip, port, appName, user, connType, qid, startTime, sql)

	assert.Equal(t, sql, r.SQL)
	assert.Equal(t, ip, r.IP)
	assert.Equal(t, user, r.User)
	assert.Equal(t, connType, r.ConnType)
	assert.Equal(t, qid, r.QID)
	assert.Equal(t, startTime, r.StartTime)
	assert.Equal(t, appName, r.AppName)
	assert.Equal(t, port, r.SourcePort)

	r.reset()
	assert.Equal(t, "", r.SQL)
	assert.Equal(t, "", r.IP)
	assert.Equal(t, "", r.User)
	assert.Equal(t, ConnType(0), r.ConnType)
	assert.Equal(t, uint64(0), r.QID)
	assert.Equal(t, time.Time{}, r.StartTime)
	assert.Equal(t, "", r.AppName)
	assert.Equal(t, "", r.SourcePort)

	r.InitBind(2, ip, port, appName, user, connType, qid, startTime, bindData)
	assert.Equal(t, bindData, r.BindData)
	assert.Equal(t, ip, r.IP)
	assert.Equal(t, user, r.User)
	assert.Equal(t, connType, r.ConnType)
	assert.Equal(t, qid, r.QID)
	assert.Equal(t, startTime, r.StartTime)
	assert.Equal(t, appName, r.AppName)
	assert.Equal(t, port, r.SourcePort)

	r.reset()
	assert.Equal(t, []byte(nil), r.BindData)
	assert.Equal(t, "", r.IP)
	assert.Equal(t, "", r.User)
	assert.Equal(t, ConnType(0), r.ConnType)
	assert.Equal(t, uint64(0), r.QID)
	assert.Equal(t, time.Time{}, r.StartTime)
	assert.Equal(t, "", r.AppName)
	assert.Equal(t, "", r.SourcePort)

	r.InitExecute(3, ip, port, appName, user, connType, qid, startTime)
	assert.Equal(t, ip, r.IP)
	assert.Equal(t, user, r.User)
	assert.Equal(t, connType, r.ConnType)
	assert.Equal(t, qid, r.QID)
	assert.Equal(t, startTime, r.StartTime)
	assert.Equal(t, appName, r.AppName)
	assert.Equal(t, port, r.SourcePort)
	r.reset()
	assert.Equal(t, []byte(nil), r.BindData)
	assert.Equal(t, "", r.IP)
	assert.Equal(t, "", r.User)
	assert.Equal(t, ConnType(0), r.ConnType)
	assert.Equal(t, uint64(0), r.QID)
	assert.Equal(t, time.Time{}, r.StartTime)
	assert.Equal(t, "", r.AppName)
	assert.Equal(t, "", r.SourcePort)

}

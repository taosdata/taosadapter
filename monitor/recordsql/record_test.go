package recordsql

import (
	"container/list"
	"encoding/csv"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/config"
)

func TestRecordInit(t *testing.T) {
	r := &Record{}
	sql := "SELECT * FROM test"
	ip := "127.0.0.1"
	user := "testuser"
	connType := HTTPType
	qid := uint64(12345)
	receiveTime := time.Now()
	port := "38000"
	appName := "testapp"

	r.Init(sql, ip, port, appName, user, connType, qid, receiveTime)

	assert.Equal(t, sql, r.SQL)
	assert.Equal(t, ip, r.IP)
	assert.Equal(t, user, r.User)
	assert.Equal(t, connType, r.ConnType)
	assert.Equal(t, qid, r.QID)
	assert.Equal(t, receiveTime, r.ReceiveTime)
	assert.Equal(t, appName, r.AppName)
	assert.Equal(t, port, r.SourcePort)
}

func TestRecordDurationSetters(t *testing.T) {
	r := &Record{}
	duration := 100 * time.Millisecond

	t.Run("SetQueryDuration", func(t *testing.T) {
		r.SetQueryDuration(duration)
		assert.Equal(t, duration, r.QueryDuration)
	})

	t.Run("AddFetchDuration", func(t *testing.T) {
		r.AddFetchDuration(duration)
		assert.Equal(t, duration, r.FetchDuration)
		r.AddFetchDuration(duration)
		assert.Equal(t, 2*duration, r.FetchDuration)
	})

	t.Run("SetGetConnDuration", func(t *testing.T) {
		r.SetGetConnDuration(duration)
		assert.Equal(t, duration, r.GetConnDuration)
	})

	t.Run("SetFreeTime", func(t *testing.T) {
		now := time.Now()
		r.SetFreeTime(now)
		assert.Equal(t, now, r.FreeTime)
	})
}

func TestRecordToRow(t *testing.T) {
	now := time.Now()
	r := &Record{
		SQL:             "SELECT * FROM test",
		IP:              "127.0.0.1",
		User:            "testuser",
		ConnType:        HTTPType,
		QID:             uint64(12345),
		ReceiveTime:     now,
		FreeTime:        now.Add(100 * time.Millisecond),
		QueryDuration:   50 * time.Millisecond,
		FetchDuration:   30 * time.Millisecond,
		GetConnDuration: 20 * time.Millisecond,
		SourcePort:      "38000",
		AppName:         "testapp",
	}

	row := r.toRow()
	rowTime, err := time.ParseInLocation(ResultTimeFormat, row[TSIndex], time.Local)
	require.NoError(t, err)
	assert.False(t, rowTime.IsZero())
	assert.Greater(t, time.Now().UnixNano(), rowTime.UnixNano())
	assert.Equal(t, r.SQL, row[SQLIndex])
	assert.Equal(t, r.IP, row[IPIndex])
	assert.Equal(t, r.User, row[UserIndex])
	assert.Equal(t, "http", row[ConnTypeIndex])
	assert.Equal(t, "0x3039", row[QIDIndex]) // 12345 in hex
	assert.Equal(t, now.Format(ResultTimeFormat), row[ReceiveTimeIndex])
	assert.Equal(t, r.FreeTime.Format(ResultTimeFormat), row[FreeTimeIndex])
	assert.Equal(t, "50000", row[QueryDurationIndex]) // microseconds
	assert.Equal(t, "30000", row[FetchDurationIndex])
	assert.Equal(t, "20000", row[GetConnDurationIndex])
	assert.Equal(t, "100000", row[TotalDurationIndex])
	assert.Equal(t, "38000", row[SourcePortIndex])
	assert.Equal(t, "testapp", row[AppNameIndex])
}

func TestRecordToRowWithZeroFreeTime(t *testing.T) {
	now := time.Now()
	r := &Record{
		SQL:         "SELECT * FROM test",
		ReceiveTime: now,
		// FreeTime is zero
	}

	row := r.toRow()

	// Should calculate duration from now to current time
	// Since we can't predict the exact duration, just verify it exists
	assert.NotEmpty(t, row[TotalDurationIndex])
}

func TestRecordReset(t *testing.T) {
	r := &Record{
		SQL:             "SELECT * FROM test",
		IP:              "127.0.0.1",
		User:            "testuser",
		ConnType:        HTTPType,
		QID:             uint64(12345),
		ReceiveTime:     time.Now(),
		FreeTime:        time.Now(),
		QueryDuration:   50 * time.Millisecond,
		FetchDuration:   30 * time.Millisecond,
		GetConnDuration: 20 * time.Millisecond,
		SourcePort:      "38000",
		AppName:         "testapp",
		totalDuration:   100 * time.Millisecond,
		mission:         &RecordMission{},
		ele:             &list.Element{},
	}

	r.reset()

	assert.Empty(t, r.SQL)
	assert.Empty(t, r.IP)
	assert.Empty(t, r.User)
	assert.Equal(t, ConnType(0), r.ConnType)
	assert.Equal(t, uint64(0), r.QID)
	assert.True(t, r.ReceiveTime.IsZero())
	assert.True(t, r.FreeTime.IsZero())
	assert.Equal(t, time.Duration(0), r.QueryDuration)
	assert.Equal(t, time.Duration(0), r.FetchDuration)
	assert.Equal(t, time.Duration(0), r.GetConnDuration)
	assert.Equal(t, time.Duration(0), r.totalDuration)
	assert.Empty(t, r.SourcePort)
	assert.Empty(t, r.AppName)
	assert.Nil(t, r.ele)
	assert.Nil(t, r.mission)
}

func TestRecordWrite(t *testing.T) {
	t.Run("with nil mission", func(t *testing.T) {
		r := &Record{}
		r.write() // should not panic
	})

	t.Run("with mission", func(t *testing.T) {
		mission := &RecordMission{
			list:   NewRecordList(),
			logger: logrus.NewEntry(logrus.New()),
		}
		r := &Record{mission: mission}
		ele := mission.list.Add(r)
		r.ele = ele

		r.write()                               // should remove from list and attempt to write
		assert.Nil(t, mission.list.Remove(ele)) // should already be removed
	})
}

func TestWriteRecord(t *testing.T) {
	t.Run("empty SQL", func(t *testing.T) {
		mission := &RecordMission{
			csvWriter: csv.NewWriter(&mockWriter{}),
			logger:    logrus.NewEntry(logrus.New()),
		}
		r := &Record{} // empty SQL
		mission.writeRecord(r)
		// Should return without writing
	})

	t.Run("successful write", func(t *testing.T) {
		mockCSV := &mockCSVWriter{}
		mission := &RecordMission{
			csvWriter: mockCSV,
			logger:    logrus.NewEntry(logrus.New()),
		}
		r := &Record{
			SQL: "SELECT 1",
		}

		mission.writeRecord(r)
		assert.True(t, mockCSV.writeCalled)
	})

	t.Run("write error", func(t *testing.T) {
		mockCSV := &mockCSVWriter{err: assert.AnError}
		mission := &RecordMission{
			csvWriter: mockCSV,
			logger:    logrus.NewEntry(logrus.New()),
		}
		r := &Record{
			SQL: "SELECT 1",
		}

		mission.writeRecord(r)
		assert.True(t, mockCSV.writeCalled)
	})
}

func TestConnTypeString(t *testing.T) {
	tests := []struct {
		connType ConnType
		expected string
	}{
		{HTTPType, "http"},
		{WSType, "ws"},
		{ConnType(3), "3"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.connType.String())
		})
	}
}

func TestGetSQLRecord(t *testing.T) {
	t.Run("no mission", func(t *testing.T) {
		setGlobalRecordMission(nil)
		record, running := GetSQLRecord()
		assert.Nil(t, record)
		assert.False(t, running)
	})

	t.Run("mission not running", func(t *testing.T) {
		mission := &RecordMission{running: false}
		setGlobalRecordMission(mission)
		record, running := GetSQLRecord()
		assert.Nil(t, record)
		assert.False(t, running)
	})

	t.Run("mission running", func(t *testing.T) {
		mission := &RecordMission{
			running: true,
			list:    NewRecordList(),
		}
		setGlobalRecordMission(mission)
		defer setGlobalRecordMission(nil)

		record, running := GetSQLRecord()
		require.NotNil(t, record)
		assert.True(t, running)
		assert.Equal(t, mission, record.mission)
		assert.NotNil(t, record.ele)
	})
}

func TestPutSQLRecord(t *testing.T) {
	mission := &RecordMission{
		running:   true,
		list:      NewRecordList(),
		logger:    logrus.NewEntry(logrus.New()),
		csvWriter: csv.NewWriter(&mockWriter{}),
	}
	setGlobalRecordMission(mission)
	defer setGlobalRecordMission(nil)

	record, _ := GetSQLRecord()
	require.NotNil(t, record)
	record.SQL = "SELECT 1"

	PutSQLRecord(record)

	// Verify record was reset and returned to pool
	assert.Empty(t, record.SQL)
	assert.Nil(t, record.mission)
	assert.Nil(t, record.ele)

	record2, _ := GetSQLRecord()
	assert.NotNil(t, record2)
	assert.Equal(t, record, record2, "Should return the same record from pool")
}

// Mock implementations for testing
type mockWriter struct{}

func (m *mockWriter) Write(p []byte) (n int, err error) {
	return 0, nil
}

type mockCSVWriter struct {
	writeCalled bool
	err         error
}

func (m *mockCSVWriter) Write(record []string) error {
	m.writeCalled = true
	return m.err
}

func (m *mockCSVWriter) Flush() {
}

func TestRotate(t *testing.T) {
	tmpDir := t.TempDir()
	oldPath := config.Conf.Log.Path
	defer func() {
		config.Conf.Log.Path = oldPath
	}()
	if globalRotateWriter != nil {
		_ = globalRotateWriter.Close()
		globalRotateWriter = nil
	}
	defer func() {
		if globalRotateWriter != nil {
			err := globalRotateWriter.Close()
			assert.NoError(t, err, "Failed to close globalRotateWriter")
			globalRotateWriter = nil
		}
	}()
	config.Conf.Log.Path = tmpDir
	oldRotateSize := config.Conf.Log.RotationSize
	defer func() {
		config.Conf.Log.RotationSize = oldRotateSize
	}()
	config.Conf.Log.RotationSize = 20
	err := StartRecordSql(time.Now().Format(InputTimeFormat), time.Now().Add(time.Second*2).Format(InputTimeFormat), "")
	require.NoError(t, err)
	defer func() {
		_ = StopRecordSql()
	}()
	record := &Record{
		SQL:             "SELECT * FROM test",
		IP:              "127.0.0.1",
		User:            "root",
		ConnType:        WSType,
		QID:             0x12345,
		ReceiveTime:     time.Now(),
		FreeTime:        time.Now().Add(time.Second),
		QueryDuration:   100,
		FetchDuration:   100,
		GetConnDuration: 100,
		totalDuration:   100,
		SourcePort:      "38000",
		AppName:         "testapp",
	}
	for i := 0; i < 10; i++ {
		getGlobalRecordMission().writeRecord(record)
		getGlobalRecordMission().csvWriter.Flush()
	}
	files, err := getRecordFiles(tmpDir)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(files), files)
}

package recordsql

import (
	"container/list"
	"context"
	"encoding/csv"
	"os"
	"path"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	rotatelogs "github.com/taosdata/file-rotatelogs/v2"
)

func TestGlobalRecordMission(t *testing.T) {
	globalRecordMission = nil

	t.Run("Test get on nil", func(t *testing.T) {
		got := getGlobalRecordMission()
		if got != nil {
			t.Errorf("Expected nil, got %v", got)
		}
	})

	t.Run("Test set and get", func(t *testing.T) {
		testRecord := &RecordMission{}

		setGlobalRecordMission(testRecord)

		got := getGlobalRecordMission()
		if got != testRecord {
			t.Errorf("Expected %v, got %v", testRecord, got)
		}
	})

	t.Run("Test concurrent access", func(t *testing.T) {

		var wg sync.WaitGroup
		iterations := 1000
		testRecord := &RecordMission{}

		wg.Add(iterations * 2)
		for i := 0; i < iterations; i++ {
			go func() {
				defer wg.Done()
				setGlobalRecordMission(testRecord)
			}()
			go func() {
				defer wg.Done()
				_ = getGlobalRecordMission()
			}()
		}
		wg.Wait()

		got := getGlobalRecordMission()
		if got != testRecord {
			t.Errorf("Expected %v after concurrent access, got %v", testRecord, got)
		}
	})
}

func TestStartRecordSql(t *testing.T) {
	// Setup
	tempDir := t.TempDir()
	validStart := time.Now().Add(1 * time.Hour).Format(InputTimeFormat)
	validEnd := time.Now().Add(2 * time.Hour).Format(InputTimeFormat)
	validOutFile := "output.csv"
	total, _, err := rotatelogs.GetDiskSize(tempDir)
	if err != nil {
		t.Fatalf("Failed to get disk size: %v", err)
	}

	// Mock global state
	originalGlobal := getGlobalRecordMission()
	defer setGlobalRecordMission(originalGlobal)

	tests := []struct {
		name         string
		startTime    string
		endTime      string
		logPath      string
		outFile      string
		location     string
		reservedDisk uint64
		preTest      func()
		postTest     func()
		expectedErr  string
	}{
		{
			name:        "already running",
			startTime:   validStart,
			endTime:     validEnd,
			logPath:     tempDir,
			outFile:     validOutFile,
			preTest:     func() { setGlobalRecordMission(&RecordMission{running: true}) },
			postTest:    func() { setGlobalRecordMission(nil) },
			expectedErr: "record sql is already running",
		},
		{
			name:        "invalid location format",
			startTime:   validStart,
			endTime:     validEnd,
			logPath:     tempDir,
			outFile:     validOutFile,
			location:    "Invalid/Location",
			expectedErr: "invalid location format",
		},
		{
			name:        "invalid start time format",
			startTime:   "invalid-time",
			endTime:     validEnd,
			logPath:     tempDir,
			outFile:     validOutFile,
			expectedErr: "invalid start time format",
		},
		{
			name:        "invalid end time format",
			startTime:   validStart,
			endTime:     "invalid-time",
			logPath:     tempDir,
			outFile:     validOutFile,
			expectedErr: "invalid end time format",
		},
		{
			name:        "start time after end time",
			startTime:   time.Now().Add(2 * time.Hour).Format(InputTimeFormat),
			endTime:     time.Now().Add(1 * time.Hour).Format(InputTimeFormat),
			logPath:     tempDir,
			outFile:     validOutFile,
			expectedErr: "is after end time",
		},
		{
			name:        "end time in past",
			startTime:   time.Now().Add(-2 * time.Hour).Format(InputTimeFormat),
			endTime:     time.Now().Add(-1 * time.Hour).Format(InputTimeFormat),
			logPath:     tempDir,
			outFile:     validOutFile,
			expectedErr: "is in the past",
		},
		{
			name:        "empty output file",
			startTime:   validStart,
			endTime:     validEnd,
			logPath:     tempDir,
			outFile:     "",
			expectedErr: "output file cannot be empty",
		},
		{
			name:        "not exists log path",
			startTime:   validStart,
			endTime:     validEnd,
			logPath:     "/nonexistent/path",
			outFile:     validOutFile,
			expectedErr: "error getting disk size for log path",
		},
		{
			name:      "output file not in log path",
			startTime: validStart,
			endTime:   validEnd,
			logPath:   tempDir,
			outFile:   "../output.csv",
			// This expects the test to create a file one level above tempDir
			expectedErr: "output file directory does not match log path",
		},
		{
			name:         "reserved disk size exceeds total disk size",
			startTime:    validStart,
			endTime:      validEnd,
			logPath:      tempDir,
			outFile:      validOutFile,
			reservedDisk: total + 1, // Set reserved disk size larger than total
			expectedErr:  "not enough disk space in log path",
		},
		{
			name:        "successful start",
			startTime:   validStart,
			endTime:     validEnd,
			logPath:     tempDir,
			outFile:     validOutFile,
			postTest:    func() { getGlobalRecordMission().cancelFunc() },
			expectedErr: "",
		},
		{
			name:      "successful with location",
			startTime: validStart,
			endTime:   validEnd,
			logPath:   tempDir,
			outFile:   "with_location.csv",
			location:  "UTC",
			postTest:  func() { getGlobalRecordMission().cancelFunc() },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset global state before each test
			setGlobalRecordMission(nil)

			if tt.preTest != nil {
				tt.preTest()
			}

			// For the "output file not in log path" case, we need to create the parent dir
			if tt.name == "output file not in log path" {
				parentDir := filepath.Join(tempDir, "..")
				err := os.MkdirAll(parentDir, 0755)
				require.NoError(t, err)
			}
			var reservedDisk uint64 = 1024
			if tt.reservedDisk != 0 {
				reservedDisk = tt.reservedDisk
			}
			err := StartRecordSql(tt.startTime, tt.endTime, tt.logPath, tt.outFile, tt.location, 1, reservedDisk)

			if tt.expectedErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			} else {
				require.NoError(t, err)
				mission := getGlobalRecordMission()
				require.NotNil(t, mission)
				assert.False(t, mission.running) // should be false until start time
				assert.NotNil(t, mission.outFile)
				assert.NotNil(t, mission.csvWriter)
				assert.NotNil(t, mission.ctx)
				assert.NotNil(t, mission.cancelFunc)
				assert.Equal(t, tt.outFile, filepath.Base(mission.outFile.Name()))
			}

			if tt.postTest != nil {
				tt.postTest()
			}
		})
	}
}

func TestStopRecordSql(t *testing.T) {
	// Setup test cases
	tmpDir := t.TempDir()
	tests := []struct {
		name        string
		setup       func() *RecordMission
		expectStop  bool
		expectError bool
	}{
		{
			name: "No running mission",
			setup: func() *RecordMission {
				setGlobalRecordMission(nil)
				return nil
			},
			expectStop:  false,
			expectError: false,
		},
		{
			name: "Running mission",
			setup: func() *RecordMission {
				out := path.Join(tmpDir, "test.csv")
				f, err := os.Create(out)
				require.NoError(t, err)
				mission := &RecordMission{
					running:    true,
					cancelFunc: func() {},
					outFile:    f,
					csvWriter:  csv.NewWriter(f),
					list:       NewRecordList(),
					logger:     logrus.NewEntry(logrus.New()),
					startChan:  make(chan struct{}, 1),
				}
				close(mission.startChan)
				setGlobalRecordMission(mission)
				return mission
			},
			expectStop:  true,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test case
			expectedMission := tt.setup()

			// Run the function
			_ = StopRecordSql()

			// Verify results
			if tt.expectStop && expectedMission != nil {
				assert.False(t, expectedMission.running, "mission should be stopped")
			}

			// Check global mission is cleared
			assert.Nil(t, getGlobalRecordMission(), "global mission should be nil after stop")
		})
	}
}

func TestStopRecordSqlConcurrency(t *testing.T) {
	// Setup a running mission
	tmpDir := t.TempDir()
	out := path.Join(tmpDir, "test.csv")
	f, err := os.Create(out)
	require.NoError(t, err)
	mission := &RecordMission{
		running:    true,
		cancelFunc: func() {},
		outFile:    f,
		csvWriter:  csv.NewWriter(f),
		list:       NewRecordList(),
		logger:     logrus.NewEntry(logrus.New()),
		startChan:  make(chan struct{}),
	}
	setGlobalRecordMission(mission)
	close(mission.startChan)
	// Test concurrent stops
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = StopRecordSql()
		}()
	}
	wg.Wait()

	// Verify mission was stopped and cleared
	assert.False(t, mission.running, "mission should be stopped")
	assert.Nil(t, getGlobalRecordMission(), "global mission should be nil after stop")
}

func TestRecordMissionStart(t *testing.T) {
	tests := []struct {
		name         string
		startTime    time.Time
		ctxCancel    bool
		expectRun    bool
		delayCheck   time.Duration // how long to wait before checking if it started
		alreadyStart bool
	}{
		{
			name:      "start immediately",
			startTime: time.Now().Add(-1 * time.Second), // in the past
			expectRun: true,
		},
		{
			name:      "start after delay",
			startTime: time.Now().Add(10 * time.Millisecond),
			expectRun: true,
		},
		{
			name:      "context canceled before start",
			startTime: time.Now().Add(100 * time.Millisecond),
			ctxCancel: true,
			expectRun: false,
		},
		{
			name:         "already started",
			startTime:    time.Now().Add(-1 * time.Second),
			alreadyStart: true,
			expectRun:    false,
		},
		{
			name:       "start with future time",
			startTime:  time.Now().Add(100 * time.Millisecond), // future time
			expectRun:  true,
			delayCheck: 100 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			tempFile, err := os.CreateTemp("", "test-recording-*.csv")
			require.NoError(t, err)
			defer func() {
				err = os.Remove(tempFile.Name())
				assert.NoError(t, err)
			}()

			mission := &RecordMission{
				startTime:  tt.startTime,
				outFile:    tempFile,
				csvWriter:  csv.NewWriter(tempFile),
				ctx:        ctx,
				cancelFunc: cancel,
				startChan:  make(chan struct{}, 1),
				list:       NewRecordList(),
				logger:     logger,
			}

			if tt.alreadyStart {
				close(mission.startChan)
			}

			if tt.ctxCancel {
				cancel()
			}

			// Execute
			mission.start()

			// Verify
			if tt.expectRun {
				// Give some time for the goroutine to start
				delay := 20 * time.Millisecond
				if tt.delayCheck != 0 {
					delay = tt.delayCheck
				}
				time.Sleep(delay)
				assert.True(t, mission.isRunning())
			} else {
				assert.False(t, mission.isRunning())
			}

			// Cleanup
			if mission.isRunning() {
				mission.Stop()
			}
		})
	}
}

func TestRecordMissionRun(t *testing.T) {
	t.Run("normal run until context done", func(t *testing.T) {
		// Setup
		ctx, cancel := context.WithCancel(context.Background())

		tempFile, err := os.CreateTemp("", "test-recording-*.csv")
		require.NoError(t, err)
		defer func() {
			err = os.Remove(tempFile.Name())
			assert.NoError(t, err)
		}()

		mission := &RecordMission{
			outFile:    tempFile,
			csvWriter:  csv.NewWriter(tempFile),
			ctx:        ctx,
			cancelFunc: cancel,
			startChan:  make(chan struct{}, 1),
			list:       NewRecordList(),
			logger:     logger,
			running:    true, // manually set to running
		}

		// Execute in goroutine
		go mission.run()
		close(mission.startChan)
		// Verify it's running
		assert.True(t, mission.isRunning())

		// Cancel the context to stop
		cancel()

		// Give some time to stop
		time.Sleep(10 * time.Millisecond)
		assert.False(t, mission.isRunning())
	})
	t.Run("check get disk size", func(t *testing.T) {
		// Setup
		tmpDir := t.TempDir()
		CheckDiskSizeInterval = 10 * time.Millisecond
		defer func() { CheckDiskSizeInterval = 25 * time.Second }()
		ctx, cancel := context.WithCancel(context.Background())

		tempFile, err := os.CreateTemp("", "test-recording-*.csv")
		require.NoError(t, err)
		defer func() {
			err = os.Remove(tempFile.Name())
			assert.NoError(t, err)
		}()

		mission := &RecordMission{
			outFile:    tempFile,
			csvWriter:  csv.NewWriter(tempFile),
			ctx:        ctx,
			cancelFunc: cancel,
			startChan:  make(chan struct{}, 1),
			list:       NewRecordList(),
			logger:     logger,
			running:    true, // manually set to running
			logDir:     tmpDir,
		}

		// Execute in goroutine
		go mission.run()
		close(mission.startChan)
		// Verify it's running
		assert.True(t, mission.isRunning())
		time.Sleep(time.Millisecond * 20)
		assert.Greater(t, mission.availDiskSize.Load(), uint64(0))
		// Cancel the context to stop
		cancel()

		// Give some time to stop
		time.Sleep(10 * time.Millisecond)
		assert.False(t, mission.isRunning())
	})
}

func TestRecordMissionStop(t *testing.T) {
	t.Run("normal stop", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		tempFile, err := os.CreateTemp("", "test-recording-*.csv")
		require.NoError(t, err)
		defer func() {
			err = os.Remove(tempFile.Name())
			assert.NoError(t, err)
		}()

		mission := &RecordMission{
			outFile:    tempFile,
			csvWriter:  csv.NewWriter(tempFile),
			ctx:        ctx,
			cancelFunc: cancel,
			startChan:  make(chan struct{}, 1),
			list:       NewRecordList(),
			logger:     logger,
			running:    true, // manually set to running
		}

		// Add some test records
		now := time.Now()
		testRecord := &Record{
			SQL:             "test sql",
			IP:              "::1",
			User:            "root",
			ConnType:        HTTPType,
			QID:             0x1234,
			ReceiveTime:     now.Add(-time.Second),
			FreeTime:        now,
			QueryDuration:   time.Millisecond,
			FetchDuration:   time.Millisecond,
			GetConnDuration: time.Millisecond,
			totalDuration:   time.Second,
			mission:         mission,
		}
		ele := mission.list.Add(testRecord)
		require.NotNil(t, ele)
		assert.Equal(t, testRecord, ele.Value)
		close(mission.startChan)
		// Execute
		mission.Stop()

		// Verify
		assert.False(t, mission.isRunning())
		assert.True(t, mission.ctx.Err() != nil) // context should be canceled

		// Verify file was closed (attempting to write should fail)
		_, err = tempFile.WriteString("test")
		assert.Error(t, err)
	})

	t.Run("double stop", func(t *testing.T) {
		// Setup
		logger := logrus.NewEntry(logrus.New())
		ctx, cancel := context.WithCancel(context.Background())

		tempFile, err := os.CreateTemp("", "test-recording-*.csv")
		require.NoError(t, err)
		defer func() {
			err = os.Remove(tempFile.Name())
			assert.NoError(t, err)
		}()

		mission := &RecordMission{
			outFile:    tempFile,
			csvWriter:  csv.NewWriter(tempFile),
			ctx:        ctx,
			cancelFunc: cancel,
			startChan:  make(chan struct{}, 1),
			list:       NewRecordList(),
			logger:     logger,
			running:    true, // manually set to running
		}
		close(mission.startChan)
		// Execute stop twice
		mission.Stop()
		mission.Stop() // should be no-op due to closeOnce

		// Verify
		assert.False(t, mission.isRunning())
	})

	t.Run("stop with write error", func(t *testing.T) {
		// Setup
		ctx, cancel := context.WithCancel(context.Background())

		// Create and immediately close a temp file to force error on write
		tempFile, err := os.CreateTemp("", "test-recording-*.csv")
		require.NoError(t, err)
		err = tempFile.Close()
		require.NoError(t, err)
		defer func() {
			err = os.Remove(tempFile.Name())
			assert.NoError(t, err)
		}()

		mission := &RecordMission{
			outFile:    tempFile,
			csvWriter:  csv.NewWriter(tempFile),
			ctx:        ctx,
			cancelFunc: cancel,
			startChan:  make(chan struct{}, 1),
			list:       NewRecordList(),
			logger:     logger,
			running:    true, // manually set to running
		}

		// Add some test records
		now := time.Now()
		testRecord := &Record{
			SQL:             "test sql",
			IP:              "::1",
			User:            "root",
			ConnType:        HTTPType,
			QID:             0x1234,
			ReceiveTime:     now.Add(-time.Second),
			FreeTime:        now,
			QueryDuration:   time.Millisecond,
			FetchDuration:   time.Millisecond,
			GetConnDuration: time.Millisecond,
			totalDuration:   time.Second,
			mission:         mission,
		}
		ele := mission.list.Add(testRecord)
		require.NotNil(t, ele)
		assert.Equal(t, testRecord, ele.Value)
		close(mission.startChan)
		// Execute
		mission.Stop()

		// Verify - main thing is that it doesn't panic
		assert.False(t, mission.isRunning())
	})
}

func TestRecordInit(t *testing.T) {
	r := &Record{}
	sql := "SELECT * FROM test"
	ip := "127.0.0.1"
	user := "testuser"
	connType := HTTPType
	qid := uint64(12345)
	receiveTime := time.Now()

	r.Init(sql, ip, user, connType, qid, receiveTime)

	assert.Equal(t, sql, r.SQL)
	assert.Equal(t, ip, r.IP)
	assert.Equal(t, user, r.User)
	assert.Equal(t, connType, r.ConnType)
	assert.Equal(t, qid, r.QID)
	assert.Equal(t, receiveTime, r.ReceiveTime)
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
	}

	row := r.toRow()

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

	t.Run("no enough disk space", func(t *testing.T) {
		mockCSV := &mockCSVWriter{err: os.ErrNotExist} // Simulate disk space error
		mission := &RecordMission{
			csvWriter:        mockCSV,
			logger:           logrus.NewEntry(logrus.New()),
			reservedDiskSize: 1024,
		}
		mission.availDiskSize.Store(10) // Set available disk size less than reserved
		r := &Record{
			SQL: "SELECT 1",
		}

		mission.writeRecord(r)
		assert.False(t, mockCSV.writeCalled)
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
	t.Run("over limit records", func(t *testing.T) {
		mission := &RecordMission{
			running:       true,
			list:          NewRecordList(),
			maxConcurrent: 1,
		}
		setGlobalRecordMission(mission)
		defer setGlobalRecordMission(nil)

		record, running := GetSQLRecord()
		require.NotNil(t, record)
		assert.True(t, running)
		assert.Equal(t, mission, record.mission)
		assert.NotNil(t, record.ele)

		// Try to get another record, should return nil
		record2, running2 := GetSQLRecord()
		assert.Nil(t, record2)
		assert.False(t, running2)
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

func TestGetRecordState(t *testing.T) {
	// Setup
	tempDir := t.TempDir()
	tempFile, err := os.CreateTemp(tempDir, "test-recording-*.csv")
	require.NoError(t, err)
	defer func() {
		err = tempFile.Close()
		assert.NoError(t, err)
	}()

	originalMission := getGlobalRecordMission()
	defer setGlobalRecordMission(originalMission)

	tests := []struct {
		name          string
		setupMission  func() *RecordMission
		expectedState RecordState
	}{
		{
			name:         "no mission exists",
			setupMission: func() *RecordMission { return nil },
			expectedState: RecordState{
				Exists: false,
			},
		},
		{
			name: "mission exists but not running",
			setupMission: func() *RecordMission {
				ctx, cancel := context.WithCancel(context.Background())
				return &RecordMission{
					running:          false,
					ctx:              ctx,
					cancelFunc:       cancel,
					maxConcurrent:    10,
					startTime:        time.Now(),
					endTime:          time.Now().Add(1 * time.Hour),
					outFile:          tempFile,
					logDir:           tempDir,
					reservedDiskSize: 1024,
					csvWriter:        csv.NewWriter(tempFile),
					startChan:        make(chan struct{}),
					list:             NewRecordList(),
					logger:           logrus.NewEntry(logrus.New()),
				}
			},
			expectedState: RecordState{
				Exists:             true,
				Running:            false,
				StartTime:          time.Now().Format(ResultTimeFormat),
				EndTime:            time.Now().Add(1 * time.Hour).Format(ResultTimeFormat),
				File:               filepath.Base(tempFile.Name()),
				MaxConcurrent:      10,
				CurrentConcurrent:  0,
				ReservedDiskBytes:  1024,
				AvailableDiskBytes: 0,
			},
		},
		{
			name: "mission exists and running",
			setupMission: func() *RecordMission {
				ctx, cancel := context.WithCancel(context.Background())
				mission := &RecordMission{
					running:          true,
					ctx:              ctx,
					cancelFunc:       cancel,
					maxConcurrent:    20,
					startTime:        time.Now(),
					endTime:          time.Now().Add(2 * time.Hour),
					outFile:          tempFile,
					logDir:           tempDir,
					reservedDiskSize: 2048,
					csvWriter:        csv.NewWriter(tempFile),
					startChan:        make(chan struct{}),
					list:             NewRecordList(),
					logger:           logrus.NewEntry(logrus.New()),
				}
				mission.currentCount.Store(5)
				mission.availDiskSize.Store(512)
				return mission
			},
			expectedState: RecordState{
				Exists:             true,
				Running:            true,
				StartTime:          time.Now().Format(ResultTimeFormat),
				EndTime:            time.Now().Add(2 * time.Hour).Format(ResultTimeFormat),
				File:               filepath.Base(tempFile.Name()),
				MaxConcurrent:      20,
				CurrentConcurrent:  5,
				ReservedDiskBytes:  2048,
				AvailableDiskBytes: 512,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test mission
			mission := tt.setupMission()
			setGlobalRecordMission(mission)

			// Execute
			state := GetRecordState()

			// Verify
			assert.Equal(t, tt.expectedState.Exists, state.Exists)
			if tt.expectedState.Exists {
				assert.Equal(t, tt.expectedState.Running, state.Running)
				assert.Equal(t, tt.expectedState.File, state.File)
				assert.Equal(t, tt.expectedState.MaxConcurrent, state.MaxConcurrent)
				assert.Equal(t, tt.expectedState.CurrentConcurrent, state.CurrentConcurrent)
				assert.Equal(t, tt.expectedState.ReservedDiskBytes, state.ReservedDiskBytes)
				assert.Equal(t, tt.expectedState.AvailableDiskBytes, state.AvailableDiskBytes)

				// For time fields, we can't compare exact values, so just check they're formatted
				assert.NotEmpty(t, state.StartTime)
				assert.NotEmpty(t, state.EndTime)
			}
		})
	}
}

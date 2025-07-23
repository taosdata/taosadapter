package recordsql

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/log"
)

const DefaultRecordSqlEndTime = "2300-01-01 00:00:00"
const DefaultFlushInterval = 5 * time.Second
const (
	InputTimeFormat  = "2006-01-02 15:04:05"
	ResultTimeFormat = "2006-01-02 15:04:05.000000"
)

const (
	TSIndex = iota
	SQLIndex
	IPIndex
	UserIndex
	ConnTypeIndex
	QIDIndex
	ReceiveTimeIndex
	FreeTimeIndex
	QueryDurationIndex
	FetchDurationIndex
	GetConnDurationIndex
	TotalDurationIndex
)

type outputWriter interface {
	Write([]string) error
	Flush()
}

type RecordMission struct {
	running      bool
	runningLock  sync.RWMutex
	ctx          context.Context
	cancelFunc   context.CancelFunc
	currentCount atomic.Int32
	startTime    time.Time
	endTime      time.Time
	csvWriter    outputWriter
	startChan    chan struct{}
	writeLock    sync.Mutex
	list         *RecordList
	logger       *logrus.Entry
	closeOnce    sync.Once
}

type RecordState struct {
	Exists            bool   `json:"exists"`
	Running           bool   `json:"running"`
	StartTime         string `json:"start_time"`
	EndTime           string `json:"end_time"`
	CurrentConcurrent int32  `json:"current_concurrent"`
}

var globalRecordMission *RecordMission
var lock sync.RWMutex

func getGlobalRecordMission() *RecordMission {
	lock.RLock()
	defer lock.RUnlock()
	return globalRecordMission
}

func setGlobalRecordMission(record *RecordMission) {
	lock.Lock()
	defer lock.Unlock()
	globalRecordMission = record
}

// record sql logger
var logger = log.GetLogger("RSQ")

func StartRecordSql(startTime, endTime string, location string) error {
	return StartRecordSqlWithTestWriter(startTime, endTime, location, nil)
}

func StartRecordSqlWithTestWriter(startTime, endTime string, location string, testWriter io.Writer) error {
	logger.Tracef("start record sql, startTime: %s, endTime: %s, location: %s", startTime, endTime, location)
	if IsRunning() {
		return fmt.Errorf("record sql is already running")
	}

	loc := time.Local
	var err error
	if location != "" {
		loc, err = time.LoadLocation(location)
		if err != nil {
			return fmt.Errorf("invalid location format: %s, error: %s", location, err)
		}
	}
	startTimeParsed, err := time.ParseInLocation(InputTimeFormat, startTime, loc)
	if err != nil {
		return fmt.Errorf("invalid start time format: %s, error: %s", startTime, err)
	}
	endTimeParsed, err := time.ParseInLocation(InputTimeFormat, endTime, loc)
	if err != nil {
		return fmt.Errorf("invalid end time format: %s, error: %s", endTime, err)
	}
	if startTimeParsed.After(endTimeParsed) {
		return fmt.Errorf("start time %s is after end time %s", startTime, endTime)
	}
	if endTimeParsed.Before(time.Now()) {
		return fmt.Errorf("end time %s is in the past", endTime)
	}
	writer := testWriter
	if writer == nil {
		writer, err = getRotateWriter()
		if err != nil {
			return fmt.Errorf("error getting rotate writer: %s", err)
		}
	}
	csvWriter := csv.NewWriter(writer)
	ctx, cancel := context.WithDeadline(context.Background(), endTimeParsed)
	mission := &RecordMission{
		startTime:  startTimeParsed,
		endTime:    endTimeParsed,
		csvWriter:  csvWriter,
		ctx:        ctx,
		cancelFunc: cancel,
		startChan:  make(chan struct{}, 1),
		list:       NewRecordList(),
		logger:     logger,
	}
	duration := time.Until(mission.startTime)
	if duration <= 0 {
		// run immediately
		mission.setRunning(true)
		go mission.run(DefaultFlushInterval)
		close(mission.startChan)
	} else {
		go mission.start()
	}
	setGlobalRecordMission(mission)
	return nil
}

type MissionInfo struct {
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time"`
}

func StopRecordSql() *MissionInfo {
	mission := getGlobalRecordMission()
	if mission == nil {
		return nil
	}
	mission.Stop()
	return &MissionInfo{
		StartTime: mission.startTime.Format(InputTimeFormat),
		EndTime:   mission.endTime.Format(InputTimeFormat),
	}
}

func IsRunning() bool {
	mission := getGlobalRecordMission()
	if mission == nil {
		return false
	}
	return mission.isRunning()
}

func (c *RecordMission) isRunning() bool {
	c.runningLock.RLock()
	defer c.runningLock.RUnlock()
	return c.running
}

func (c *RecordMission) setRunning(running bool) {
	c.runningLock.Lock()
	defer c.runningLock.Unlock()
	c.running = running
}

func (c *RecordMission) start() {
	select {
	case <-c.startChan:
		// already started
		return
	default:
	}
	defer func() {
		close(c.startChan)
	}()
	duration := time.Until(c.startTime)
	c.logger.Infof("start recording after %s", duration.String())
	timer := time.NewTimer(duration)
	defer func() {
		timer.Stop()
	}()
	select {
	case <-timer.C:
		c.setRunning(true)
		go c.run(DefaultFlushInterval)
		return
	case <-c.ctx.Done():
		return
	}
}

func (c *RecordMission) run(flushInterval time.Duration) {
	c.logger.Infof("start recording sql")
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			c.Stop()
		case <-ticker.C:
			if c.writeLock.TryLock() {
				c.csvWriter.Flush()
				c.writeLock.Unlock()
			}
		}
	}
}

func (c *RecordMission) Stop() {
	c.closeOnce.Do(func() {
		c.logger.Infof("stop recording sql")
		// set flag false to stop
		c.runningLock.Lock()
		defer c.runningLock.Unlock()
		c.running = false
		// cancel the context to stop the goroutine
		c.cancelFunc()
		// wait for the start to complete
		<-c.startChan
		// write all remaining records to the csv file
		records := c.list.RemoveAll()
		for i := 0; i < len(records); i++ {
			c.writeRecord(records[i])
		}
		// flush csv
		c.csvWriter.Flush()
		setGlobalRecordMission(nil)
	})
}

func (c *RecordMission) writeRecord(record *Record) {
	logger.Tracef("write record: %s", record.SQL)
	c.currentCount.Add(-1)
	record.Lock()
	defer record.Unlock()
	if len(record.SQL) == 0 {
		logger.Warn("record SQL is empty, skip writing")
		return
	}
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	row := record.toRow()
	c.logger.Tracef("writing record to csv: %s", row)
	err := c.csvWriter.Write(row)
	if err != nil {
		c.logger.Errorf("error writing record to csv: %s, data: %s", err, row)
	}
}

func GetState() RecordState {
	mission := getGlobalRecordMission()
	if mission == nil {
		return RecordState{
			Exists: false,
		}
	}
	mission.runningLock.RLock()
	defer mission.runningLock.RUnlock()
	state := RecordState{
		Exists:            true,
		Running:           mission.running,
		StartTime:         mission.startTime.Format(InputTimeFormat),
		EndTime:           mission.endTime.Format(InputTimeFormat),
		CurrentConcurrent: mission.currentCount.Load(),
	}
	return state
}

func Init() error {
	if config.Conf.Log.EnableSqlToCsvLogging {
		return StartRecordSql(time.Now().Format(InputTimeFormat), DefaultRecordSqlEndTime, "")
	}
	return nil
}

func Close() {
	// close the global record mission and writer
	mission := getGlobalRecordMission()
	if mission != nil {
		mission.Stop()
	}
	setGlobalRecordMission(nil)
	// close the global rotate writer if it exists
	if globalRotateWriter != nil {
		_ = globalRotateWriter.Close()
	}
}

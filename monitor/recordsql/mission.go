package recordsql

import (
	"container/list"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/log"
)

type RecordMission struct {
	enabled    bool
	enableLock sync.RWMutex
	ctx        context.Context
	cancelFunc context.CancelFunc
	startTime  time.Time
	endTime    time.Time
	outFile    *os.File
	csvWriter  *csv.Writer
	startChan  chan struct{}
	writeLock  sync.Mutex
	list       *RecordList
	logger     *logrus.Entry
	closeOnce  sync.Once
}

var globalRecordMission *RecordMission
var lock sync.RWMutex

func getGlobalRecordMission() *RecordMission {
	lock.RLock()
	defer lock.RUnlock()
	return globalRecordMission
}

func setGlobalRecordSql(record *RecordMission) {
	lock.Lock()
	defer lock.Unlock()
	globalRecordMission = record
}

const RecordSQLTimeFormat = "2006-01-02 15:04:05"

// record sql logger
var logger = log.GetLogger("RSQ")

func StartRecordSql(startTime, endTime string, logPath string, outFile string, location string) error {
	if Enabled() {
		return fmt.Errorf("record sql is already enabled")
	}

	loc := time.Local
	var err error
	defer func() {
		if err != nil {
			logger.Error(err.Error())
		}
	}()
	if location != "" {
		loc, err = time.LoadLocation(location)
		if err != nil {
			return fmt.Errorf("invalid location format: %v, error: %v", location, err)
		}
	}
	startTimeParsed, err := time.ParseInLocation(RecordSQLTimeFormat, startTime, loc)
	if err != nil {
		return fmt.Errorf("invalid start time format: %v, error: %v", startTime, err)
	}
	endTimeParsed, err := time.ParseInLocation(RecordSQLTimeFormat, endTime, loc)
	if err != nil {
		return fmt.Errorf("invalid end time format: %v, error: %v", endTime, err)
	}
	if startTimeParsed.IsZero() || endTimeParsed.IsZero() {
		return fmt.Errorf("start time or end time is zero")
	}
	if startTimeParsed.After(endTimeParsed) {
		return fmt.Errorf("start time %v is after end time %v", startTime, endTime)
	}
	if endTimeParsed.Before(time.Now()) {
		return fmt.Errorf("end time %v is in the past", endTime)
	}
	if outFile == "" {
		return fmt.Errorf("output file cannot be empty")
	}

	logPathAbs, err := filepath.Abs(logPath)
	if err != nil {
		return fmt.Errorf("error getting absolute path for log path %s: %v", logPath, err)
	}
	writeFile := path.Join(logPathAbs, outFile)
	// check if the output file is in the same directory as the log path
	absWriteFile, err := filepath.Abs(writeFile)
	if err != nil {
		return fmt.Errorf("error getting absolute path for output file %s: %v", writeFile, err)
	}
	outFileDir := path.Dir(absWriteFile)
	if outFileDir != logPathAbs {
		return fmt.Errorf("output file directory does not match log path, out dir: %s, log dir: %s", outFileDir, logPathAbs)
	}
	// create the output file
	f, err := os.Create(writeFile)
	if err != nil {
		return fmt.Errorf("error creating output file %s: %v", logPathAbs, err)
	}
	csvWriter := csv.NewWriter(f)
	ctx, cancel := context.WithDeadline(context.Background(), endTimeParsed)
	mission := &RecordMission{
		startTime:  startTimeParsed,
		endTime:    endTimeParsed,
		outFile:    f,
		csvWriter:  csvWriter,
		ctx:        ctx,
		cancelFunc: cancel,
		startChan:  make(chan struct{}, 1),
		list:       NewRecordList(),
		logger:     logger,
	}
	go mission.start()
	setGlobalRecordSql(mission)
	return nil
}

func StopRecordSql() {
	mission := getGlobalRecordMission()
	if mission == nil {
		return
	}
	mission.Stop()
}

func Enabled() bool {
	mission := getGlobalRecordMission()
	if mission == nil {
		return false
	}
	return mission.getEnabled()
}

func (c *RecordMission) getEnabled() bool {
	c.enableLock.RLock()
	defer c.enableLock.RUnlock()
	return c.enabled
}

func (c *RecordMission) setEnabled(enabled bool) {
	c.enableLock.Lock()
	defer c.enableLock.Unlock()
	c.enabled = enabled
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
	if duration <= 0 {
		c.setEnabled(true)
		go c.run()
		return
	}
	c.logger.Infof("start recording after %s", duration.String())
	timer := time.NewTimer(duration)
	defer func() {
		timer.Stop()
	}()
	select {
	case <-timer.C:
		c.setEnabled(true)
		go c.run()
		return
	case <-c.ctx.Done():
		return
	}
}

func (c *RecordMission) run() {
	c.logger.Infof("start recording sql")
	for {
		select {
		case <-c.ctx.Done():
			c.Stop()
			return
		}
	}
}

func (c *RecordMission) Stop() {
	c.closeOnce.Do(func() {
		// set flag false to stop
		c.setEnabled(false)
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
		// close file
		err := c.outFile.Close()
		if err != nil {
			c.logger.Errorf("error closing output file: %v", err)
		}
	})
}

func (c *RecordMission) writeRecord(record *Record) {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	record.Lock()
	defer record.Unlock()
	if len(record.SQL) == 0 {
		return
	}
	row := record.toRow()
	err := c.csvWriter.Write(row)
	if err != nil {
		c.logger.Errorf("error writing record to csv: %v", err)
	}
}

type RecordSQLType uint8

const (
	HTTPType RecordSQLType = 1
	WSType   RecordSQLType = 2
)

func (t RecordSQLType) String() string {
	switch t {
	case HTTPType:
		return "http"
	case WSType:
		return "ws"
	default:
		return strconv.Itoa(int(t))
	}
}

type Record struct {
	SQL             string         // SQL
	IP              string         // Client ip
	User            string         // Username
	Type            RecordSQLType  // ConnType (HTTP,WS)
	QID             uint64         // Query ID
	ReceiveTime     time.Time      // Receive time
	FreeTime        time.Time      // taos free time
	QueryDuration   time.Duration  // taos query interface duration
	FetchDuration   time.Duration  // taos fetch interface duration total
	GetConnDuration time.Duration  // taos get connection duration
	totalDuration   time.Duration  // total duration
	mission         *RecordMission // mission to which this record belongs
	ele             *list.Element  // element in the record list
	sync.Mutex                     // lock for thread safety
}

func (r *Record) Init(sql string, ip string, user string, typ RecordSQLType, qid uint64, receiveTime time.Time) {
	r.Lock()
	defer r.Unlock()
	r.SQL = sql
	r.IP = ip
	r.User = user
	r.Type = typ
	r.QID = qid
	r.ReceiveTime = receiveTime
}

func (r *Record) SetQueryDuration(duration time.Duration) {
	r.Lock()
	defer r.Unlock()
	r.QueryDuration = duration
}

func (r *Record) AddFetchDuration(duration time.Duration) {
	r.Lock()
	defer r.Unlock()
	r.FetchDuration += duration
}

func (r *Record) SetGetConnDuration(duration time.Duration) {
	r.Lock()
	defer r.Unlock()
	r.GetConnDuration = duration
}

func (r *Record) SetFreeTime(freeTime time.Time) {
	r.Lock()
	defer r.Unlock()
	r.FreeTime = freeTime
}

const (
	SQLIndex = iota
	IPIndex
	UserIndex
	TypeIndex
	QIDIndex
	ReceiveTimeIndex
	FreeTimeIndex
	QueryDurationIndex
	FetchDurationIndex
	GetConnDurationIndex
	TotalDurationIndex
)

func (r *Record) toRow() []string {
	row := make([]string, 11)
	row[SQLIndex] = r.SQL
	row[IPIndex] = r.IP
	row[UserIndex] = r.User
	row[TypeIndex] = r.Type.String()
	row[QIDIndex] = fmt.Sprintf("0x%x", r.QID)
	row[ReceiveTimeIndex] = r.ReceiveTime.Format(RecordSQLTimeFormat)
	row[FreeTimeIndex] = r.FreeTime.Format(RecordSQLTimeFormat)
	row[QueryDurationIndex] = strconv.FormatInt(r.QueryDuration.Microseconds(), 10)
	row[FetchDurationIndex] = strconv.FormatInt(r.FetchDuration.Microseconds(), 10)
	row[GetConnDurationIndex] = strconv.FormatInt(r.GetConnDuration.Microseconds(), 10)
	if !r.FreeTime.IsZero() {
		r.totalDuration = r.FreeTime.Sub(r.ReceiveTime)
	} else {
		r.totalDuration = time.Since(r.ReceiveTime)
	}
	row[TotalDurationIndex] = strconv.FormatInt(r.totalDuration.Microseconds(), 10)
	return row
}

func (r *Record) reset() {
	r.SQL = ""
	r.IP = ""
	r.User = ""
	r.Type = 0
	r.QID = 0
	r.ReceiveTime = time.Time{}
	r.FreeTime = time.Time{}
	r.QueryDuration = 0
	r.FetchDuration = 0
	r.GetConnDuration = 0
	r.totalDuration = 0
	r.ele = nil
	r.mission = nil
}

func (r *Record) write() {
	if r.mission != nil && r.mission.list != nil {
		val := r.mission.list.Remove(r.ele)
		if val != nil {
			r.mission.writeRecord(r)
		}
	}
}

var sqlRecordPool sync.Pool

func GetSQLRecord() *Record {
	mission := getGlobalRecordMission()
	var r *Record
	record := sqlRecordPool.Get()
	if record == nil {
		r = &Record{}
	} else {
		r = record.(*Record)
	}
	r.mission = mission
	if mission != nil {
		ele := mission.list.Add(r)
		r.ele = ele
	}
	return r
}

func PutSQLRecord(record *Record) {
	record.write()
	record.reset()
	sqlRecordPool.Put(record)
}

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
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/log"
)

type outputWriter interface {
	Write([]string) error
	Flush()
}
type RecordMission struct {
	running       bool
	runningLock   sync.RWMutex
	ctx           context.Context
	cancelFunc    context.CancelFunc
	maxConcurrent int32
	currentCount  atomic.Int32
	startTime     time.Time
	endTime       time.Time
	outFile       *os.File
	csvWriter     outputWriter
	startChan     chan struct{}
	writeLock     sync.Mutex
	list          *RecordList
	logger        *logrus.Entry
	closeOnce     sync.Once
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

const InputTimeFormat = "2006-01-02 15:04:05"

const ResultTimeFormat = "2006-01-02 15:04:05.000000"
const (
	SQLIndex = iota
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

var CsvHeader = []string{
	"SQL",
	"IP",
	"User",
	"ConnType",
	"QID",
	"ReceiveTime",
	"FreeTime",
	"QueryDuration(us)",
	"FetchDuration(us)",
	"GetConnDuration(us)",
	"TotalDuration(us)",
}

// record sql logger
var logger = log.GetLogger("RSQ")

func StartRecordSql(startTime, endTime string, logPath string, outFile string, location string, maxConcurrent int32) error {
	if IsRunning() {
		return fmt.Errorf("record sql is already running")
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
	if outFile == "" {
		return fmt.Errorf("output file cannot be empty")
	}

	logPathAbs, err := filepath.Abs(logPath)
	if err != nil {
		return fmt.Errorf("error getting absolute path for log path %s: %s", logPath, err)
	}
	writeFile := path.Join(logPathAbs, outFile)
	// check if the output file is in the same directory as the log path
	absWriteFile, err := filepath.Abs(writeFile)
	if err != nil {
		return fmt.Errorf("error getting absolute path for output file %s: %s", writeFile, err)
	}
	outFileDir := path.Dir(absWriteFile)
	if outFileDir != logPathAbs {
		return fmt.Errorf("output file directory does not match log path, out dir: %s, log dir: %s", outFileDir, logPathAbs)
	}
	// create the output file
	f, err := os.Create(writeFile)
	if err != nil {
		return fmt.Errorf("error creating output file %s: %s", logPathAbs, err)
	}
	csvWriter := csv.NewWriter(f)
	err = csvWriter.Write(CsvHeader)
	if err != nil {
		return fmt.Errorf("error writing csv header: %s", err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), endTimeParsed)
	mission := &RecordMission{
		startTime:     startTimeParsed,
		endTime:       endTimeParsed,
		outFile:       f,
		csvWriter:     csvWriter,
		ctx:           ctx,
		cancelFunc:    cancel,
		startChan:     make(chan struct{}, 1),
		list:          NewRecordList(),
		logger:        logger,
		maxConcurrent: maxConcurrent,
	}
	go mission.start()
	setGlobalRecordMission(mission)
	return nil
}

func StopRecordSql() {
	mission := getGlobalRecordMission()
	if mission == nil {
		return
	}
	mission.Stop()
	setGlobalRecordMission(nil)
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
	if duration <= 0 {
		c.setRunning(true)
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
		c.setRunning(true)
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
		// close file
		err := c.outFile.Close()
		if err != nil {
			c.logger.Errorf("error closing output file: %s", err)
		}
	})
}

func (c *RecordMission) writeRecord(record *Record) {
	c.currentCount.Add(-1)
	record.Lock()
	defer record.Unlock()
	if len(record.SQL) == 0 {
		return
	}
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	row := record.toRow()
	err := c.csvWriter.Write(row)
	if err != nil {
		c.logger.Errorf("error writing record to csv: %s", err)
	}
}

type ConnType uint8

const (
	HTTPType ConnType = 1
	WSType   ConnType = 2
)

func (t ConnType) String() string {
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
	ConnType        ConnType       // ConnType (HTTP,WS)
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

func (r *Record) Init(sql string, ip string, user string, connType ConnType, qid uint64, receiveTime time.Time) {
	r.Lock()
	defer r.Unlock()
	r.SQL = sql
	r.IP = ip
	r.User = user
	r.ConnType = connType
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

func (r *Record) toRow() []string {
	row := make([]string, 11)
	row[SQLIndex] = r.SQL
	row[IPIndex] = r.IP
	row[UserIndex] = r.User
	row[ConnTypeIndex] = r.ConnType.String()
	row[QIDIndex] = fmt.Sprintf("0x%x", r.QID)
	row[ReceiveTimeIndex] = r.ReceiveTime.Format(ResultTimeFormat)
	row[FreeTimeIndex] = r.FreeTime.Format(ResultTimeFormat)
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
	r.ConnType = 0
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

func GetSQLRecord() (record *Record, running bool) {
	mission := getGlobalRecordMission()
	if mission == nil {
		return nil, false
	}
	mission.runningLock.RLock()
	defer mission.runningLock.RUnlock()
	if !mission.running {
		return nil, false
	}
	if mission.maxConcurrent > 0 && mission.currentCount.Load() >= mission.maxConcurrent {
		return nil, false
	}
	cachedRecord := sqlRecordPool.Get()
	if cachedRecord == nil {
		record = &Record{}
	} else {
		record = cachedRecord.(*Record)
	}
	record.mission = mission
	mission.currentCount.Add(1)
	ele := mission.list.Add(record)
	record.ele = ele
	return record, true
}

func PutSQLRecord(record *Record) {
	record.write()
	record.reset()
	sqlRecordPool.Put(record)
}

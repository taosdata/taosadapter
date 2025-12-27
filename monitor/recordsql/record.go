package recordsql

import (
	"container/list"
	"fmt"
	"strconv"
	"sync"
	"time"
)

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
	SourcePort      string         // source port
	AppName         string         // application name
	totalDuration   time.Duration  // total duration
	mission         *RecordMission // mission to which this record belongs
	ele             *list.Element  // element in the record list
	sync.Mutex                     // lock for thread safety
}

func (r *Record) Init(sql string, ip string, port string, AppName string, user string, connType ConnType, qid uint64, receiveTime time.Time) {
	r.Lock()
	defer r.Unlock()
	r.SQL = sql
	r.IP = ip
	r.User = user
	r.ConnType = connType
	r.QID = qid
	r.ReceiveTime = receiveTime
	r.SourcePort = port
	r.AppName = AppName
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

func (r *Record) SetUser(user string) {
	r.Lock()
	defer r.Unlock()
	r.User = user
}

func (r *Record) toRow() []string {
	now := time.Now()
	row := make([]string, FiledCount)
	row[TSIndex] = now.Format(ResultTimeFormat)
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
		r.totalDuration = now.Sub(r.ReceiveTime)
	}
	row[TotalDurationIndex] = strconv.FormatInt(r.totalDuration.Microseconds(), 10)
	row[SourcePortIndex] = r.SourcePort
	row[AppNameIndex] = r.AppName
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
	r.SourcePort = ""
	r.AppName = ""
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
	logger.Tracef("start get sql record")
	mission := getGlobalRecordMission()
	if mission == nil {
		logger.Tracef("no record mission running")
		return nil, false
	}
	mission.runningLock.RLock()
	defer mission.runningLock.RUnlock()
	if !mission.running {
		logger.Tracef("record mission is not running")
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
	logger.Tracef("get sql record scuccess")
	return record, true
}

func PutSQLRecord(record *Record) {
	record.write()
	record.reset()
	sqlRecordPool.Put(record)
}

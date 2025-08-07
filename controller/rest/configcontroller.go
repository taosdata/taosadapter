package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor/recordsql"
)

type ConfigController struct {
}

func (ctl *ConfigController) Init(r gin.IRouter) {
	r.PUT("config", prepareCtx, checkConcurrent(changeConfigLocker), CheckAuth, checkTDengineConnection, ctl.changeConfig)
	r.POST("record_sql", prepareCtx, checkConcurrent(recordSqlLocker), CheckAuth, checkTDengineConnection, ctl.startRecordSql)
	r.DELETE("record_sql", prepareCtx, checkConcurrent(recordSqlLocker), CheckAuth, checkTDengineConnection, ctl.stopRecordSql)
	r.GET("record_sql", prepareCtx, CheckAuth, checkTDengineConnection, ctl.getRecordSqlState)
}

const (
	unlocked = 0
	locked   = 1
)

var changeConfigLocker = &locker{
	locking: unlocked,
}

var recordSqlLocker = &locker{
	locking: unlocked,
}

type locker struct {
	locking int32
}

func (l *locker) tryLock() bool {
	return atomic.CompareAndSwapInt32(&l.locking, unlocked, locked)
}

func (l *locker) unlock() {
	atomic.StoreInt32(&l.locking, unlocked)
}

type ModifyConfig struct {
	LogLevel *string `json:"log.level"`
}

type RecordSql struct {
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time"`
	Location  string `json:"location"`
}

func checkConcurrent(locker *locker) gin.HandlerFunc {
	return func(c *gin.Context) {
		logger := c.MustGet(LoggerKey).(*logrus.Entry)
		if !locker.tryLock() {
			TooManyRequestResponse(c, logger, fmt.Sprintf("API '%s' does not allow concurrent execution", c.FullPath()))
			return
		}
		c.Next()
		defer locker.unlock()
	}
}

func (ctl *ConfigController) changeConfig(c *gin.Context) {
	logger := c.MustGet(LoggerKey).(*logrus.Entry)
	body, err := c.GetRawData()
	if err != nil {
		logger.Errorf("get request body error, err:%s", err)
		BadRequestResponseWithMsg(c, logger, 0xffff, "get request body error")
		return
	}
	logger.Tracef("get modify config request, req:%s", body)
	var modifyConfig ModifyConfig
	err = json.Unmarshal(body, &modifyConfig)
	if err != nil {
		logger.Errorf("unmarshal json error, err:%s, req:%s", err, body)
		BadRequestResponseWithMsg(c, logger, 0xffff, "unmarshal json error")
		return
	}
	if modifyConfig.LogLevel != nil {
		logLevel := *modifyConfig.LogLevel
		logger.Debugf("change config, log.level:%s", logLevel)
		err = log.SetLevel(logLevel)
		if err != nil {
			logger.Errorf("change log.level error, err:%s", err)
			BadRequestResponseWithMsg(c, logger, 0xffff, "change log.level error")
			return
		}
	}
	c.JSON(http.StatusOK, &Message{
		Code: 0,
		Desc: "",
	})
	logger.Debugf("change config success")
}

func (ctl *ConfigController) startRecordSql(c *gin.Context) {
	logger := c.MustGet(LoggerKey).(*logrus.Entry)
	body, err := c.GetRawData()
	if err != nil {
		logger.Errorf("get request body error, err:%s", err)
		BadRequestResponseWithMsg(c, logger, 0xffff, "get request body error")
		return
	}
	logger.Tracef("get start record sql request, req:%s", body)
	var recordSql RecordSql
	if len(body) != 0 {
		err = json.Unmarshal(body, &recordSql)
		if err != nil {
			logger.Errorf("unmarshal json error, err:%s, req:%s", err, body)
			BadRequestResponseWithMsg(c, logger, 0xffff, "unmarshal json error")
			return
		}
	} else {
		logger.Debugf("no request body, use default record sql config")
		now := time.Now()
		microseconds := now.Nanosecond() / 1000
		formattedWithMicro := fmt.Sprintf(
			"%04d%02d%02d_%02d%02d%02d_%06d",
			now.Year(), now.Month(), now.Day(),
			now.Hour(), now.Minute(), now.Second(),
			microseconds,
		)
		recordFile := fmt.Sprintf("record_sql_%d_%s.csv", config.Conf.InstanceID, formattedWithMicro)
		logger.Debugf("use default record sql config %s", recordFile)
		recordSql = RecordSql{
			StartTime: now.Format(recordsql.InputTimeFormat),
			EndTime:   recordsql.DefaultRecordSqlEndTime,
			Location:  "",
		}
	}

	err = recordsql.StartRecordSql(
		recordSql.StartTime,
		recordSql.EndTime,
		recordSql.Location,
	)
	if err != nil {
		logger.Errorf("start record sql error, err:%s", err)
		BadRequestResponseWithMsg(c, logger, 0xffff, fmt.Sprintf("start record sql error: %s", err))
		return
	}
	c.JSON(http.StatusOK, &Message{
		Code: 0,
		Desc: "",
	})
	logger.Debugf("start record sql success")
}

type StopRecordSqlResp struct {
	Message
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time"`
}

func (ctl *ConfigController) stopRecordSql(c *gin.Context) {
	logger := c.MustGet(LoggerKey).(*logrus.Entry)
	logger.Debugf("get stop record sql request")
	info := recordsql.StopRecordSql()
	if info != nil {
		logger.Tracef("stop record sql info: %+v", info)
		c.JSON(http.StatusOK, &StopRecordSqlResp{
			StartTime: info.StartTime,
			EndTime:   info.EndTime,
		})
	} else {
		logger.Tracef("no record sql running")
		c.JSON(http.StatusOK, &Message{
			Code: 0,
			Desc: "",
		})
	}
	logger.Debugf("stop record sql success")
}

type GetRecordSqlStateResp struct {
	Message
	Exists            bool   `json:"exists"`
	Running           bool   `json:"running"`
	StartTime         string `json:"start_time"`
	EndTime           string `json:"end_time"`
	CurrentConcurrent int32  `json:"current_concurrent"`
}

func (ctl *ConfigController) getRecordSqlState(c *gin.Context) {
	logger := c.MustGet(LoggerKey).(*logrus.Entry)
	logger.Debugf("get record sql state request")
	state := recordsql.GetState()
	resp := &GetRecordSqlStateResp{
		Exists:            state.Exists,
		Running:           state.Running,
		StartTime:         state.StartTime,
		EndTime:           state.EndTime,
		CurrentConcurrent: state.CurrentConcurrent,
	}
	logger.Tracef("get record sql state: %+v", state)
	c.JSON(http.StatusOK, resp)
	logger.Debugf("get stop record sql success")
}

func init() {
	r := &ConfigController{}
	controller.AddController(r)
}

package monitor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/process"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/monitor/metrics"
	"github.com/taosdata/taosadapter/v3/thread"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/sqltype"
)

// rest
var (
	RestTotal *metrics.Gauge
	RestQuery *metrics.Gauge
	RestWrite *metrics.Gauge
	RestOther *metrics.Gauge

	RestSuccess      *metrics.Gauge
	RestQuerySuccess *metrics.Gauge
	RestWriteSuccess *metrics.Gauge
	RestOtherSuccess *metrics.Gauge

	RestFail      *metrics.Gauge
	RestQueryFail *metrics.Gauge
	RestWriteFail *metrics.Gauge
	RestOtherFail *metrics.Gauge

	RestInProcess      *metrics.Gauge
	RestQueryInProcess *metrics.Gauge
	RestWriteInProcess *metrics.Gauge
	RestOtherInProcess *metrics.Gauge
)

var (
	WSTotal *metrics.Gauge
	WSQuery *metrics.Gauge
	WSWrite *metrics.Gauge
	WSOther *metrics.Gauge

	WSSuccess      *metrics.Gauge
	WSQuerySuccess *metrics.Gauge
	WSWriteSuccess *metrics.Gauge
	WSOtherSuccess *metrics.Gauge

	WSFail      *metrics.Gauge
	WSQueryFail *metrics.Gauge
	WSWriteFail *metrics.Gauge
	WSOtherFail *metrics.Gauge

	WSInProcess      *metrics.Gauge
	WSQueryInProcess *metrics.Gauge
	WSWriteInProcess *metrics.Gauge
	WSOtherInProcess *metrics.Gauge
)

var (
	ConnPoolInUse sync.Map
	WSQueryConn   *metrics.Gauge
	WSSMLConn     *metrics.Gauge
	WSStmtConn    *metrics.Gauge
	WSWSConn      *metrics.Gauge
	WSTMQConn     *metrics.Gauge

	AsyncCInflight *metrics.Gauge
	SyncCInflight  *metrics.Gauge
)

var (
	TaosFreeResultCounter                               *metrics.Gauge
	TaosCloseCounter                                    *metrics.Gauge
	TaosSelectDBCounter                                 *metrics.Gauge
	TaosConnectCounter                                  *metrics.Gauge
	TaosGetTablesVgIDCounter                            *metrics.Gauge
	TaosStmtInitWithReqIDCounter                        *metrics.Gauge
	TaosStmtPrepareCounter                              *metrics.Gauge
	TaosStmtIsInsertCounter                             *metrics.Gauge
	TaosStmtSetTBNameCounter                            *metrics.Gauge
	TaosStmtGetTagFieldsCounter                         *metrics.Gauge
	TaosStmtReclaimFieldsCounter                        *metrics.Gauge
	TaosStmtSetTagsCounter                              *metrics.Gauge
	TaosStmtGetColFieldsCounter                         *metrics.Gauge
	TaosStmtBindParamBatchCounter                       *metrics.Gauge
	TaosStmtAddBatchCounter                             *metrics.Gauge
	TaosStmtExecuteCounter                              *metrics.Gauge
	TaosStmtCloseCounter                                *metrics.Gauge
	TMQWriteRawCounter                                  *metrics.Gauge
	TaosWriteRawBlockWithReqIDCounter                   *metrics.Gauge
	TaosWriteRawBlockWithFieldsWithReqIDCounter         *metrics.Gauge
	TaosGetCurrentDBCounter                             *metrics.Gauge
	TaosGetServerInfoCounter                            *metrics.Gauge
	TaosStmtNumParamsCounter                            *metrics.Gauge
	TaosStmtGetParamCounter                             *metrics.Gauge
	TaosSchemalessInsertRawTTLWithReqIDTBNameKeyCounter *metrics.Gauge
	TaosStmt2InitCounter                                *metrics.Gauge
	TaosStmt2PrepareCounter                             *metrics.Gauge
	TaosStmt2IsInsertCounter                            *metrics.Gauge
	TaosStmt2GetFieldsCounter                           *metrics.Gauge
	TaosStmt2ExecCounter                                *metrics.Gauge
	TaosStmt2CloseCounter                               *metrics.Gauge
	TaosStmt2BindBinaryCounter                          *metrics.Gauge
	TaosOptionsConnectionCounter                        *metrics.Gauge
	TaosValidateSqlCounter                              *metrics.Gauge
	TaosCheckServerStatusCounter                        *metrics.Gauge
	TMQSubscriptionCounter                              *metrics.Gauge
	TaosErrorStrCounter                                 *metrics.Gauge
	TaosIsUpdateQueryCounter                            *metrics.Gauge
	TaosErrorCounter                                    *metrics.Gauge
	TaosAffectedRowsCounter                             *metrics.Gauge
	TaosNumFieldsCounter                                *metrics.Gauge
	TaosFetchFieldsE                                    *metrics.Gauge
	TaosResultPrecisionCounter                          *metrics.Gauge
	TaosGetRawBlockCounter                              *metrics.Gauge
	TaosFetchRawBlockCounter                            *metrics.Gauge
	TaosQueryCounter                                    *metrics.Gauge
	TMQErr2StrCounter                                   *metrics.Gauge
	TMQConfSetCounter                                   *metrics.Gauge
	TaosFetchLengthsCounter                             *metrics.Gauge
	TaosStmtErrStrCounter                               *metrics.Gauge
	TaosStmtAffectedRowsOnceCounter                     *metrics.Gauge
	TaosStmt2FreeFieldsCounter                          *metrics.Gauge
	TaosStmt2ErrorCounter                               *metrics.Gauge
	TaosSetNotifyCBCounter                              *metrics.Gauge
	TMQListNewCounter                                   *metrics.Gauge
	TMQListDestroyCounter                               *metrics.Gauge
	TMQListAppendCounter                                *metrics.Gauge
	TMQGetResTypeCounter                                *metrics.Gauge
	TMQGetTopicNameCounter                              *metrics.Gauge
	TMQGetVgroupIDCounter                               *metrics.Gauge
	TMQGetVgroupOffsetCounter                           *metrics.Gauge
	TMQGetDBNameCounter                                 *metrics.Gauge
	TMQGetTableNameCounter                              *metrics.Gauge
	TMQListGetSizeCounter                               *metrics.Gauge
	TMQConfNewCounter                                   *metrics.Gauge
	TMQConfDestroyCounter                               *metrics.Gauge
	TMQGetConnectCounter                                *metrics.Gauge
	TMQFreeRawCounter                                   *metrics.Gauge
	TMQFreeJsonMetaCounter                              *metrics.Gauge
	TaosStmtUseResultCounter                            *metrics.Gauge
)

var (
	TaosFreeResultSuccessCounter                               *metrics.Gauge
	TaosCloseSuccessCounter                                    *metrics.Gauge
	TaosSelectDBSuccessCounter                                 *metrics.Gauge
	TaosConnectSuccessCounter                                  *metrics.Gauge
	TaosGetTablesVgIDSuccessCounter                            *metrics.Gauge
	TaosStmtInitWithReqIDSuccessCounter                        *metrics.Gauge
	TaosStmtPrepareSuccessCounter                              *metrics.Gauge
	TaosStmtIsInsertSuccessCounter                             *metrics.Gauge
	TaosStmtSetTBNameSuccessCounter                            *metrics.Gauge
	TaosStmtGetTagFieldsSuccessCounter                         *metrics.Gauge
	TaosStmtReclaimFieldsSuccessCounter                        *metrics.Gauge
	TaosStmtSetTagsSuccessCounter                              *metrics.Gauge
	TaosStmtGetColFieldsSuccessCounter                         *metrics.Gauge
	TaosStmtBindParamBatchSuccessCounter                       *metrics.Gauge
	TaosStmtAddBatchSuccessCounter                             *metrics.Gauge
	TaosStmtExecuteSuccessCounter                              *metrics.Gauge
	TaosStmtCloseSuccessCounter                                *metrics.Gauge
	TMQWriteRawSuccessCounter                                  *metrics.Gauge
	TaosWriteRawBlockWithReqIDSuccessCounter                   *metrics.Gauge
	TaosWriteRawBlockWithFieldsWithReqIDSuccessCounter         *metrics.Gauge
	TaosGetCurrentDBSuccessCounter                             *metrics.Gauge
	TaosGetServerInfoSuccessCounter                            *metrics.Gauge
	TaosStmtNumParamsSuccessCounter                            *metrics.Gauge
	TaosStmtGetParamSuccessCounter                             *metrics.Gauge
	TaosSchemalessInsertRawTTLWithReqIDTBNameKeySuccessCounter *metrics.Gauge
	TaosStmt2InitSuccessCounter                                *metrics.Gauge
	TaosStmt2PrepareSuccessCounter                             *metrics.Gauge
	TaosStmt2IsInsertSuccessCounter                            *metrics.Gauge
	TaosStmt2GetFieldsSuccessCounter                           *metrics.Gauge
	TaosStmt2ExecSuccessCounter                                *metrics.Gauge
	TaosStmt2CloseSuccessCounter                               *metrics.Gauge
	TaosStmt2BindBinarySuccessCounter                          *metrics.Gauge
	TaosOptionsConnectionSuccessCounter                        *metrics.Gauge
	TaosValidateSqlSuccessCounter                              *metrics.Gauge
	TaosCheckServerStatusSuccessCounter                        *metrics.Gauge
	TMQSubscriptionSuccessCounter                              *metrics.Gauge
	TaosErrorStrSuccessCounter                                 *metrics.Gauge
	TaosIsUpdateQuerySuccessCounter                            *metrics.Gauge
	TaosErrorSuccessCounter                                    *metrics.Gauge
	TaosAffectedRowsSuccessCounter                             *metrics.Gauge
	TaosNumFieldsSuccessCounter                                *metrics.Gauge
	TaosFetchFieldsESuccessCounter                             *metrics.Gauge
	TaosResultPrecisionSuccessCounter                          *metrics.Gauge
	TaosGetRawBlockSuccessCounter                              *metrics.Gauge
	TaosFetchRawBlockSuccessCounter                            *metrics.Gauge
	TaosQuerySuccessCounter                                    *metrics.Gauge
	TMQErr2StrSuccessCounter                                   *metrics.Gauge
	TMQConfSetSuccessCounter                                   *metrics.Gauge
	TaosFetchLengthsSuccessCounter                             *metrics.Gauge
	TaosStmtErrStrSuccessCounter                               *metrics.Gauge
	TaosStmtAffectedRowsOnceSuccessCounter                     *metrics.Gauge
	TaosStmt2FreeFieldsSuccessCounter                          *metrics.Gauge
	TaosStmt2ErrorSuccessCounter                               *metrics.Gauge
	TaosSetNotifyCBSuccessCounter                              *metrics.Gauge
	TMQListNewSuccessCounter                                   *metrics.Gauge
	TMQListDestroySuccessCounter                               *metrics.Gauge
	TMQListAppendSuccessCounter                                *metrics.Gauge
	TMQGetResTypeSuccessCounter                                *metrics.Gauge
	TMQGetTopicNameSuccessCounter                              *metrics.Gauge
	TMQGetVgroupIDSuccessCounter                               *metrics.Gauge
	TMQGetVgroupOffsetSuccessCounter                           *metrics.Gauge
	TMQGetDBNameSuccessCounter                                 *metrics.Gauge
	TMQGetTableNameSuccessCounter                              *metrics.Gauge
	TMQListGetSizeSuccessCounter                               *metrics.Gauge
	TMQConfNewSuccessCounter                                   *metrics.Gauge
	TMQConfDestroySuccessCounter                               *metrics.Gauge
	TMQGetConnectSuccessCounter                                *metrics.Gauge
	TMQFreeRawSuccessCounter                                   *metrics.Gauge
	TMQFreeJsonMetaSuccessCounter                              *metrics.Gauge
	TaosStmtUseResultSuccessCounter                            *metrics.Gauge
)

var (
	TaosFreeResultFailCounter                               *metrics.Gauge
	TaosCloseFailCounter                                    *metrics.Gauge
	TaosSelectDBFailCounter                                 *metrics.Gauge
	TaosConnectFailCounter                                  *metrics.Gauge
	TaosGetTablesVgIDFailCounter                            *metrics.Gauge
	TaosStmtInitWithReqIDFailCounter                        *metrics.Gauge
	TaosStmtPrepareFailCounter                              *metrics.Gauge
	TaosStmtIsInsertFailCounter                             *metrics.Gauge
	TaosStmtSetTBNameFailCounter                            *metrics.Gauge
	TaosStmtGetTagFieldsFailCounter                         *metrics.Gauge
	TaosStmtReclaimFieldsFailCounter                        *metrics.Gauge
	TaosStmtSetTagsFailCounter                              *metrics.Gauge
	TaosStmtGetColFieldsFailCounter                         *metrics.Gauge
	TaosStmtBindParamBatchFailCounter                       *metrics.Gauge
	TaosStmtAddBatchFailCounter                             *metrics.Gauge
	TaosStmtExecuteFailCounter                              *metrics.Gauge
	TaosStmtCloseFailCounter                                *metrics.Gauge
	TMQWriteRawFailCounter                                  *metrics.Gauge
	TaosWriteRawBlockWithReqIDFailCounter                   *metrics.Gauge
	TaosWriteRawBlockWithFieldsWithReqIDFailCounter         *metrics.Gauge
	TaosGetCurrentDBFailCounter                             *metrics.Gauge
	TaosGetServerInfoFailCounter                            *metrics.Gauge
	TaosStmtNumParamsFailCounter                            *metrics.Gauge
	TaosStmtGetParamFailCounter                             *metrics.Gauge
	TaosSchemalessInsertRawTTLWithReqIDTBNameKeyFailCounter *metrics.Gauge
	TaosStmt2InitFailCounter                                *metrics.Gauge
	TaosStmt2PrepareFailCounter                             *metrics.Gauge
	TaosStmt2IsInsertFailCounter                            *metrics.Gauge
	TaosStmt2GetFieldsFailCounter                           *metrics.Gauge
	TaosStmt2ExecFailCounter                                *metrics.Gauge
	TaosStmt2CloseFailCounter                               *metrics.Gauge
	TaosStmt2BindBinaryFailCounter                          *metrics.Gauge
	TaosOptionsConnectionFailCounter                        *metrics.Gauge
	TaosValidateSqlFailCounter                              *metrics.Gauge
	TaosCheckServerStatusFailCounter                        *metrics.Gauge
	TMQSubscriptionFailCounter                              *metrics.Gauge
	TaosErrorStrFailCounter                                 *metrics.Gauge
	TaosIsUpdateQueryFailCounter                            *metrics.Gauge
	TaosErrorFailCounter                                    *metrics.Gauge
	TaosAffectedRowsFailCounter                             *metrics.Gauge
	TaosFetchFieldsEFailCounter                             *metrics.Gauge
	TaosNumFieldsFailCounter                                *metrics.Gauge
	TaosResultPrecisionFailCounter                          *metrics.Gauge
	TaosGetRawBlockFailCounter                              *metrics.Gauge
	TaosFetchRawBlockFailCounter                            *metrics.Gauge
	TaosQueryFailCounter                                    *metrics.Gauge
	TMQErr2StrFailCounter                                   *metrics.Gauge
	TMQConfSetFailCounter                                   *metrics.Gauge
	TaosFetchLengthsFailCounter                             *metrics.Gauge
	TaosStmtErrStrFailCounter                               *metrics.Gauge
	TaosStmtAffectedRowsOnceFailCounter                     *metrics.Gauge
	TaosStmt2FreeFieldsFailCounter                          *metrics.Gauge
	TaosStmt2ErrorFailCounter                               *metrics.Gauge
	TaosSetNotifyCBFailCounter                              *metrics.Gauge
	TMQListNewFailCounter                                   *metrics.Gauge
	TMQListDestroyFailCounter                               *metrics.Gauge
	TMQListAppendFailCounter                                *metrics.Gauge
	TMQGetResTypeFailCounter                                *metrics.Gauge
	TMQGetTopicNameFailCounter                              *metrics.Gauge
	TMQGetVgroupIDFailCounter                               *metrics.Gauge
	TMQGetVgroupOffsetFailCounter                           *metrics.Gauge
	TMQGetDBNameFailCounter                                 *metrics.Gauge
	TMQGetTableNameFailCounter                              *metrics.Gauge
	TMQListGetSizeFailCounter                               *metrics.Gauge
	TMQConfNewFailCounter                                   *metrics.Gauge
	TMQConfDestroyFailCounter                               *metrics.Gauge
	TMQGetConnectFailCounter                                *metrics.Gauge
	TMQFreeRawFailCounter                                   *metrics.Gauge
	TMQFreeJsonMetaFailCounter                              *metrics.Gauge
	TaosStmtUseResultFailCounter                            *metrics.Gauge
)

func InitKeeper() {
	if config.Conf.UploadKeeper.Enable {
		// rest
		RestTotal = metrics.NewGauge("rest_total")
		RestQuery = metrics.NewGauge("rest_query")
		RestWrite = metrics.NewGauge("rest_write")
		RestOther = metrics.NewGauge("rest_other")

		RestSuccess = metrics.NewGauge("rest_success")
		RestQuerySuccess = metrics.NewGauge("rest_query_success")
		RestWriteSuccess = metrics.NewGauge("rest_write_success")
		RestOtherSuccess = metrics.NewGauge("rest_other_success")

		RestFail = metrics.NewGauge("rest_fail")
		RestQueryFail = metrics.NewGauge("rest_query_fail")
		RestWriteFail = metrics.NewGauge("rest_write_fail")
		RestOtherFail = metrics.NewGauge("rest_other_fail")

		RestInProcess = metrics.NewGauge("rest_in_process")
		RestQueryInProcess = metrics.NewGauge("rest_query_in_process")
		RestWriteInProcess = metrics.NewGauge("rest_write_in_process")
		RestOtherInProcess = metrics.NewGauge("rest_other_in_process")

		// ws
		WSTotal = metrics.NewGauge("ws_total")
		WSQuery = metrics.NewGauge("ws_query")
		WSWrite = metrics.NewGauge("ws_write")
		WSOther = metrics.NewGauge("ws_other")

		WSSuccess = metrics.NewGauge("ws_success")
		WSQuerySuccess = metrics.NewGauge("ws_query_success")
		WSWriteSuccess = metrics.NewGauge("ws_write_success")
		WSOtherSuccess = metrics.NewGauge("ws_other_success")

		WSFail = metrics.NewGauge("ws_fail")
		WSQueryFail = metrics.NewGauge("ws_query_fail")
		WSWriteFail = metrics.NewGauge("ws_write_fail")
		WSOtherFail = metrics.NewGauge("ws_other_fail")

		WSInProcess = metrics.NewGauge("ws_in_process")
		WSQueryInProcess = metrics.NewGauge("ws_query_in_process")
		WSWriteInProcess = metrics.NewGauge("ws_write_in_process")
		WSOtherInProcess = metrics.NewGauge("ws_other_in_process")

		recordMetrics = append(recordMetrics,
			RestTotal,
			RestQuery,
			RestWrite,
			RestOther,

			RestSuccess,
			RestQuerySuccess,
			RestWriteSuccess,
			RestOtherSuccess,

			RestFail,
			RestQueryFail,
			RestWriteFail,
			RestOtherFail,

			WSTotal,
			WSQuery,
			WSWrite,
			WSOther,

			WSSuccess,
			WSQuerySuccess,
			WSWriteSuccess,
			WSOtherSuccess,

			WSFail,
			WSQueryFail,
			WSWriteFail,
			WSOtherFail,
		)
		inflightMetrics = append(inflightMetrics,
			RestInProcess,
			RestQueryInProcess,
			RestWriteInProcess,
			RestOtherInProcess,

			WSInProcess,
			WSQueryInProcess,
			WSWriteInProcess,
			WSOtherInProcess,
		)

		WSQueryConn = metrics.NewGauge("ws_query_conn")
		WSSMLConn = metrics.NewGauge("ws_sml_conn")
		WSStmtConn = metrics.NewGauge("ws_stmt_conn")
		WSWSConn = metrics.NewGauge("ws_ws_conn")
		WSTMQConn = metrics.NewGauge("ws_tmq_conn")
		AsyncCInflight = metrics.NewGauge("async_c_inflight")
		SyncCInflight = metrics.NewGauge("sync_c_inflight")

		thread.AsyncSemaphore.SetGauge(AsyncCInflight)
		thread.SyncSemaphore.SetGauge(SyncCInflight)
	}
}

var recordMetrics []*metrics.Gauge
var inflightMetrics []*metrics.Gauge

func RestRecordRequest(sql string) sqltype.SqlType {
	if config.Conf.UploadKeeper.Enable {
		RestTotal.Inc()
		RestInProcess.Inc()
		sqlType := sqltype.GetSqlType(sql)
		switch sqlType {
		case sqltype.InsertType:
			RestWrite.Inc()
			RestWriteInProcess.Inc()
		case sqltype.SelectType:
			RestQuery.Inc()
			RestQueryInProcess.Inc()
		default:
			RestOther.Inc()
			RestOtherInProcess.Inc()
		}
		return sqlType
	}
	return sqltype.OtherType
}

func RestRecordResult(sqlType sqltype.SqlType, success bool) {
	if config.Conf.UploadKeeper.Enable {
		RestInProcess.Dec()
		if success {
			RestSuccess.Inc()
		} else {
			RestFail.Inc()
		}
		switch sqlType {
		case sqltype.InsertType:
			RestWriteInProcess.Dec()
			if success {
				RestWriteSuccess.Inc()
			} else {
				RestWriteFail.Inc()
			}
		case sqltype.SelectType:
			RestQueryInProcess.Dec()
			if success {
				RestQuerySuccess.Inc()
			} else {
				RestQueryFail.Inc()
			}
		default:
			RestOtherInProcess.Dec()
			if success {
				RestOtherSuccess.Inc()
			} else {
				RestOtherFail.Inc()
			}
		}
	}
}

func WSRecordRequest(sql string) sqltype.SqlType {
	if config.Conf.UploadKeeper.Enable {
		WSTotal.Inc()
		WSInProcess.Inc()
		sqlType := sqltype.GetSqlType(sql)
		switch sqlType {
		case sqltype.InsertType:
			WSWrite.Inc()
			WSWriteInProcess.Inc()
		case sqltype.SelectType:
			WSQuery.Inc()
			WSQueryInProcess.Inc()
		default:
			WSOther.Inc()
			WSOtherInProcess.Inc()
		}
		return sqlType
	}
	return sqltype.OtherType
}

func WSRecordResult(sqlType sqltype.SqlType, success bool) {
	if config.Conf.UploadKeeper.Enable {
		WSInProcess.Dec()
		if success {
			WSSuccess.Inc()
		} else {
			WSFail.Inc()
		}
		switch sqlType {
		case sqltype.InsertType:
			WSWriteInProcess.Dec()
			if success {
				WSWriteSuccess.Inc()
			} else {
				WSWriteFail.Inc()
			}
		case sqltype.SelectType:
			WSQueryInProcess.Dec()
			if success {
				WSQuerySuccess.Inc()
			} else {
				WSQueryFail.Inc()
			}
		default:
			WSOtherInProcess.Dec()
			if success {
				WSOtherSuccess.Inc()
			} else {
				WSOtherFail.Inc()
			}
		}
	}
}

func RecordNewConnectionPool(userName string) *metrics.Gauge {
	if config.Conf.UploadKeeper.Enable {
		gauge := metrics.NewGauge(fmt.Sprintf("conn_pool_in_use_%s", userName))
		ConnPoolInUse.Store(userName, gauge)
		return gauge
	}
	return nil
}

func RecordWSQueryConn() {
	if config.Conf.UploadKeeper.Enable {
		WSQueryConn.Inc()
	}
}

func RecordWSQueryDisconnect() {
	if config.Conf.UploadKeeper.Enable {
		WSQueryConn.Dec()
	}
}

func RecordWSSMLConn() {
	if config.Conf.UploadKeeper.Enable {
		WSSMLConn.Inc()
	}
}

func RecordWSSMLDisconnect() {
	if config.Conf.UploadKeeper.Enable {
		WSSMLConn.Dec()
	}
}

func RecordWSStmtConn() {
	if config.Conf.UploadKeeper.Enable {
		WSStmtConn.Inc()
	}
}

func RecordWSStmtDisconnect() {
	if config.Conf.UploadKeeper.Enable {
		WSStmtConn.Dec()
	}
}

func RecordWSWSConn() {
	if config.Conf.UploadKeeper.Enable {
		WSWSConn.Inc()
	}
}

func RecordWSWSDisconnect() {
	if config.Conf.UploadKeeper.Enable {
		WSWSConn.Dec()
	}
}

func RecordWSTMQConn() {
	if config.Conf.UploadKeeper.Enable {
		WSTMQConn.Inc()
	}
}

func RecordWSTMQDisconnect() {
	if config.Conf.UploadKeeper.Enable {
		WSTMQConn.Dec()
	}
}

func StartUpload() {
	if config.Conf.UploadKeeper.Enable {
		client := &http.Client{
			Timeout: config.Conf.UploadKeeper.Timeout,
		}
		p, err := process.NewProcess(int32(os.Getpid()))
		if err != nil {
			logger.Panicf("get process error, err:%s", err)
		}
		go func() {
			nextUploadTime := getNextUploadTime()
			logger.Debugf("start upload keeper when %s", nextUploadTime.Format("2006-01-02 15:04:05.000000000"))
			startTimer := time.NewTimer(time.Until(nextUploadTime))
			<-startTimer.C
			startTimer.Stop()
			go func() {
				reqID := generator.GetUploadKeeperReqID()
				err := upload(p, client, reqID)
				if err != nil {
					logger.Errorf("upload_id:0x%x, upload to keeper error, err:%s", reqID, err)
				}
			}()
			ticker := time.NewTicker(config.Conf.UploadKeeper.Interval)
			for range ticker.C {
				go func() {
					reqID := generator.GetUploadKeeperReqID()
					err := upload(p, client, reqID)
					if err != nil {
						logger.Errorf("upload_id:0x%x, upload to keeper error, err:%s", reqID, err)
					}
				}()
			}
		}()
	}
}

func getNextUploadTime() time.Time {
	now := time.Now()
	next := now.Round(config.Conf.UploadKeeper.Interval)
	if next.Before(now) {
		next = next.Add(config.Conf.UploadKeeper.Interval)
	}
	return next
}

type UploadData struct {
	Ts           int64          `json:"ts"`
	Metrics      map[string]int `json:"metrics"`
	Endpoint     string         `json:"endpoint"`
	ExtraMetrics []*ExtraMetric `json:"extra_metrics"`
}

type ExtraMetric struct {
	Ts       string   `json:"ts"`
	Protocol int      `json:"protocol"`
	Tables   []*Table `json:"tables"`
}

type Table struct {
	Name         string         `json:"name"`
	MetricGroups []*MetricGroup `json:"metric_groups"`
}

type MetricGroup struct {
	Tags    []*Tag    `json:"tags"`
	Metrics []*Metric `json:"metrics"`
}

type Tag struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Metric struct {
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
}

func upload(p *process.Process, client *http.Client, reqID int64) error {
	ts := time.Now().Round(config.Conf.UploadKeeper.Interval)
	extraMetric, err := generateExtraMetrics(ts, p)
	if err != nil {
		logger.Errorf("generate extra metrics error, err:%s", err)
		return err
	}
	data := UploadData{
		Ts:           ts.Unix(),
		Metrics:      make(map[string]int, len(recordMetrics)+len(inflightMetrics)),
		Endpoint:     identity,
		ExtraMetrics: extraMetric,
	}
	for _, metric := range recordMetrics {
		value := metric.Value()
		data.Metrics[metric.MetricName()] = int(value)
		metric.Sub(value)
	}
	for _, metric := range inflightMetrics {
		data.Metrics[metric.MetricName()] = int(metric.Value())
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	err = doRequest(client, jsonData, reqID)
	if err != nil {
		for i := 0; i < int(config.Conf.UploadKeeper.RetryTimes); i++ {
			logger.Debugf("upload_id:0x%x, upload to keeper error, will retry in %s", reqID, config.Conf.UploadKeeper.RetryInterval)
			time.Sleep(config.Conf.UploadKeeper.RetryInterval)
			logger.Debugf("upload_id:0x%x, retry upload to keeper, retry times:%d", reqID, i+1)
			err = doRequest(client, jsonData, reqID)
			if err == nil {
				return nil
			}
		}
	}
	return err
}

func generateExtraMetrics(ts time.Time, p *process.Process) ([]*ExtraMetric, error) {
	memState, err := p.MemoryInfo()
	if err != nil {
		return nil, fmt.Errorf("get memory info error, err:%s", err.Error())
	}
	memStats := new(runtime.MemStats)
	runtime.ReadMemStats(memStats)
	statusTable := &Table{
		Name: "adapter_status",
		MetricGroups: []*MetricGroup{
			{
				Tags: []*Tag{
					{
						Name:  "endpoint",
						Value: identity,
					},
				},
				Metrics: []*Metric{
					{
						Name:  "go_heap_sys",
						Value: memStats.HeapSys,
					},
					{
						Name:  "go_heap_inuse",
						Value: memStats.HeapInuse,
					},
					{
						Name:  "go_stack_sys",
						Value: memStats.StackSys,
					},
					{
						Name:  "go_stack_inuse",
						Value: memStats.StackInuse,
					},
					{
						Name:  "rss",
						Value: memState.RSS,
					},
					{
						Name:  "ws_query_conn",
						Value: WSQueryConn.Value(),
					},
					{
						Name:  "ws_stmt_conn",
						Value: WSStmtConn.Value(),
					},
					{
						Name:  "ws_sml_conn",
						Value: WSSMLConn.Value(),
					},
					{
						Name:  "ws_ws_conn",
						Value: WSWSConn.Value(),
					},
					{
						Name:  "ws_tmq_conn",
						Value: WSTMQConn.Value(),
					},
					{
						Name:  "async_c_limit",
						Value: config.Conf.MaxAsyncMethodLimit,
					},
					{
						Name:  "async_c_inflight",
						Value: AsyncCInflight.Value(),
					},
					{
						Name:  "sync_c_limit",
						Value: config.Conf.MaxSyncMethodLimit,
					},
					{
						Name:  "sync_c_inflight",
						Value: SyncCInflight.Value(),
					},
				},
			},
		},
	}
	connTable := &Table{
		Name: "adapter_conn_pool",
	}
	ConnPoolInUse.Range(func(k, v interface{}) bool {
		connTable.MetricGroups = append(connTable.MetricGroups, &MetricGroup{
			Tags: []*Tag{
				{
					Name:  "endpoint",
					Value: identity,
				},
				{
					Name:  "user",
					Value: k.(string),
				},
			},
			Metrics: []*Metric{
				{
					Name:  "conn_pool_total",
					Value: config.Conf.Pool.MaxConnect,
				},
				{
					Name:  "conn_pool_in_use",
					Value: v.(*metrics.Gauge).Value(),
				},
			},
		})
		return true
	})
	metric := &ExtraMetric{
		Ts:       strconv.FormatInt(ts.UnixMilli(), 10),
		Protocol: 2,
		Tables: []*Table{
			statusTable,
		},
	}
	if len(connTable.MetricGroups) > 0 {
		metric.Tables = append(metric.Tables, connTable)
	}
	return []*ExtraMetric{metric}, nil
}
func doRequest(client *http.Client, data []byte, reqID int64) error {
	req, err := http.NewRequest(http.MethodPost, config.Conf.UploadKeeper.Url, bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("create new request error: %s", err)
	}
	req.Header.Set("X-QID", fmt.Sprintf("0x%x", reqID))
	logger.Tracef("upload_id:0x%x, upload to keeper, url:%s, data:%s", reqID, config.Conf.UploadKeeper.Url, data)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	logger.Debugf("upload_id:0x%x, upload to keeper success", reqID)
	if resp.StatusCode != http.StatusOK {
		logger.Errorf("upload_id:0x%x, upload keeper error, code: %d", reqID, resp.StatusCode)
		return fmt.Errorf("upload keeper error, code: %d", resp.StatusCode)
	}
	return nil
}

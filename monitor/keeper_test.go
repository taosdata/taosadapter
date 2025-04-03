package monitor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/shirou/gopsutil/v3/process"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/thread"
	"github.com/taosdata/taosadapter/v3/tools/sqltype"
)

func TestRecordRequest(t *testing.T) {
	// Mock the configuration to enable monitoring
	config.Conf.UploadKeeper.Enable = true
	InitKeeper()

	// Test with an INSERT SQL statement
	sql := "INSERT INTO table_name VALUES (1, 'data')"
	result := RestRecordRequest(sql)
	if result != sqltype.InsertType {
		t.Errorf("Expected InsertType, got %v", result)
	}
	if RestTotal.Value() != 1.0 {
		t.Errorf("Expected RestTotal to be 1.0, got %f", RestTotal.Value())
	}
	if RestInProcess.Value() != 1.0 {
		t.Errorf("Expected RestInProcess to be 1.0, got %f", RestInProcess.Value())
	}
	if RestWrite.Value() != 1.0 {
		t.Errorf("Expected RestWrite to be 1.0, got %f", RestWrite.Value())
	}
	if RestWriteInProcess.Value() != 1.0 {
		t.Errorf("Expected RestWriteInProcess to be 1.0, got %f", RestWriteInProcess.Value())
	}

	// Test with a SELECT SQL statement
	sql = "SELECT * FROM table_name"
	result = RestRecordRequest(sql)
	if result != sqltype.SelectType {
		t.Errorf("Expected SelectType, got %v", result)
	}
	if RestTotal.Value() != 2.0 {
		t.Errorf("Expected RestTotal to be 2.0, got %f", RestTotal.Value())
	}
	if RestInProcess.Value() != 2.0 {
		t.Errorf("Expected RestInProcess to be 2.0, got %f", RestInProcess.Value())
	}
	if RestQuery.Value() != 1.0 {
		t.Errorf("Expected RestQuery to be 1.0, got %f", RestQuery.Value())
	}
	if RestQueryInProcess.Value() != 1.0 {
		t.Errorf("Expected RestQueryInProcess to be 1.0, got %f", RestQueryInProcess.Value())
	}

	// Test with an unsupported SQL statement
	sql = "UPDATE table_name SET column = 'value'"
	result = RestRecordRequest(sql)
	if result != sqltype.OtherType {
		t.Errorf("Expected OtherType, got %v", result)
	}
	if RestTotal.Value() != 3.0 {
		t.Errorf("Expected RestTotal to be 3.0, got %f", RestTotal.Value())
	}
	if RestInProcess.Value() != 3.0 {
		t.Errorf("Expected RestInProcess to be 3.0, got %f", RestInProcess.Value())
	}
	if RestOther.Value() != 1.0 {
		t.Errorf("Expected RestOther to be 1.0, got %f", RestOther.Value())
	}
	if RestOtherInProcess.Value() != 1.0 {
		t.Errorf("Expected RestOtherInProcess to be 1.0, got %f", RestOtherInProcess.Value())
	}

	// Reset the configuration
	config.Conf.UploadKeeper.Enable = false
}

func TestRecordResult(t *testing.T) {
	// Mock the configuration to enable monitoring
	config.Conf.UploadKeeper.Enable = true
	InitKeeper()
	// Test a successful result
	sqlType := RestRecordRequest("INSERT INTO table_name VALUES (1, 'data')")
	success := true
	RestRecordResult(sqlType, success)
	if RestSuccess.Value() != 1.0 {
		t.Errorf("Expected RestSuccess to be 1.0, got %f", RestSuccess.Value())
	}
	if RestInProcess.Value() != 0.0 {
		t.Errorf("Expected RestInProcess to be 0.0, got %f", RestInProcess.Value())
	}
	if RestWriteInProcess.Value() != 0.0 {
		t.Errorf("Expected RestWriteInProcess to be 0.0, got %f", RestWriteInProcess.Value())
	}

	// Test a failed result
	sqlType = RestRecordRequest("SELECT * FROM table_name")
	success = false
	RestRecordResult(sqlType, success)
	if RestFail.Value() != 1.0 {
		t.Errorf("Expected RestFail to be 1.0, got %f", RestFail.Value())
	}
	if RestInProcess.Value() != 0.0 {
		t.Errorf("Expected RestInProcess to be 0.0, got %f", RestInProcess.Value())
	}
	if RestQueryInProcess.Value() != 0.0 {
		t.Errorf("Expected RestQueryInProcess to be 0.0, got %f", RestQueryInProcess.Value())
	}

	// Reset the configuration
	config.Conf.UploadKeeper.Enable = false
}

func TestWSRecordRequest(t *testing.T) {
	// Mock the configuration to enable monitoring
	config.Conf.UploadKeeper.Enable = true
	InitKeeper()

	// Test with an INSERT SQL statement
	sql := "INSERT INTO table_name VALUES (1, 'data')"
	result := WSRecordRequest(sql)
	if result != sqltype.InsertType {
		t.Errorf("Expected InsertType, got %v", result)
	}
	if WSTotal.Value() != 1.0 {
		t.Errorf("Expected WSTotal to be 1.0, got %f", WSTotal.Value())
	}
	if WSInProcess.Value() != 1.0 {
		t.Errorf("Expected WSInProcess to be 1.0, got %f", WSInProcess.Value())
	}
	if WSWrite.Value() != 1.0 {
		t.Errorf("Expected WSWrite to be 1.0, got %f", WSWrite.Value())
	}
	if WSWriteInProcess.Value() != 1.0 {
		t.Errorf("Expected WSWriteInProcess to be 1.0, got %f", WSWriteInProcess.Value())
	}

	// Test with a SELECT SQL statement
	sql = "SELECT * FROM table_name"
	result = WSRecordRequest(sql)
	if result != sqltype.SelectType {
		t.Errorf("Expected SelectType, got %v", result)
	}
	if WSTotal.Value() != 2.0 {
		t.Errorf("Expected WSTotal to be 2.0, got %f", WSTotal.Value())
	}
	if WSInProcess.Value() != 2.0 {
		t.Errorf("Expected WSInProcess to be 2.0, got %f", WSInProcess.Value())
	}
	if WSQuery.Value() != 1.0 {
		t.Errorf("Expected WSQuery to be 1.0, got %f", WSQuery.Value())
	}
	if WSQueryInProcess.Value() != 1.0 {
		t.Errorf("Expected WSQueryInProcess to be 1.0, got %f", WSQueryInProcess.Value())
	}

	// Test with an unsupported SQL statement
	sql = "UPDATE table_name SET column = 'value'"
	result = WSRecordRequest(sql)
	if result != sqltype.OtherType {
		t.Errorf("Expected OtherType, got %v", result)
	}
	if WSTotal.Value() != 3.0 {
		t.Errorf("Expected WSTotal to be 3.0, got %f", WSTotal.Value())
	}
	if WSInProcess.Value() != 3.0 {
		t.Errorf("Expected WSInProcess to be 3.0, got %f", WSInProcess.Value())
	}
	if WSOther.Value() != 1.0 {
		t.Errorf("Expected WSOther to be 1.0, got %f", WSOther.Value())
	}
	if WSOtherInProcess.Value() != 1.0 {
		t.Errorf("Expected WSOtherInProcess to be 1.0, got %f", WSOtherInProcess.Value())
	}

	// Reset the configuration
	config.Conf.UploadKeeper.Enable = false
}

func TestWSRecordResult(t *testing.T) {
	// Mock the configuration to enable monitoring
	config.Conf.UploadKeeper.Enable = true
	InitKeeper()
	// Test a successful result
	sqlType := WSRecordRequest("INSERT INTO table_name VALUES (1, 'data')")
	success := true
	WSRecordResult(sqlType, success)
	if WSSuccess.Value() != 1.0 {
		t.Errorf("Expected WSSuccess to be 1.0, got %f", WSSuccess.Value())
	}
	if WSInProcess.Value() != 0.0 {
		t.Errorf("Expected WSInProcess to be 0.0, got %f", WSInProcess.Value())
	}
	if WSWriteInProcess.Value() != 0.0 {
		t.Errorf("Expected WSWriteInProcess to be 0.0, got %f", WSWriteInProcess.Value())
	}

	// Test a failed result
	sqlType = WSRecordRequest("SELECT * FROM table_name")
	success = false
	WSRecordResult(sqlType, success)
	if WSFail.Value() != 1.0 {
		t.Errorf("Expected WSFail to be 1.0, got %f", WSFail.Value())
	}
	if WSInProcess.Value() != 0.0 {
		t.Errorf("Expected WSInProcess to be 0.0, got %f", WSInProcess.Value())
	}
	if WSQueryInProcess.Value() != 0.0 {
		t.Errorf("Expected WSQueryInProcess to be 0.0, got %f", WSQueryInProcess.Value())
	}

	// Reset the configuration
	config.Conf.UploadKeeper.Enable = false
}

func TestUpload(t *testing.T) {
	config.Conf.UploadKeeper.Enable = true
	InitKeeper()
	times := 0
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/upload" {
			b, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			t.Log(string(b))
			times += 1
			if times == 1 {
				w.WriteHeader(http.StatusRequestTimeout)
				return
			}
			close(done)
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()
	config.Conf.UploadKeeper.Url = server.URL + "/upload"
	config.Conf.UploadKeeper.Interval = time.Second
	config.Conf.UploadKeeper.RetryTimes = 1
	config.Conf.UploadKeeper.RetryInterval = time.Millisecond * 100
	StartUpload()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	select {
	case <-ctx.Done():
		t.Errorf("upload failed")
	case <-done:
	}
	config.Conf.UploadKeeper.Enable = false
}

func TestRecordWSConn(t *testing.T) {
	config.Conf.UploadKeeper.Enable = true
	InitKeeper()
	RecordWSQueryConn()
	assert.Equal(t, float64(1), WSQueryConn.Value())
	RecordWSQueryDisconnect()
	assert.Equal(t, float64(0), WSQueryConn.Value())
	RecordWSSMLConn()
	assert.Equal(t, float64(1), WSSMLConn.Value())
	RecordWSSMLDisconnect()
	assert.Equal(t, float64(0), WSSMLConn.Value())
	RecordWSStmtConn()
	assert.Equal(t, float64(1), WSStmtConn.Value())
	RecordWSStmtDisconnect()
	assert.Equal(t, float64(0), WSStmtConn.Value())
	RecordWSWSConn()
	assert.Equal(t, float64(1), WSWSConn.Value())
	RecordWSWSDisconnect()
	assert.Equal(t, float64(0), WSWSConn.Value())
	RecordWSTMQConn()
	assert.Equal(t, float64(1), WSTMQConn.Value())
	RecordWSTMQDisconnect()
	assert.Equal(t, float64(0), WSTMQConn.Value())
	config.Conf.UploadKeeper.Enable = false
}

func TestGenerateExtraMetrics(t *testing.T) {
	config.Conf.UploadKeeper.Enable = true
	InitKeeper()
	ts := time.Now()
	p, err := process.NewProcess(int32(os.Getpid()))
	assert.NoError(t, err)
	RecordWSQueryConn()
	RecordWSSMLConn()
	RecordWSStmtConn()
	RecordWSWSConn()
	RecordWSTMQConn()
	thread.AsyncLocker.Lock()
	thread.SyncLocker.Lock()
	g := RecordNewConnectionPool("test")
	g.Inc()
	metrics, err := generateExtraMetrics(ts, p)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(metrics))
	metric := metrics[0]
	assert.Equal(t, ts.UnixMilli(), metric.Ts)
	assert.Equal(t, 2, metric.Protocol)
	assert.Equal(t, 2, len(metric.Tables))
	statusTable := metric.Tables[0]
	assert.Equal(t, "taosadapter_status", statusTable.Name)
	assert.Equal(t, 1, len(statusTable.MetricGroups))
	statusMetricGroup := statusTable.MetricGroups[0]
	assert.Equal(t, 1, len(statusMetricGroup.Tags))
	assert.Equal(t, "endpoint", statusMetricGroup.Tags[0].Name)
	assert.Equal(t, identity, statusMetricGroup.Tags[0].Value)
	statusMetric := statusMetricGroup.Metrics
	assert.Equal(t, 14, len(statusMetric))
	assert.Equal(t, "go_heap_sys", statusMetric[0].Name)
	assert.Greater(t, statusMetric[0].Value, uint64(0))
	assert.Equal(t, "go_heap_inuse", statusMetric[1].Name)
	assert.Less(t, statusMetric[1].Value, statusMetric[0].Value)
	assert.Equal(t, "go_stack_sys", statusMetric[2].Name)
	assert.Greater(t, statusMetric[2].Value, uint64(0))
	assert.Equal(t, "go_stack_inuse", statusMetric[3].Name)
	assert.LessOrEqual(t, statusMetric[3].Value, statusMetric[2].Value)
	assert.Equal(t, "rss", statusMetric[4].Name)
	assert.Greater(t, statusMetric[4].Value, statusMetric[0].Value.(uint64)+statusMetric[2].Value.(uint64))
	assert.Equal(t, "ws_query_conn", statusMetric[5].Name)
	assert.Equal(t, float64(1), statusMetric[5].Value)
	assert.Equal(t, "ws_stmt_conn", statusMetric[6].Name)
	assert.Equal(t, float64(1), statusMetric[6].Value)
	assert.Equal(t, "ws_sml_conn", statusMetric[7].Name)
	assert.Equal(t, float64(1), statusMetric[7].Value)
	assert.Equal(t, "ws_ws_conn", statusMetric[8].Name)
	assert.Equal(t, float64(1), statusMetric[8].Value)
	assert.Equal(t, "ws_tmq_conn", statusMetric[9].Name)
	assert.Equal(t, float64(1), statusMetric[9].Value)
	assert.Equal(t, "async_c_limit", statusMetric[10].Name)
	assert.Equal(t, config.Conf.MaxAsyncMethodLimit, statusMetric[10].Value)
	assert.Equal(t, "async_c_inflight", statusMetric[11].Name)
	assert.Equal(t, float64(1), statusMetric[11].Value)
	assert.Equal(t, "sync_c_limit", statusMetric[12].Name)
	assert.Equal(t, config.Conf.MaxSyncMethodLimit, statusMetric[12].Value)
	assert.Equal(t, "sync_c_inflight", statusMetric[13].Name)
	assert.Equal(t, float64(1), statusMetric[13].Value)
	connPoolTable := metric.Tables[1]
	assert.Equal(t, "taosadapter_conn_pool", connPoolTable.Name)
	assert.Equal(t, 1, len(connPoolTable.MetricGroups))
	assert.Equal(t, 2, len(connPoolTable.MetricGroups[0].Tags))
	assert.Equal(t, "endpoint", connPoolTable.MetricGroups[0].Tags[0].Name)
	assert.Equal(t, identity, connPoolTable.MetricGroups[0].Tags[0].Value)
	assert.Equal(t, "user", connPoolTable.MetricGroups[0].Tags[1].Name)
	assert.Equal(t, "test", connPoolTable.MetricGroups[0].Tags[1].Value)
	assert.Equal(t, 2, len(connPoolTable.MetricGroups[0].Metrics))
	assert.Equal(t, "conn_pool_total", connPoolTable.MetricGroups[0].Metrics[0].Name)
	assert.Equal(t, config.Conf.Pool.MaxConnect, connPoolTable.MetricGroups[0].Metrics[0].Value)
	assert.Equal(t, "conn_pool_in_use", connPoolTable.MetricGroups[0].Metrics[1].Name)
	assert.Equal(t, float64(1), connPoolTable.MetricGroups[0].Metrics[1].Value)

	RecordWSQueryDisconnect()
	RecordWSSMLDisconnect()
	RecordWSStmtDisconnect()
	RecordWSWSDisconnect()
	RecordWSTMQDisconnect()
	thread.AsyncLocker.Unlock()
	thread.SyncLocker.Unlock()
	g = RecordNewConnectionPool("test")
	g.Inc()
	g.Inc()

	metrics, err = generateExtraMetrics(ts, p)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(metrics))
	metric = metrics[0]
	assert.Equal(t, ts.UnixMilli(), metric.Ts)
	assert.Equal(t, 2, metric.Protocol)
	assert.Equal(t, 2, len(metric.Tables))
	statusTable = metric.Tables[0]
	assert.Equal(t, "taosadapter_status", statusTable.Name)
	assert.Equal(t, 1, len(statusTable.MetricGroups))
	statusMetricGroup = statusTable.MetricGroups[0]
	assert.Equal(t, 1, len(statusMetricGroup.Tags))
	assert.Equal(t, "endpoint", statusMetricGroup.Tags[0].Name)
	assert.Equal(t, identity, statusMetricGroup.Tags[0].Value)
	statusMetric = statusMetricGroup.Metrics
	assert.Equal(t, 14, len(statusMetric))
	assert.Equal(t, "go_heap_sys", statusMetric[0].Name)
	assert.Greater(t, statusMetric[0].Value, uint64(0))
	assert.Equal(t, "go_heap_inuse", statusMetric[1].Name)
	assert.Less(t, statusMetric[1].Value, statusMetric[0].Value)
	assert.Equal(t, "go_stack_sys", statusMetric[2].Name)
	assert.Greater(t, statusMetric[2].Value, uint64(0))
	assert.Equal(t, "go_stack_inuse", statusMetric[3].Name)
	assert.LessOrEqual(t, statusMetric[3].Value, statusMetric[2].Value)
	assert.Equal(t, "rss", statusMetric[4].Name)
	assert.Greater(t, statusMetric[4].Value, statusMetric[0].Value.(uint64)+statusMetric[2].Value.(uint64))
	assert.Equal(t, "ws_query_conn", statusMetric[5].Name)
	assert.Equal(t, float64(0), statusMetric[5].Value)
	assert.Equal(t, "ws_stmt_conn", statusMetric[6].Name)
	assert.Equal(t, float64(0), statusMetric[6].Value)
	assert.Equal(t, "ws_sml_conn", statusMetric[7].Name)
	assert.Equal(t, float64(0), statusMetric[7].Value)
	assert.Equal(t, "ws_ws_conn", statusMetric[8].Name)
	assert.Equal(t, float64(0), statusMetric[8].Value)
	assert.Equal(t, "ws_tmq_conn", statusMetric[9].Name)
	assert.Equal(t, float64(0), statusMetric[9].Value)
	assert.Equal(t, "async_c_limit", statusMetric[10].Name)
	assert.Equal(t, config.Conf.MaxAsyncMethodLimit, statusMetric[10].Value)
	assert.Equal(t, "async_c_inflight", statusMetric[11].Name)
	assert.Equal(t, float64(0), statusMetric[11].Value)
	assert.Equal(t, "sync_c_limit", statusMetric[12].Name)
	assert.Equal(t, config.Conf.MaxSyncMethodLimit, statusMetric[12].Value)
	assert.Equal(t, "sync_c_inflight", statusMetric[13].Name)
	assert.Equal(t, float64(0), statusMetric[13].Value)
	connPoolTable = metric.Tables[1]
	assert.Equal(t, "taosadapter_conn_pool", connPoolTable.Name)
	assert.Equal(t, 1, len(connPoolTable.MetricGroups))
	assert.Equal(t, 2, len(connPoolTable.MetricGroups[0].Tags))
	assert.Equal(t, "endpoint", connPoolTable.MetricGroups[0].Tags[0].Name)
	assert.Equal(t, identity, connPoolTable.MetricGroups[0].Tags[0].Value)
	assert.Equal(t, "user", connPoolTable.MetricGroups[0].Tags[1].Name)
	assert.Equal(t, "test", connPoolTable.MetricGroups[0].Tags[1].Value)
	assert.Equal(t, 2, len(connPoolTable.MetricGroups[0].Metrics))
	assert.Equal(t, "conn_pool_total", connPoolTable.MetricGroups[0].Metrics[0].Name)
	assert.Equal(t, config.Conf.Pool.MaxConnect, connPoolTable.MetricGroups[0].Metrics[0].Value)
	assert.Equal(t, "conn_pool_in_use", connPoolTable.MetricGroups[0].Metrics[1].Name)
	assert.Equal(t, float64(2), connPoolTable.MetricGroups[0].Metrics[1].Value)

	config.Conf.UploadKeeper.Enable = false
}

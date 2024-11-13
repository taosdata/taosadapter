package monitor_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/tools/sqltype"
)

func TestRecordRequest(t *testing.T) {
	// Mock the configuration to enable monitoring
	config.Conf.UploadKeeper.Enable = true
	monitor.InitKeeper()

	// Test with an INSERT SQL statement
	sql := "INSERT INTO table_name VALUES (1, 'data')"
	result := monitor.RestRecordRequest(sql)
	if result != sqltype.InsertType {
		t.Errorf("Expected InsertType, got %v", result)
	}
	if monitor.RestTotal.Value() != 1.0 {
		t.Errorf("Expected RestTotal to be 1.0, got %f", monitor.RestTotal.Value())
	}
	if monitor.RestInProcess.Value() != 1.0 {
		t.Errorf("Expected RestInProcess to be 1.0, got %f", monitor.RestInProcess.Value())
	}
	if monitor.RestWrite.Value() != 1.0 {
		t.Errorf("Expected RestWrite to be 1.0, got %f", monitor.RestWrite.Value())
	}
	if monitor.RestWriteInProcess.Value() != 1.0 {
		t.Errorf("Expected RestWriteInProcess to be 1.0, got %f", monitor.RestWriteInProcess.Value())
	}

	// Test with a SELECT SQL statement
	sql = "SELECT * FROM table_name"
	result = monitor.RestRecordRequest(sql)
	if result != sqltype.SelectType {
		t.Errorf("Expected SelectType, got %v", result)
	}
	if monitor.RestTotal.Value() != 2.0 {
		t.Errorf("Expected RestTotal to be 2.0, got %f", monitor.RestTotal.Value())
	}
	if monitor.RestInProcess.Value() != 2.0 {
		t.Errorf("Expected RestInProcess to be 2.0, got %f", monitor.RestInProcess.Value())
	}
	if monitor.RestQuery.Value() != 1.0 {
		t.Errorf("Expected RestQuery to be 1.0, got %f", monitor.RestQuery.Value())
	}
	if monitor.RestQueryInProcess.Value() != 1.0 {
		t.Errorf("Expected RestQueryInProcess to be 1.0, got %f", monitor.RestQueryInProcess.Value())
	}

	// Test with an unsupported SQL statement
	sql = "UPDATE table_name SET column = 'value'"
	result = monitor.RestRecordRequest(sql)
	if result != sqltype.OtherType {
		t.Errorf("Expected OtherType, got %v", result)
	}
	if monitor.RestTotal.Value() != 3.0 {
		t.Errorf("Expected RestTotal to be 3.0, got %f", monitor.RestTotal.Value())
	}
	if monitor.RestInProcess.Value() != 3.0 {
		t.Errorf("Expected RestInProcess to be 3.0, got %f", monitor.RestInProcess.Value())
	}
	if monitor.RestOther.Value() != 1.0 {
		t.Errorf("Expected RestOther to be 1.0, got %f", monitor.RestOther.Value())
	}
	if monitor.RestOtherInProcess.Value() != 1.0 {
		t.Errorf("Expected RestOtherInProcess to be 1.0, got %f", monitor.RestOtherInProcess.Value())
	}

	// Reset the configuration
	config.Conf.UploadKeeper.Enable = false
}

func TestRecordResult(t *testing.T) {
	// Mock the configuration to enable monitoring
	config.Conf.UploadKeeper.Enable = true
	monitor.InitKeeper()
	// Test a successful result
	sqlType := monitor.RestRecordRequest("INSERT INTO table_name VALUES (1, 'data')")
	success := true
	monitor.RestRecordResult(sqlType, success)
	if monitor.RestSuccess.Value() != 1.0 {
		t.Errorf("Expected RestSuccess to be 1.0, got %f", monitor.RestSuccess.Value())
	}
	if monitor.RestInProcess.Value() != 0.0 {
		t.Errorf("Expected RestInProcess to be 0.0, got %f", monitor.RestInProcess.Value())
	}
	if monitor.RestWriteInProcess.Value() != 0.0 {
		t.Errorf("Expected RestWriteInProcess to be 0.0, got %f", monitor.RestWriteInProcess.Value())
	}

	// Test a failed result
	sqlType = monitor.RestRecordRequest("SELECT * FROM table_name")
	success = false
	monitor.RestRecordResult(sqlType, success)
	if monitor.RestFail.Value() != 1.0 {
		t.Errorf("Expected RestFail to be 1.0, got %f", monitor.RestFail.Value())
	}
	if monitor.RestInProcess.Value() != 0.0 {
		t.Errorf("Expected RestInProcess to be 0.0, got %f", monitor.RestInProcess.Value())
	}
	if monitor.RestQueryInProcess.Value() != 0.0 {
		t.Errorf("Expected RestQueryInProcess to be 0.0, got %f", monitor.RestQueryInProcess.Value())
	}

	// Reset the configuration
	config.Conf.UploadKeeper.Enable = false
}

func TestWSRecordRequest(t *testing.T) {
	// Mock the configuration to enable monitoring
	config.Conf.UploadKeeper.Enable = true
	monitor.InitKeeper()

	// Test with an INSERT SQL statement
	sql := "INSERT INTO table_name VALUES (1, 'data')"
	result := monitor.WSRecordRequest(sql)
	if result != sqltype.InsertType {
		t.Errorf("Expected InsertType, got %v", result)
	}
	if monitor.WSTotal.Value() != 1.0 {
		t.Errorf("Expected WSTotal to be 1.0, got %f", monitor.WSTotal.Value())
	}
	if monitor.WSInProcess.Value() != 1.0 {
		t.Errorf("Expected WSInProcess to be 1.0, got %f", monitor.WSInProcess.Value())
	}
	if monitor.WSWrite.Value() != 1.0 {
		t.Errorf("Expected WSWrite to be 1.0, got %f", monitor.WSWrite.Value())
	}
	if monitor.WSWriteInProcess.Value() != 1.0 {
		t.Errorf("Expected WSWriteInProcess to be 1.0, got %f", monitor.WSWriteInProcess.Value())
	}

	// Test with a SELECT SQL statement
	sql = "SELECT * FROM table_name"
	result = monitor.WSRecordRequest(sql)
	if result != sqltype.SelectType {
		t.Errorf("Expected SelectType, got %v", result)
	}
	if monitor.WSTotal.Value() != 2.0 {
		t.Errorf("Expected WSTotal to be 2.0, got %f", monitor.WSTotal.Value())
	}
	if monitor.WSInProcess.Value() != 2.0 {
		t.Errorf("Expected WSInProcess to be 2.0, got %f", monitor.WSInProcess.Value())
	}
	if monitor.WSQuery.Value() != 1.0 {
		t.Errorf("Expected WSQuery to be 1.0, got %f", monitor.WSQuery.Value())
	}
	if monitor.WSQueryInProcess.Value() != 1.0 {
		t.Errorf("Expected WSQueryInProcess to be 1.0, got %f", monitor.WSQueryInProcess.Value())
	}

	// Test with an unsupported SQL statement
	sql = "UPDATE table_name SET column = 'value'"
	result = monitor.WSRecordRequest(sql)
	if result != sqltype.OtherType {
		t.Errorf("Expected OtherType, got %v", result)
	}
	if monitor.WSTotal.Value() != 3.0 {
		t.Errorf("Expected WSTotal to be 3.0, got %f", monitor.WSTotal.Value())
	}
	if monitor.WSInProcess.Value() != 3.0 {
		t.Errorf("Expected WSInProcess to be 3.0, got %f", monitor.WSInProcess.Value())
	}
	if monitor.WSOther.Value() != 1.0 {
		t.Errorf("Expected WSOther to be 1.0, got %f", monitor.WSOther.Value())
	}
	if monitor.WSOtherInProcess.Value() != 1.0 {
		t.Errorf("Expected WSOtherInProcess to be 1.0, got %f", monitor.WSOtherInProcess.Value())
	}

	// Reset the configuration
	config.Conf.UploadKeeper.Enable = false
}

func TestWSRecordResult(t *testing.T) {
	// Mock the configuration to enable monitoring
	config.Conf.UploadKeeper.Enable = true
	monitor.InitKeeper()
	// Test a successful result
	sqlType := monitor.WSRecordRequest("INSERT INTO table_name VALUES (1, 'data')")
	success := true
	monitor.WSRecordResult(sqlType, success)
	if monitor.WSSuccess.Value() != 1.0 {
		t.Errorf("Expected WSSuccess to be 1.0, got %f", monitor.WSSuccess.Value())
	}
	if monitor.WSInProcess.Value() != 0.0 {
		t.Errorf("Expected WSInProcess to be 0.0, got %f", monitor.WSInProcess.Value())
	}
	if monitor.WSWriteInProcess.Value() != 0.0 {
		t.Errorf("Expected WSWriteInProcess to be 0.0, got %f", monitor.WSWriteInProcess.Value())
	}

	// Test a failed result
	sqlType = monitor.WSRecordRequest("SELECT * FROM table_name")
	success = false
	monitor.WSRecordResult(sqlType, success)
	if monitor.WSFail.Value() != 1.0 {
		t.Errorf("Expected WSFail to be 1.0, got %f", monitor.WSFail.Value())
	}
	if monitor.WSInProcess.Value() != 0.0 {
		t.Errorf("Expected WSInProcess to be 0.0, got %f", monitor.WSInProcess.Value())
	}
	if monitor.WSQueryInProcess.Value() != 0.0 {
		t.Errorf("Expected WSQueryInProcess to be 0.0, got %f", monitor.WSQueryInProcess.Value())
	}

	// Reset the configuration
	config.Conf.UploadKeeper.Enable = false
}

func TestUpload(t *testing.T) {
	config.Conf.UploadKeeper.Enable = true
	monitor.InitKeeper()
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
	monitor.StartUpload()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	select {
	case <-ctx.Done():
		t.Errorf("upload failed")
	case <-done:
	}
	config.Conf.UploadKeeper.Enable = false
}

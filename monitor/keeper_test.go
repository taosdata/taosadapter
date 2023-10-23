package monitor

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/taosdata/taosadapter/v3/config"
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
			b, _ := ioutil.ReadAll(r.Body)
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

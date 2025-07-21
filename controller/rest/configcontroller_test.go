package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor/recordsql"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
	"github.com/taosdata/taosadapter/v3/version"
)

func TestChangeConfig(t *testing.T) {
	baseLevel := log.GetLogLevel().String()
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPut, "/config", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code, w.Body.String())

	// wrong password
	user := "root"
	password := "wrong"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPut, "/config", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code, w.Body.String())

	// whitelist error
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPut, "/config", nil)
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 403, w.Code, w.Body.String())

	// no body
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body := strings.NewReader("")
	req, _ = http.NewRequest(http.MethodPut, "/config", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code, w.Body.String())

	// wrong json
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body = strings.NewReader("xxx")
	req, _ = http.NewRequest(http.MethodPut, "/config", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code, w.Body.String())

	// wrong log.level
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body = strings.NewReader(`{"log.level":"wrong"}`)
	req, _ = http.NewRequest(http.MethodPut, "/config", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code, w.Body.String())

	// success
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()

	body = strings.NewReader(`{"log.level":"warn"}`)
	req, _ = http.NewRequest(http.MethodPut, "/config", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
	assert.Equal(t, logrus.WarnLevel, log.GetLogLevel())
	t.Log(log.GetLogLevel())

	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`{"log.level":"%s"}`, baseLevel))
	req, _ = http.NewRequest(http.MethodPut, "/config", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
}

func TestRecordSql(t *testing.T) {
	//log.SetLevel("debug")
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/record_sql", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code, w.Body.String())

	// wrong password
	user := "root"
	password := "wrong"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPost, "/record_sql", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code, w.Body.String())

	// whitelist error
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPost, "/record_sql", nil)
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 403, w.Code, w.Body.String())

	// wrong json
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body := strings.NewReader("xxx")
	req, _ = http.NewRequest(http.MethodPost, "/record_sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code, w.Body.String())

	// wrong config
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body = strings.NewReader(`{"start_time":"xxx"}`)
	req, _ = http.NewRequest(http.MethodPost, "/record_sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code, w.Body.String())

	// get record sql state
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, "/record_sql", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	t.Log(w.Body.String())
	assert.Equal(t, `{"code":0,"desc":"","exists":false,"running":false,"start_time":"","end_time":"","current_concurrent":0}`, w.Body.String())

	// stop record sql
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodDelete, "/record_sql", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
	assert.Equal(t, `{"code":0,"desc":""}`, w.Body.String())

	// start record sql with start time and end time
	tmpDir := t.TempDir()
	oldPath := config.Conf.Log.Path
	config.Conf.Log.Path = tmpDir
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	start := time.Now().Format(recordsql.InputTimeFormat)
	end := time.Now().Add(time.Minute).Format(recordsql.InputTimeFormat)
	body = strings.NewReader(fmt.Sprintf(`{"start_time":"%s","end_time":"%s"}`, start, end))
	req, _ = http.NewRequest(http.MethodPost, "/record_sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
	config.Conf.Log.Path = oldPath

	time.Sleep(time.Millisecond * 10)

	// execute sql
	sqlRequestAddr := testtools.GetRandomRemoteAddr()
	host, _, err := net.SplitHostPort(strings.TrimSpace(sqlRequestAddr))
	assert.NoError(t, err)
	w = httptest.NewRecorder()
	body = strings.NewReader("show databases")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = sqlRequestAddr
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	// get record sql state
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, "/record_sql", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	t.Log(w.Body.String())
	var stateResp GetRecordSqlStateResp
	err = json.Unmarshal(w.Body.Bytes(), &stateResp)
	assert.NoError(t, err)
	assert.Equal(t, start, stateResp.StartTime)
	assert.Equal(t, end, stateResp.EndTime)

	// stop record sql
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodDelete, "/record_sql", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
	var stopResp StopRecordSqlResp
	err = json.Unmarshal(w.Body.Bytes(), &stopResp)
	assert.NoError(t, err)
	assert.Equal(t, stopResp.StartTime, start)
	assert.Equal(t, stopResp.EndTime, end)

	// check record sql file
	files, err := getRecordFiles(tmpDir)
	require.NoError(t, err)
	require.Equal(t, 1, len(files))
	t.Log(files)
	recordFile := filepath.Join(tmpDir, files[0])
	recordContent, err := os.ReadFile(recordFile)
	assert.NoError(t, err)
	expect := fmt.Sprintf("show databases,%s,root,http,", host)
	t.Log(string(recordContent))
	t.Log(expect)
	assert.True(t, bytes.Contains(recordContent, []byte(expect)))

	// start record sql without start time and end time
	config.Conf.Log.Path = tmpDir
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body = strings.NewReader("")
	req, _ = http.NewRequest(http.MethodPost, "/record_sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
	config.Conf.Log.Path = oldPath
	t.Log(w.Body.String())

	time.Sleep(time.Millisecond * 10)
	// execute sql
	sqlRequestAddr = testtools.GetRandomRemoteAddr()
	host, _, err = net.SplitHostPort(strings.TrimSpace(sqlRequestAddr))
	assert.NoError(t, err)
	w = httptest.NewRecorder()
	body = strings.NewReader("show databases")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = sqlRequestAddr
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create database if not exists test_record_sql")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = sqlRequestAddr
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	// wrong sql
	w = httptest.NewRecorder()
	body = strings.NewReader("xxxx")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = sqlRequestAddr
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	// get record sql state
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, "/record_sql", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	t.Log(w.Body.String())
	err = json.Unmarshal(w.Body.Bytes(), &stateResp)
	assert.NoError(t, err)
	assert.Equal(t, recordsql.DefaultRecordFileEndTime, stateResp.EndTime)

	// stop record sql
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodDelete, "/record_sql", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
	t.Log(w.Body.String())
	err = json.Unmarshal(w.Body.Bytes(), &stopResp)
	assert.NoError(t, err)
	assert.Equal(t, stateResp.StartTime, stopResp.StartTime)
	assert.Equal(t, recordsql.DefaultRecordFileEndTime, stopResp.EndTime)

	files, err = getRecordFiles(tmpDir)
	require.NoError(t, err)
	require.Equal(t, 2, len(files))
	t.Log(files)
	recordFile = ""
	for _, file := range files {
		if !strings.HasSuffix(file, ".csv") {
			recordFile = file
		}
	}
	require.True(t, recordFile != "")
	recordFile = filepath.Join(tmpDir, recordFile)
	recordContent, err = os.ReadFile(recordFile)
	assert.NoError(t, err)
	expect = fmt.Sprintf("show databases,%s,root,http,", host)
	t.Log(string(recordContent))
	t.Log(expect)
	assert.True(t, bytes.Contains(recordContent, []byte(expect)))
	expect = fmt.Sprintf("create database if not exists test_record_sql,%s,root,http,", host)
	t.Log(expect)
	assert.True(t, bytes.Contains(recordContent, []byte(expect)))
	expect = fmt.Sprintf("xxxx,%s,root,http,", host)
	t.Log(expect)
	assert.True(t, bytes.Contains(recordContent, []byte(expect)))
}

func getRecordFiles(dir string) ([]string, error) {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.IsDir() && strings.HasPrefix(info.Name(), fmt.Sprintf("%sadapterSql_", version.CUS_PROMPT)) && !strings.HasSuffix(info.Name(), "_lock") {
			files = append(files, info.Name())
		}
		return nil
	})
	return files, err
}

package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
)

var router *gin.Engine

func TestMain(m *testing.M) {
	viper.Set("pool.maxConnect", 10000)
	viper.Set("pool.maxIdle", 10000)
	viper.Set("logLevel", "debug")
	config.Init()
	db.PrepareConnection()
	gin.SetMode(gin.ReleaseMode)
	router = gin.New()
	router.Use(func(context *gin.Context) {
		context.Set("currentID", uint32(0))
	})
	var ctl Restful
	ctl.Init(router)
	m.Run()
}

func BenchmarkRestful(b *testing.B) {
	w := httptest.NewRecorder()
	for i := 0; i < b.N; i++ {
		body := strings.NewReader("show databases")
		req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
		router.ServeHTTP(w, req)
		assert.Equal(b, 200, w.Code)
	}
}

// @author: xftan
// @date: 2021/12/14 15:10
// @description: test restful sql
func TestSql(t *testing.T) {
	w := httptest.NewRecorder()
	body := strings.NewReader("show databases")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql/log", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

// @author: xftan
// @date: 2021/12/14 15:11
// @description: test restful login
func TestLogin(t *testing.T) {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/rest/login/root/password", nil)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	req, _ = http.NewRequest(http.MethodGet, "/rest/login/root111111111111111111111111111/password", nil)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

// @author: xftan
// @date: 2021/12/14 15:11
// @description: test restful wrong sql
func TestWrongSql(t *testing.T) {
	w := httptest.NewRecorder()
	body := strings.NewReader("wrong sql")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql/log", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

// @author: xftan
// @date: 2021/12/14 15:11
// @description: test restful no sql
func TestNoSql(t *testing.T) {
	w := httptest.NewRecorder()
	body := strings.NewReader("")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql/log", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

// @author: xftan
// @date: 2022/1/10 15:15
// @description: test restful all type query
func TestAllType(t *testing.T) {
	now := time.Now().Local().UnixNano() / 1e6
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists test_alltype")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists alltype(ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20)) tags (info json)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_alltype", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into t1 using alltype tags('{"table":"t1"}') values (%d,true,2,3,4,5,6,7,8,9,10.123,11.123,'中文"binary','中文nchar')`, now))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_alltype", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`select alltype.*,info->'table' from alltype where ts = %d`, now))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_alltype", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	var result TDEngineRestfulRespDoc
	err := json.Unmarshal(w.Body.Bytes(), &result)
	assert.NoError(t, err)
	assert.Equal(t, 0, result.Code)
	assert.Equal(t, true, result.Data[0][1])
	assert.Equal(t, float64(2), result.Data[0][2])
	assert.Equal(t, float64(3), result.Data[0][3])
	assert.Equal(t, float64(4), result.Data[0][4])
	assert.Equal(t, float64(5), result.Data[0][5])
	assert.Equal(t, float64(6), result.Data[0][6])
	assert.Equal(t, float64(7), result.Data[0][7])
	assert.Equal(t, float64(8), result.Data[0][8])
	assert.Equal(t, float64(9), result.Data[0][9])
	assert.Equal(t, float64(10.123), result.Data[0][10])
	assert.Equal(t, float64(11.123), result.Data[0][11])
	assert.Equal(t, "中文\"binary", result.Data[0][12])
	assert.Equal(t, "中文nchar", result.Data[0][13])
	assert.Equal(t, map[string]interface{}{"table": "t1"}, result.Data[0][14])
	assert.Equal(t, "t1", result.Data[0][15])
	w = httptest.NewRecorder()
	body = strings.NewReader("drop database if exists test_alltype")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

// @author: xftan
// @date: 2022/1/18 18:12
// @description: test restful row limit
func TestRowLimit(t *testing.T) {
	config.Conf.RestfulRowLimit = 1
	now := time.Now().Local().UnixNano() / 1e6
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists test_rowlimit")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists test_rowlimit(ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20)) tags (info json)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_rowlimit", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into t1 using test_rowlimit tags('{"table":"t1"}') values (%d,true,2,3,4,5,6,7,8,9,10,11,'中文"binary','中文nchar')(%d,false,12,13,14,15,16,17,18,19,110,111,'中文"binary','中文nchar')`, now, now+1))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_rowlimit", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader(`select test_rowlimit.*,info->'table' from test_rowlimit`)
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_rowlimit", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	var result TDEngineRestfulRespDoc
	err := json.Unmarshal(w.Body.Bytes(), &result)
	assert.NoError(t, err)
	assert.Equal(t, 0, result.Code)
	assert.Equal(t, 1, len(result.Data))
	assert.Equal(t, true, result.Data[0][1])
	assert.Equal(t, float64(2), result.Data[0][2])
	assert.Equal(t, float64(3), result.Data[0][3])
	assert.Equal(t, float64(4), result.Data[0][4])
	assert.Equal(t, float64(5), result.Data[0][5])
	assert.Equal(t, float64(6), result.Data[0][6])
	assert.Equal(t, float64(7), result.Data[0][7])
	assert.Equal(t, float64(8), result.Data[0][8])
	assert.Equal(t, float64(9), result.Data[0][9])
	assert.Equal(t, float64(10), result.Data[0][10])
	assert.Equal(t, float64(11), result.Data[0][11])
	assert.Equal(t, "中文\"binary", result.Data[0][12])
	assert.Equal(t, "中文nchar", result.Data[0][13])
	assert.Equal(t, map[string]interface{}{"table": "t1"}, result.Data[0][14])
	assert.Equal(t, "t1", result.Data[0][15])
	config.Conf.RestfulRowLimit = -1
	w = httptest.NewRecorder()
	body = strings.NewReader(`drop database if exists test_rowlimit`)
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

func TestUpload(t *testing.T) {
	data := `2022-08-30 11:45:30.754,123,123.123000000,"abcd,abcd"
2022-08-30 11:45:40.871,123,123.123000000,"a""bcd,abcd"
2022-08-30 11:45:47.039,123,123.123000000,"abcd"",""abcd"
2022-08-30 11:47:22.607,123,123.123000000,"abcd"",""abcd",""""
2022-08-30 11:52:40.548,123,123.123000000,"abcd','abcd"
2022-08-30 11:52:40.549,123,123.123000000,
`
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists test_upload")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists test_upload.t2(ts timestamp,n1 int,n2 double,n3 binary(30))")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	payload := &bytes.Buffer{}
	writer := multipart.NewWriter(payload)
	part1, _ := writer.CreateFormFile("data", filepath.Base("sql.csv"))
	_, err := io.Copy(part1, strings.NewReader(data))
	if err != nil {
		t.Error(err)
		return
	}
	err = writer.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	req, _ = http.NewRequest(http.MethodPost, "/rest/upload?db=test_upload&table=t2&batch=10", payload)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	req.Header.Set("Content-Type", writer.FormDataContentType())
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("select n1,n2,n3 from test_upload.t2")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	var result TDEngineRestfulRespDoc
	err = json.Unmarshal(w.Body.Bytes(), &result)
	assert.NoError(t, err)
	assert.Equal(t, 0, result.Code)
	assert.Equal(t, 6, len(result.Data))
	for i := 0; i < 6; i++ {
		result.Data[i][0] = float64(123)
		result.Data[i][1] = float64(123.123)
	}
	result.Data[0][2] = "abcd,abcd"
	result.Data[1][2] = "a\"bcd,abcd"
	result.Data[2][2] = "abcd\",\"abcd"
	result.Data[3][2] = "abcd\",\"abcd"
	result.Data[4][2] = "abcd','abcd"
	result.Data[5][2] = nil

	w = httptest.NewRecorder()
	body = strings.NewReader(`drop database if exists test_upload`)
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

func TestPrecision(t *testing.T) {
	config.Conf.RestfulRowLimit = -1
	now := time.Now().Unix()
	nowT := time.Unix(now, 0)
	{
		w := httptest.NewRecorder()
		body := strings.NewReader("create database if not exists test_pc_ms precision 'ms'")
		req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		w = httptest.NewRecorder()
		body = strings.NewReader("create table if not exists t1(ts timestamp,v1 bool)")
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_pc_ms", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		w = httptest.NewRecorder()
		body = strings.NewReader(fmt.Sprintf(`insert into t1 values ('%s',true)`, nowT.Format(time.RFC3339Nano)))
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_pc_ms", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		w = httptest.NewRecorder()
		body = strings.NewReader(`select * from t1 limit 1`)
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_pc_ms", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		var result TDEngineRestfulRespDoc
		err := json.Unmarshal(w.Body.Bytes(), &result)
		assert.NoError(t, err)
		spT := strings.Split(result.Data[0][0].(string), ".")
		assert.Equal(t, 2, len(spT))
		assert.Equal(t, "000Z", spT[1])
		w = httptest.NewRecorder()
		body = strings.NewReader(`drop database test_pc_ms`)
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}
	{
		w := httptest.NewRecorder()
		body := strings.NewReader("create database if not exists test_pc_us precision 'us'")
		req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		w = httptest.NewRecorder()
		body = strings.NewReader("create table if not exists t1(ts timestamp,v1 bool)")
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_pc_us", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		w = httptest.NewRecorder()
		body = strings.NewReader(fmt.Sprintf(`insert into t1 values ('%s',true)`, nowT.Format(time.RFC3339Nano)))
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_pc_us", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		w = httptest.NewRecorder()
		body = strings.NewReader(`select * from t1 limit 1`)
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_pc_us", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		var result TDEngineRestfulRespDoc
		err := json.Unmarshal(w.Body.Bytes(), &result)
		assert.NoError(t, err)
		spT := strings.Split(result.Data[0][0].(string), ".")
		assert.Equal(t, 2, len(spT))
		assert.Equal(t, "000000Z", spT[1])
		w = httptest.NewRecorder()
		body = strings.NewReader(`drop database test_pc_us`)
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}
	{
		w := httptest.NewRecorder()
		body := strings.NewReader("create database if not exists test_pc_ns precision 'ns'")
		req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		w = httptest.NewRecorder()
		body = strings.NewReader("create table if not exists t1(ts timestamp,v1 bool)")
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_pc_ns", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		w = httptest.NewRecorder()
		body = strings.NewReader(fmt.Sprintf(`insert into t1 values ('%s',true)`, nowT.Format(time.RFC3339Nano)))
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_pc_ns", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		w = httptest.NewRecorder()
		body = strings.NewReader(`select * from t1 limit 1`)
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_pc_ns", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		var result TDEngineRestfulRespDoc
		err := json.Unmarshal(w.Body.Bytes(), &result)
		assert.NoError(t, err)
		spT := strings.Split(result.Data[0][0].(string), ".")
		assert.Equal(t, 2, len(spT))
		assert.Equal(t, "000000000Z", spT[1])
		w = httptest.NewRecorder()
		body = strings.NewReader(`drop database test_pc_ns`)
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}
}

func TestTimeZone(t *testing.T) {

	config.Conf.RestfulRowLimit = -1
	now := time.Now().Unix()
	nowT := time.Unix(now, 0)
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists test_tz")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists t1(ts timestamp,v1 bool)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_tz", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into t1 values ('%s',true)`, nowT.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_tz", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	//Asia/Shanghai
	testZone := map[string][]string{
		"UTC":           {"000Z"},
		"Asia/Shanghai": {"000+08:00"},
		"Europe/Moscow": {"000+03:00"},
		//Daylight Saving Time
		"America/New_York": {"000-04:00", "000-05:00"},
	}
	for zone, timeZone := range testZone {
		w := httptest.NewRecorder()
		body := strings.NewReader(`select * from t1 limit 1`)
		req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("/rest/sql/test_tz?tz=%s", zone), body)
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		var result TDEngineRestfulRespDoc
		err := json.Unmarshal(w.Body.Bytes(), &result)
		assert.NoError(t, err)
		spT := strings.Split(result.Data[0][0].(string), ".")
		assert.Equal(t, 2, len(spT))
		assert.Contains(t, timeZone, spT[1])
	}
	w = httptest.NewRecorder()
	body = strings.NewReader(`drop database test_tz`)
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

package inputjson

import (
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

type transformationSqlResult struct {
	Db       string  `json:"db"`
	Time     string  `json:"time"`
	Location string  `json:"location"`
	GroupID  int     `json:"groupid"`
	Stb      string  `json:"stb"`
	Table    string  `json:"table"`
	Current  float64 `json:"current"`
	Voltage  int     `json:"voltage"`
	Phase    int     `json:"phase"`
}

func TestHandle(t *testing.T) {
	testUser := "root"
	testPass := "taosdata"
	inputData := `{
    "time": "2025-11-04 09:24:13.123456",
    "Los Angeles": {
        "group_1": {
            "d_001": {
                "current": 10.5,
                "voltage": 220,
                "phase": 30
            },
            "d_002": {
                "current": 15.2,
                "voltage": 230,
                "phase": 45
            },
            "d_003": {
                "current": 8.7,
                "voltage": 210,
                "phase": 60
            }
        },
        "group_2": {
            "d_004": {
                "current": 12.3,
                "voltage": 225,
                "phase": 15
            },
            "d_005": {
                "current": 9.8,
                "voltage": 215,
                "phase": 75
            }
        }
    },
    "New York": {
        "group_1": {
            "d_006": {
                "current": 11.0,
                "voltage": 240,
                "phase": 20
            },
            "d_007": {
                "current": 14.5,
                "voltage": 235,
                "phase": 50
            }
        },
        "group_2": {
            "d_008": {
                "current": 13.2,
                "voltage": 245,
                "phase": 10
            },
            "d_009": {
                "current": 7.9,
                "voltage": 220,
                "phase": 80
            }
        }
    }
}`
	expectJson := `[
  {
    "db": "test_input_json",
    "time": "2025-11-04 09:24:13.123456",
    "location": "Los Angeles",
    "groupid": 1,
    "stb": "meters",
    "table": "d_001",
    "current": 10.5,
    "voltage": 220,
    "phase": 30
  },
  {
    "db": "test_input_json",
    "time": "2025-11-04 09:24:13.123456",
    "location": "Los Angeles",
    "groupid": 1,
    "stb": "meters",
    "table": "d_002",
    "current": 15.2,
    "voltage": 230,
    "phase": 45
  },
  {
    "db": "test_input_json",
    "time": "2025-11-04 09:24:13.123456",
    "location": "Los Angeles",
    "groupid": 1,
    "stb": "meters",
    "table": "d_003",
    "current": 8.7,
    "voltage": 210,
    "phase": 60
  },
  {
    "db": "test_input_json",
    "time": "2025-11-04 09:24:13.123456",
    "location": "Los Angeles",
    "groupid": 2,
    "stb": "meters",
    "table": "d_004",
    "current": 12.3,
    "voltage": 225,
    "phase": 15
  },
  {
    "db": "test_input_json",
    "time": "2025-11-04 09:24:13.123456",
    "location": "Los Angeles",
    "groupid": 2,
    "stb": "meters",
    "table": "d_005",
    "current": 9.8,
    "voltage": 215,
    "phase": 75
  },
  {
    "db": "test_input_json",
    "time": "2025-11-04 09:24:13.123456",
    "location": "New York",
    "groupid": 1,
    "stb": "meters",
    "table": "d_006",
    "current": 11,
    "voltage": 240,
    "phase": 20
  },
  {
    "db": "test_input_json",
    "time": "2025-11-04 09:24:13.123456",
    "location": "New York",
    "groupid": 1,
    "stb": "meters",
    "table": "d_007",
    "current": 14.5,
    "voltage": 235,
    "phase": 50
  },
  {
    "db": "test_input_json",
    "time": "2025-11-04 09:24:13.123456",
    "location": "New York",
    "groupid": 2,
    "stb": "meters",
    "table": "d_008",
    "current": 13.2,
    "voltage": 245,
    "phase": 10
  },
  {
    "db": "test_input_json",
    "time": "2025-11-04 09:24:13.123456",
    "location": "New York",
    "groupid": 2,
    "stb": "meters",
    "table": "d_009",
    "current": 7.9,
    "voltage": 220,
    "phase": 80
  }
]`
	expectSql := []string{"insert into `test_input_json`.`meters`(`tbname`,`ts`,`current`,`voltage`,`phase`,`location`,`groupid`)values" +
		"('d_001','2025-11-04T09:24:13.123456Z',10.5,220,30,'Los Angeles',1)" +
		"('d_002','2025-11-04T09:24:13.123456Z',15.2,230,45,'Los Angeles',1)" +
		"('d_003','2025-11-04T09:24:13.123456Z',8.7,210,60,'Los Angeles',1)" +
		"('d_004','2025-11-04T09:24:13.123456Z',12.3,225,15,'Los Angeles',2)" +
		"('d_005','2025-11-04T09:24:13.123456Z',9.8,215,75,'Los Angeles',2)" +
		"('d_006','2025-11-04T09:24:13.123456Z',11,240,20,'New York',1)" +
		"('d_007','2025-11-04T09:24:13.123456Z',14.5,235,50,'New York',1)" +
		"('d_008','2025-11-04T09:24:13.123456Z',13.2,245,10,'New York',2)" +
		"('d_009','2025-11-04T09:24:13.123456Z',7.9,220,80,'New York',2)"}
	configContent := `[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
dbKey = "db"
superTableKey = "stb"
subTableKey = "table"
timeKey = "time"
timeFormat = "iso8601nano"
timeTimeZone = "UTC"
transformation = '''
$sort(
    (
        $ts := time;
        $each($, function($value, $key) {
            $key = "time" ? [] : (
                $each($value, function($groupValue, $groupKey) {
                    $each($groupValue, function($deviceValue, $deviceKey) {
                        {
                            "db": "test_input_json",
                            "time": $ts,
                            "location": $key,
                            "groupid": $number($split($groupKey, "_")[1]),
                            "stb": "meters",
                            "table": $deviceKey,
                            "current": $deviceValue.current,
                            "voltage": $deviceValue.voltage,
                            "phase": $deviceValue.phase
                        }
                    })
                })
            )
        })
    ).[*][*],
    function($l, $r) {
        $l.table > $r.table
    }
)
'''
fields = [
    {key = "current", optional = false},
    {key = "voltage", optional = false},
    {key = "phase", optional = false},
    {key = "location", optional = false},
    {key = "groupid", optional = false},
]`
	tmpDir := t.TempDir()
	p := &Plugin{}
	configPath := filepath.Join(tmpDir, "config.toml")
	err := os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)
	v := viper.New()
	v.SetConfigType("toml")
	v.SetConfigFile(configPath)
	err = v.ReadInConfig()
	require.NoError(t, err)
	r := gin.New()
	err = p.initWithViper(r, v)
	require.NoError(t, err)

	// invalid endpoint
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/rule2", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(testUser, testPass)
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
	t.Log(w.Body.String())

	// invalid req_id
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/rule1?req_id=abc", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(testUser, testPass)
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	t.Log(w.Body.String())

	// wrong auth
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/rule1?req_id=1123", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(testUser, "wrong_pass")
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	t.Log(w.Body.String())

	// invalid body
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/rule1", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(testUser, testPass)
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	t.Log(w.Body.String())

	// invalid json
	body := `[invalid json`
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/rule1", strings.NewReader(body))
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(testUser, testPass)
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	t.Log(w.Body.String())

	// dry run
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/rule1?dry_run=true", strings.NewReader(inputData))
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(testUser, testPass)
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	var resp dryRunResp
	err = json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Code)
	assert.Equal(t, "", resp.Desc)
	assert.Equal(t, expectSql, resp.Sql)
	var expectedResults []transformationSqlResult
	err = json.Unmarshal([]byte(expectJson), &expectedResults)
	require.NoError(t, err)
	var respResults []transformationSqlResult
	err = json.Unmarshal([]byte(resp.Json), &respResults)
	assert.Equal(t, expectedResults, respResults)

	// actual insert with db not exists
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/rule1", strings.NewReader(inputData))
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(testUser, testPass)
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	t.Log(w.Body.String())
	var insertResp message
	err = json.Unmarshal(w.Body.Bytes(), &insertResp)
	require.NoError(t, err)
	assert.Equal(t, 904, insertResp.Code)
	assert.Equal(t, "Database not exist", insertResp.Desc)
	assert.Equal(t, 0, insertResp.Affected)

	// create db and table for insert test
	taosConn, err := commonpool.GetConnection(testUser, testPass, iptool.GetRealIP(req))
	require.NoError(t, err)
	defer func() {
		err = taosConn.Put()
		require.NoError(t, err)
	}()
	totalAffected, err := execute(taosConn.TaosConnection, 0x123, []string{
		"drop database if exists test_input_json",
		"create database test_input_json",
		"create table test_input_json.meters (ts timestamp, current float, voltage int, phase float) tags (location nchar(64), `groupid` int)",
	}, logger, false)
	require.NoError(t, err)
	require.Equal(t, 0, totalAffected)
	defer func() {
		_, err = execute(taosConn.TaosConnection, 0x123, []string{
			"drop database if exists test_input_json",
		}, logger, false)
		require.NoError(t, err)
	}()
	// actual insert
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/rule1", strings.NewReader(inputData))
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(testUser, testPass)
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	t.Log(w.Body.String())
	insertResp = message{}
	err = json.Unmarshal(w.Body.Bytes(), &insertResp)
	require.NoError(t, err)
	assert.Equal(t, 0, insertResp.Code)
	assert.Equal(t, "", insertResp.Desc)
	assert.Equal(t, 9, insertResp.Affected)

	// verify data inserted
	result, err := async.GlobalAsync.TaosExec(taosConn.TaosConnection, logger, false, "select tbname,* from test_input_json.meters order by tbname asc;", func(ts int64, precision int) driver.Value {
		return ts
	}, 0x123)
	require.NoError(t, err)
	assert.Equal(t, 9, len(result.Data))
	//t.Log(result.Data)
	var expectedInsertedValues = [][]driver.Value{
		{"d_001", int64(1762248253123), float32(10.5), int32(220), float32(30.0), "Los Angeles", int32(1)},
		{"d_002", int64(1762248253123), float32(15.2), int32(230), float32(45.0), "Los Angeles", int32(1)},
		{"d_003", int64(1762248253123), float32(8.7), int32(210), float32(60.0), "Los Angeles", int32(1)},
		{"d_004", int64(1762248253123), float32(12.3), int32(225), float32(15.0), "Los Angeles", int32(2)},
		{"d_005", int64(1762248253123), float32(9.8), int32(215), float32(75.0), "Los Angeles", int32(2)},
		{"d_006", int64(1762248253123), float32(11.0), int32(240), float32(20.0), "New York", int32(1)},
		{"d_007", int64(1762248253123), float32(14.5), int32(235), float32(50.0), "New York", int32(1)},
		{"d_008", int64(1762248253123), float32(13.2), int32(245), float32(10.0), "New York", int32(2)},
		{"d_009", int64(1762248253123), float32(7.9), int32(220), float32(80.0), "New York", int32(2)},
	}
	assert.Equal(t, expectedInsertedValues, result.Data)
}

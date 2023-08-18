package schemaless

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/spf13/viper"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
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
	controllers := controller.GetControllers()
	for _, webController := range controllers {
		webController.Init(router)
	}
	m.Run()
}

func TestRestful_InitSchemaless(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	wrapper.TaosFreeResult(wrapper.TaosQuery(conn, "drop database if exists test_schemaless_ws"))
	wrapper.TaosFreeResult(wrapper.TaosQuery(conn, "create database if not exists test_schemaless_ws"))
	defer func() {
		//wrapper.TaosFreeResult(wrapper.TaosQuery(conn, "drop database if exists test_schemaless_ws"))
	}()

	s := httptest.NewServer(router)
	defer s.Close()
	url := strings.Replace(s.URL, "http", "ws", 1) + "/rest/schemaless"

	cases := []struct {
		name      string
		protocol  int
		precision string
		data      string
		ttl       int
		code      int
	}{
		{
			name:      "influxdb",
			protocol:  1,
			precision: "ms",
			data: "measurement,host=host1 field1=2i,field2=2.0 1577837300000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837400000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837500000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837600000",
			ttl:  1000,
			code: 0,
		},
		{
			name:      "opentsdb_telnet",
			protocol:  2,
			precision: "ms",
			data: "meters.current 1648432611249 10.3 location=California.SanFrancisco group=2\n" +
				"meters.current 1648432611250 12.6 location=California.SanFrancisco group=2\n" +
				"meters.current 1648432611249 10.8 location=California.LosAngeles group=3\n" +
				"meters.current 1648432611250 11.3 location=California.LosAngeles group=3\n" +
				"meters.voltage 1648432611249 219 location=California.SanFrancisco group=2\n" +
				"meters.voltage 1648432611250 218 location=California.SanFrancisco group=2\n" +
				"meters.voltage 1648432611249 221 location=California.LosAngeles group=3\n" +
				"meters.voltage 1648432611250 217 location=California.LosAngeles group=3",
			ttl:  1000,
			code: 0,
		},
		{
			name:      "opentsdb_json",
			protocol:  3,
			precision: "ms",
			data: `[
    {
        "metric": "meters2.current",
        "timestamp": 1648432611249,
        "value": 10.3,
        "tags": {
            "location": "California.SanFrancisco",
            "groupid": 2
        }
    },
    {
        "metric": "meters2.voltage",
        "timestamp": 1648432611249,
        "value": 219,
        "tags": {
            "location": "California.LosAngeles",
            "groupid": 1
        }
    },
    {
        "metric": "meters2.current",
        "timestamp": 1648432611250,
        "value": 12.6,
        "tags": {
            "location": "California.SanFrancisco",
            "groupid": 2
        }
    },
    {
        "metric": "meters2.voltage",
        "timestamp": 1648432611250,
        "value": 221,
        "tags": {
            "location": "California.LosAngeles",
            "groupid": 1
        }
    }
]`,
			ttl:  100,
			code: 0,
		},
	}

	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal("connect error", err)
	}
	defer ws.Close()

	j, _ := json.Marshal(map[string]interface{}{
		"action": "conn",
		"args": map[string]string{
			"user":     "root",
			"password": "taosdata",
			"db":       "test_schemaless_ws",
		},
	})

	if err := ws.WriteMessage(websocket.TextMessage, j); err != nil {
		t.Fatal("send connect message error", err)
	}
	_, msg, err := ws.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var resp wstool.WSErrorResp
	_ = json.Unmarshal(msg, &resp)
	if resp.Code != 0 {
		t.Fatal(resp)
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			j, _ := json.Marshal(map[string]interface{}{
				"action": "insert",
				"args": map[string]interface{}{
					"protocol":  c.protocol,
					"precision": c.precision,
					"data":      c.data,
					"ttl":       c.ttl,
				},
			})
			if err := ws.WriteMessage(websocket.TextMessage, j); err != nil {
				t.Fatal(c.name, err)
			}
			_, msg, err := ws.ReadMessage()
			if err != nil {
				t.Fatal(c.name, err)
			}
			var resp wstool.WSErrorResp
			_ = json.Unmarshal(msg, &resp)
			if resp.Code != 0 {
				t.Fatal(c.name, string(msg))
			}
		})
	}
}

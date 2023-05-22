package wstool

import (
	"database/sql/driver"
	"encoding/json"

	"github.com/huskar-t/melody"
	"github.com/taosdata/taosadapter/v3/version"
)

type TDEngineRestfulResp struct {
	Code       int              `json:"code,omitempty"`
	Desc       string           `json:"desc,omitempty"`
	ColumnMeta [][]interface{}  `json:"column_meta,omitempty"`
	Data       [][]driver.Value `json:"data,omitempty"`
	Rows       int              `json:"rows,omitempty"`
}

func WSWriteJson(session *melody.Session, data interface{}) {
	b, _ := json.Marshal(data)
	session.Write(b)
}

type WSVersionResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	Version string `json:"version"`
}

var VersionResp []byte

type WSAction struct {
	Action string          `json:"action"`
	Args   json.RawMessage `json:"args"`
}

func init() {
	resp := WSVersionResp{
		Action:  ClientVersion,
		Version: version.TaosClientVersion,
	}
	VersionResp, _ = json.Marshal(resp)
}

package rest

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/huskar-t/melody"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/schemaless/inserter"
	"github.com/taosdata/taosadapter/v3/tools/web"
)

const reqIdKey = "reqId"
const connectedKey = "connected"

func (ctl *Restful) InitSchemaless() {
	ctl.schemaless = melody.New()
	ctl.schemaless.Config.MaxMessageSize = 4 * 1024 * 1024

	ctl.schemaless.HandleConnect(func(session *melody.Session) {
		l := session.MustGet("logger").(*logrus.Entry)
		l.Debugln("ws connect")
		session.Set(taosSchemalessKey, NewTaos())
	})

	ctl.schemaless.HandleMessage(ctl.handleMessage)
	ctl.schemaless.HandleMessageBinary(ctl.handleMessage)

	ctl.schemaless.HandleClose(func(session *melody.Session, i int, s string) error {
		l := session.MustGet("logger").(*logrus.Entry)
		l.Debugln("ws close", i, s)
		closeTaos(session)
		return nil
	})

	ctl.schemaless.HandleError(func(session *melody.Session, err error) {
		l := session.MustGet("logger").(*logrus.Entry)
		if _, is := err.(*websocket.CloseError); is {
			l.WithError(err).Debugln("ws close in error")
		} else {
			l.WithError(err).Errorln("ws error")
		}

		closeTaos(session)
	})

	ctl.schemaless.HandleDisconnect(func(session *melody.Session) {
		l := session.MustGet("logger").(*logrus.Entry)
		l.Debugln("ws disconnect")
		closeTaos(session)
	})
}

var unknownProtocolError = errors.New("unknown protocol")
var unConnectedError = errors.New("unconnected")
var paramsError = errors.New("args error")

func (ctl *Restful) handleMessage(session *melody.Session, bytes []byte) {
	ctx := context.WithValue(context.Background(), StartTimeKey, time.Now().UnixNano())
	if ctl.schemaless.IsClosed() {
		return
	}
	l := session.MustGet("logger").(*logrus.Entry)

	l.Debugln("get ws byte message data:", string(bytes))
	var action WSAction
	err := json.Unmarshal(bytes, &action)
	if err != nil {
		logger.WithError(err).Errorln("unmarshal ws request")
		return
	}

	switch action.Action {
	case SchemalessConn:
		var wsConnect WSConnectReq
		if err = json.Unmarshal(action.Args, &wsConnect); err != nil {
			logger.WithError(err).Errorln("unmarshal connect request args")
			return
		}
		t := session.MustGet(taosSchemalessKey)
		t.(*Taos).connect(ctx, session, &wsConnect)
		session.Set(connectedKey, true)
	case SchemalessWrite:
		var schemaless SchemalessWriteReq
		if err = json.Unmarshal(action.Args, &schemaless); err != nil {
			logger.WithError(err).Errorln("unmarshal schemaless write request args")
			return
		}

		// check connect
		if _, ok := session.Get(connectedKey); !ok {
			wsError(ctx, session, unConnectedError, SchemalessWrite, schemaless.ReqID)
			return
		}

		if schemaless.Protocol == 0 || len(schemaless.Precision) == 0 || len(schemaless.DB) == 0 || len(schemaless.DB) == 0 {
			wsError(ctx, session, paramsError, SchemalessWrite, schemaless.ReqID)
			return
		}

		conn := session.MustGet(taosSchemalessKey).(*Taos).conn
		switch schemaless.Protocol {
		case wrapper.InfluxDBLineProtocol:
			_, err = inserter.InsertInfluxdbRaw(conn, []byte(schemaless.Data), schemaless.DB, schemaless.Precision)
		case wrapper.OpenTSDBTelnetLineProtocol:
			err = inserter.InsertOpentsdbTelnetBatchRaw(conn, strings.Split(schemaless.Data, "\n"), schemaless.DB)
		case wrapper.OpenTSDBJsonFormatProtocol:
			err = inserter.InsertOpentsdbJsonRaw(conn, []byte(schemaless.Data), schemaless.DB)
		default:
			err = unknownProtocolError
		}
		if err != nil {
			wsError(ctx, session, err, SchemalessWrite, schemaless.ReqID)
		}
		resp := &SchemalessWriteResp{Action: SchemalessWrite, ReqID: schemaless.ReqID, Timing: getDuration(ctx)}
		wsWriteJson(session, resp)
	}
}

// schemalessWs
// @Tags websocket
// @Param Authorization header string true "authorization token"
// @Router /schemaless?db=test&precision=ms
func (ctl *Restful) schemalessWs(c *gin.Context) {
	reqId := web.GetRequestID(c)
	l := logger.WithField("sessionID", reqId)

	_ = ctl.schemaless.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{
		"logger": l,
		reqIdKey: reqId,
	})
}

func closeTaos(session *melody.Session) {
	if t, exist := session.Get(taosSchemalessKey); exist && t != nil {
		t.(*Taos).Close()
	}
}

type SchemalessWriteReq struct {
	ReqID     uint64 `json:"req_id"`
	DB        string `json:"db"`
	Protocol  int    `json:"protocol"`
	Precision string `json:"precision"`
	Data      string `json:"data"`
}

type SchemalessWriteResp struct {
	ReqID  uint64 `json:"req_id"`
	Action string `json:"action"`
	Timing int64  `json:"timing"`
}

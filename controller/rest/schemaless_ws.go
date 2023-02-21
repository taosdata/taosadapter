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
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/schemaless/inserter"
	"github.com/taosdata/taosadapter/v3/tools/web"
)

func (ctl *Restful) InitSchemaless() {
	ctl.schemaless = melody.New()
	ctl.schemaless.Config.MaxMessageSize = 4 * 1024 * 1024

	ctl.schemaless.HandleConnect(func(session *melody.Session) {
	})

	ctl.schemaless.HandleMessage(ctl.handleMessage)
	ctl.schemaless.HandleMessageBinary(ctl.handleMessage)

	ctl.schemaless.HandleClose(func(session *melody.Session, i int, s string) error {
		return nil
	})

	ctl.schemaless.HandleError(func(session *melody.Session, err error) {
		l := session.MustGet("logger").(*logrus.Entry)
		if _, is := err.(*websocket.CloseError); is {
			l.WithError(err).Debugln("ws close in error")
		} else {
			l.WithError(err).Errorln("ws error")
		}
	})

	ctl.schemaless.HandleDisconnect(func(session *melody.Session) {
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
		wsError(ctx, session, err, action.Action, 0)
		return
	}

	switch action.Action {
	case SchemalessConn:
		var connReq schemalessConnReq
		if err = json.Unmarshal(action.Args, &connReq); err != nil {
			logger.WithError(err).Errorln("unmarshal connect request args")
			wsError(ctx, session, err, SchemalessConn, 0)
			return
		}

		conn, err := commonpool.GetConnection(connReq.User, connReq.Password)
		if err != nil {
			logger.WithError(err).Errorln("unmarshal connect request args")
			wsError(ctx, session, err, SchemalessConn, connReq.ReqID)
			return
		}
		_ = conn.Put()
		session.Set(taosSchemalessKey, &connReq)
	case SchemalessWrite:
		var schemaless schemalessWriteReq
		if err = json.Unmarshal(action.Args, &schemaless); err != nil {
			logger.WithError(err).Errorln("unmarshal schemaless write request args")
			wsError(ctx, session, err, SchemalessWrite, schemaless.ReqID)
			return
		}
		if schemaless.Protocol == 0 || len(schemaless.Precision) == 0 || len(schemaless.DB) == 0 || len(schemaless.DB) == 0 {
			wsError(ctx, session, paramsError, SchemalessWrite, schemaless.ReqID)
			return
		}

		connReq, ok := session.Get(taosSchemalessKey)
		if !ok {
			wsError(ctx, session, unConnectedError, SchemalessWrite, schemaless.ReqID)
			return
		}
		connInfo := connReq.(*schemalessConnReq)
		conn, err := commonpool.GetConnection(connInfo.User, connInfo.Password)
		if err != nil {
			logger.WithError(err).Errorln("get taos connection error ")
			wsError(ctx, session, err, SchemalessWrite, schemaless.ReqID)
			return
		}
		defer func() { _ = conn.Put() }()

		switch schemaless.Protocol {
		case wrapper.InfluxDBLineProtocol:
			_, err = inserter.InsertInfluxdb(conn.TaosConnection, []byte(schemaless.Data), schemaless.DB,
				schemaless.Precision, schemaless.TTL, schemaless.ReqID)
		case wrapper.OpenTSDBTelnetLineProtocol:
			err = inserter.InsertOpentsdbTelnetBatch(conn.TaosConnection, strings.Split(schemaless.Data, "\n"),
				schemaless.DB, schemaless.TTL, schemaless.ReqID)
		case wrapper.OpenTSDBJsonFormatProtocol:
			err = inserter.InsertOpentsdbJson(conn.TaosConnection, []byte(schemaless.Data), schemaless.DB,
				schemaless.TTL, schemaless.ReqID)
		default:
			err = unknownProtocolError
		}
		if err != nil {
			wsError(ctx, session, err, SchemalessWrite, schemaless.ReqID)
		}
		resp := &schemalessWriteResp{Action: SchemalessWrite, ReqID: schemaless.ReqID, Timing: getDuration(ctx)}
		wsWriteJson(session, resp)
	}
}

// schemalessWs
// @Tags websocket
// @Param Authorization header string true "authorization token"
// @Router /schemaless?db=test&precision=ms
func (ctl *Restful) schemalessWs(c *gin.Context) {
	l := logger.WithField("sessionID", web.GetRequestID(c))
	_ = ctl.schemaless.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": l})
}

type schemalessConnReq struct {
	ReqID    uint64 `json:"req_id"`
	User     string `json:"user"`
	Password string `json:"password"`
	DB       string `json:"db"`
}

type schemalessWriteReq struct {
	ReqID     uint64 `json:"req_id"`
	DB        string `json:"db"`
	Protocol  int    `json:"protocol"`
	Precision string `json:"precision"`
	TTL       int    `json:"ttl"`
	Data      string `json:"data"`
}

type schemalessWriteResp struct {
	ReqID  uint64 `json:"req_id"`
	Action string `json:"action"`
	Timing int64  `json:"timing"`
}

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
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/schemaless/inserter"
)

func (ctl *Restful) InitSchemaless() {
	ctl.schemaless = melody.New()
	ctl.schemaless.Config.MaxMessageSize = 4 * 1024 * 1024

	ctl.schemaless.HandleConnect(func(session *melody.Session) {
	})

	ctl.schemaless.HandleMessage(ctl.handleMessage)
	ctl.schemaless.HandleMessageBinary(ctl.handleMessage)

	ctl.schemaless.HandleClose(func(session *melody.Session, i int, s string) error {
		sessionConn, ok := session.Get(taosSchemalessKey)
		if !ok {
			return nil
		}
		_ = sessionConn.(*commonpool.Conn).Put()
		return nil
	})

	ctl.schemaless.HandleError(func(session *melody.Session, err error) {
		if _, is := err.(*websocket.CloseError); is {
			logger.WithError(err).Debugln("ws close in error")
		} else {
			logger.WithError(err).Errorln("ws error")
		}
	})

	ctl.schemaless.HandleDisconnect(func(session *melody.Session) {
		sessionConn, ok := session.Get(taosSchemalessKey)
		if !ok {
			return
		}
		_ = sessionConn.(*commonpool.Conn).Put()
		return
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

	logger.Debugln("get ws byte message data:", string(bytes))
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
			wsError(ctx, session, err, SchemalessConn, connReq.ReqID)
			return
		}

		conn, err := commonpool.GetConnection(connReq.User, connReq.Password)
		if err != nil {
			logger.WithError(err).Errorln("get connection error")
			wsError(ctx, session, err, SchemalessConn, connReq.ReqID)
			return
		}
		session.Set(taosSchemalessKey, conn)
		session.Set(taosSchemalessDBKey, connReq.DB)
		wsWriteJson(session, &schemalessConnResp{
			Action: SchemalessConn,
			ReqID:  connReq.ReqID,
			Timing: getDuration(ctx),
		})
	case SchemalessWrite:
		var schemaless schemalessWriteReq
		if err = json.Unmarshal(action.Args, &schemaless); err != nil {
			logger.WithError(err).WithField(config.ReqIDKey, schemaless.ReqID).
				Errorln("unmarshal schemaless write request args")
			wsError(ctx, session, err, SchemalessWrite, schemaless.ReqID)
			return
		}
		if schemaless.Protocol == 0 || len(schemaless.Precision) == 0 {
			wsError(ctx, session, paramsError, SchemalessWrite, schemaless.ReqID)
			return
		}

		sessionConn, ok := session.Get(taosSchemalessKey)
		if !ok {
			wsError(ctx, session, unConnectedError, SchemalessWrite, schemaless.ReqID)
			return
		}
		conn := sessionConn.(*commonpool.Conn)
		db := session.MustGet(taosSchemalessDBKey).(string)

		switch schemaless.Protocol {
		case wrapper.InfluxDBLineProtocol:
			err = inserter.InsertInfluxdb(conn.TaosConnection, []byte(schemaless.Data), db, schemaless.Precision,
				schemaless.TTL, schemaless.ReqID)
		case wrapper.OpenTSDBTelnetLineProtocol:
			err = inserter.InsertOpentsdbTelnetBatch(conn.TaosConnection, strings.Split(schemaless.Data, "\n"),
				db, schemaless.TTL, schemaless.ReqID)
		case wrapper.OpenTSDBJsonFormatProtocol:
			err = inserter.InsertOpentsdbJson(conn.TaosConnection, []byte(schemaless.Data), db, schemaless.TTL,
				schemaless.ReqID)
		default:
			err = unknownProtocolError
		}
		if err != nil {
			wsError(ctx, session, err, SchemalessWrite, schemaless.ReqID)
		}
		resp := &schemalessResp{Action: SchemalessWrite, ReqID: schemaless.ReqID, Timing: getDuration(ctx)}
		wsWriteJson(session, resp)
	}
}

// schemalessWs
// @Tags websocket
// @Param Authorization header string true "authorization token"
// @Router /schemaless?db=test&precision=ms
func (ctl *Restful) schemalessWs(c *gin.Context) {
	_ = ctl.schemaless.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{})
}

type schemalessConnReq struct {
	ReqID    uint64 `json:"req_id"`
	User     string `json:"user"`
	Password string `json:"password"`
	DB       string `json:"db"`
}

type schemalessConnResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

type schemalessWriteReq struct {
	ReqID     uint64 `json:"req_id"`
	Protocol  int    `json:"protocol"`
	Precision string `json:"precision"`
	TTL       int    `json:"ttl"`
	Data      string `json:"data"`
}

type schemalessResp struct {
	ReqID  uint64 `json:"req_id"`
	Action string `json:"action"`
	Timing int64  `json:"timing"`
}

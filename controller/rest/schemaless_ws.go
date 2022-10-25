package rest

import (
	"context"
	"errors"
	"net/http"
	"strconv"
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

const protocolKey = "protocol"
const dbKey = "db"
const precisionKey = "precision"
const reqIdKey = "reqId"

func (ctl *Restful) InitSchemaless() {
	ctl.schemaless = melody.New()
	ctl.schemaless.Config.MaxMessageSize = 4 * 1024 * 1024

	ctl.schemaless.HandleConnect(func(session *melody.Session) {
		l := session.MustGet("logger").(*logrus.Entry)
		l.Debugln("ws connect")
		session.Set(TaosSessionKey, NewTaos())
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

func (ctl *Restful) handleMessage(session *melody.Session, bytes []byte) {
	ctx := context.WithValue(context.Background(), StartTimeKey, time.Now().UnixNano())
	reqId := session.MustGet(reqIdKey).(uint32)
	l := session.MustGet("logger").(*logrus.Entry)

	if ctl.schemaless.IsClosed() {
		return
	}
	l.Debugln("get ws byte message data:", string(bytes))
	db := session.MustGet(dbKey).(string)
	precision := session.MustGet(precisionKey).(string)

	taos, err := commonpool.GetConnection(session.MustGet(UserKey).(string), session.MustGet(PasswordKey).(string))
	if err != nil {
		wsError(ctx, session, err, WSWriteSchemaless, uint64(reqId))
		return
	}
	defer func() {
		if err := taos.Put(); err != nil {
			l.WithError(err).Errorln("taos connect pool put error")
		}
	}()

	protocol, _ := strconv.Atoi(session.MustGet(protocolKey).(string))
	switch protocol {
	case wrapper.InfluxDBLineProtocol:
		_, err = inserter.InsertInfluxdbRaw(taos.TaosConnection, bytes, db, precision)
	case wrapper.OpenTSDBTelnetLineProtocol:
		err = inserter.InsertOpentsdbTelnetBatchRaw(taos.TaosConnection, strings.Split(string(bytes), "\n"), db)
	case wrapper.OpenTSDBJsonFormatProtocol:
		err = inserter.InsertOpentsdbJsonRaw(taos.TaosConnection, bytes, db)
	default:
		err = unknownProtocolError
	}
	if err != nil {
		wsError(ctx, session, err, WSWriteSchemaless, uint64(reqId))
		return
	}
	resp := &WSWriteMetaResp{Action: WSWriteSchemaless, ReqID: uint64(reqId), Timing: getDuration(ctx)}
	wsWriteJson(session, resp)
}

// schemalessWs
// @Tags websocket
// @Param Authorization header string true "authorization token"
// @Router /schemaless?db=test&precision=ms
func (ctl *Restful) schemalessWs(c *gin.Context) {
	reqId := web.GetRequestID(c)
	l := logger.WithField("sessionID", reqId)

	protocol := c.Query("protocol")
	if len(protocol) == 0 {
		c.JSON(http.StatusBadRequest, "protocol required")
		return
	}

	db := c.Query("db")
	if len(db) == 0 {
		c.JSON(http.StatusBadRequest, "db required")
		return
	}

	precision := c.Query("precision")
	if len(precision) == 0 {
		precision = "ms"
	}

	_ = ctl.schemaless.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{
		"logger":     l,
		reqIdKey:     reqId,
		precisionKey: precision,
		dbKey:        db,
		protocolKey:  protocol,
		UserKey:      c.MustGet(UserKey).(string),
		PasswordKey:  c.MustGet(PasswordKey).(string),
	})
}

func closeTaos(session *melody.Session) {
	if t, exist := session.Get(TaosSessionKey); exist && t != nil {
		t.(*Taos).Close()
	}
}

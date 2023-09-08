package ws

import (
	"errors"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/huskar-t/melody"
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/log"
)

var logger = log.GetLogger("websocket")

func init() {
	controller.AddController(initController())
}

type webSocketCtl struct {
	m *melody.Melody
}

func (ws *webSocketCtl) Init(ctl gin.IRouter) {
	ctl.GET("ws", func(c *gin.Context) {
		if err := ws.m.HandleRequest(c.Writer, c.Request); err != nil {
			panic(err)
		}
	})
}

func initController() *webSocketCtl {
	m := melody.New()
	m.Config.MaxMessageSize = 0

	m.HandleConnect(func(session *melody.Session) {
		logger.Debugln("ws connect")
		session.Set(TaosKey, newHandler())
	})
	m.HandleMessage(func(session *melody.Session, data []byte) {
		if m.IsClosed() {
			return
		}
		session.MustGet(TaosKey).(*messageHandler).handleMessage(session, data)
	})
	m.HandleMessageBinary(func(session *melody.Session, bytes []byte) {
		if m.IsClosed() {
			return
		}
		session.MustGet(TaosKey).(*messageHandler).handleMessageBinary(session, bytes)
	})
	m.HandleClose(func(session *melody.Session, i int, s string) error {
		logger.Debugln("ws close", i, s)
		CloseWs(session)
		return session.Close()
	})
	m.HandleError(func(session *melody.Session, err error) {
		var closeError *websocket.CloseError
		is := errors.As(err, &closeError)
		if is {
			logger.WithError(err).Debugln("ws close in error")
		} else {
			logger.WithError(err).Errorln("ws error")
		}

		CloseWs(session)
	})
	m.HandleDisconnect(func(session *melody.Session) {
		logger.Debugln("ws disconnect")
		CloseWs(session)
	})

	return &webSocketCtl{m: m}
}

func CloseWs(session *melody.Session) {
	t, exist := session.Get(TaosKey)
	if exist && t != nil {
		t.(*messageHandler).Close()
	}
	session.Set(TaosKey, nil)
}

package ws

import (
	"github.com/gin-gonic/gin"
	"github.com/huskar-t/melody"
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
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
		if err := ws.m.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": logger}); err != nil {
			panic(err)
		}
	})
}

func initController() *webSocketCtl {
	m := melody.New()
	m.Config.MaxMessageSize = 0

	m.HandleConnect(func(session *melody.Session) {
		logger.Debugln("ws connect")
		session.Set(TaosKey, newHandler(session))
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
		wstool.LogWSError(session, err)
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

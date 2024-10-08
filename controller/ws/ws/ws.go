package ws

import (
	"github.com/gin-gonic/gin"
	"github.com/huskar-t/melody"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/generator"
)

func init() {
	controller.AddController(initController())
}

type webSocketCtl struct {
	m *melody.Melody
}

func (ws *webSocketCtl) Init(ctl gin.IRouter) {
	ctl.GET("ws", func(c *gin.Context) {
		sessionID := generator.GetSessionID()
		logger := log.GetLogger("WSC").WithFields(logrus.Fields{
			config.SessionIDKey: sessionID})
		if err := ws.m.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": logger}); err != nil {
			panic(err)
		}
	})
}

func initController() *webSocketCtl {
	m := melody.New()
	m.Config.MaxMessageSize = 0
	m.UpGrader.EnableCompression = true

	m.HandleConnect(func(session *melody.Session) {
		logger := wstool.GetLogger(session)
		logger.Debug("ws connect")
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
		logger := wstool.GetLogger(session)
		logger.Debugf("ws close, code:%d, msg %s", i, s)
		CloseWs(session)
		return session.Close()
	})
	m.HandleError(func(session *melody.Session, err error) {
		wstool.LogWSError(session, err)
		CloseWs(session)
	})
	m.HandleDisconnect(func(session *melody.Session) {
		logger := wstool.GetLogger(session)
		logger.Debug("ws disconnect")
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

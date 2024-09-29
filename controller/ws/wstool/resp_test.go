package wstool

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/huskar-t/melody"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestWSWriteJson(t *testing.T) {
	m := melody.New()
	m.Config.MaxMessageSize = 4 * 1024 * 1024
	data := &WSVersionResp{
		Code:    200,
		Message: "Success",
		Action:  "version",
		Version: "1.0.0",
	}
	m.HandleMessage(func(session *melody.Session, msg []byte) {
		if m.IsClosed() {
			return
		}
		logger := logrus.New().WithField("test", "TestWSWriteJson")
		session.Set("logger", logger)
		WSWriteJson(session, logger, data)
	})
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.GET("/test", func(c *gin.Context) {
		_ = m.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{})
	})
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/test", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer ws.Close()
	err = ws.WriteMessage(websocket.TextMessage, []byte{'1'})
	assert.NoError(t, err)
	wt, resp, err := ws.ReadMessage()
	assert.NoError(t, err)
	assert.NoError(t, err)
	assert.Equal(t, websocket.TextMessage, wt)
	var respS WSVersionResp
	err = json.Unmarshal(resp, &respS)
	assert.NoError(t, err)
	assert.Equal(t, 200, respS.Code)
	assert.Equal(t, "Success", respS.Message)
	assert.Equal(t, "1.0.0", respS.Version)
}

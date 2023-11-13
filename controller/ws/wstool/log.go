package wstool

import (
	"context"
	"errors"
	"time"

	"github.com/gorilla/websocket"
	"github.com/huskar-t/melody"
	"github.com/sirupsen/logrus"
)

func GetDuration(ctx context.Context) int64 {
	return time.Now().UnixNano() - ctx.Value(StartTimeKey).(int64)
}

func GetLogger(session *melody.Session) *logrus.Entry {
	return session.MustGet("logger").(*logrus.Entry)
}

func LogWSError(session *melody.Session, err error) {
	logger := session.MustGet("logger").(*logrus.Entry)
	var wsCloseErr *websocket.CloseError
	is := errors.As(err, &wsCloseErr)
	if is {
		if wsCloseErr.Code == websocket.CloseNormalClosure {
			logger.Debugln("ws close normal")
		} else {
			logger.WithError(err).Debugln("ws close in error")
		}
	} else {
		logger.WithError(err).Errorln("ws error")
	}
}

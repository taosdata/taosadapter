package wstool

import (
	"context"
	"time"

	"github.com/huskar-t/melody"
	"github.com/sirupsen/logrus"
)

func GetDuration(ctx context.Context) int64 {
	return time.Now().UnixNano() - ctx.Value(StartTimeKey).(int64)
}

func GetLogger(session *melody.Session) *logrus.Entry {
	return session.MustGet("logger").(*logrus.Entry)
}

package wstool

import (
	"context"
	"testing"
	"time"

	"github.com/huskar-t/melody"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestGetDuration(t *testing.T) {
	startTime := time.Now().UnixNano()
	ctx := context.WithValue(context.Background(), StartTimeKey, startTime)
	time.Sleep(100 * time.Millisecond)
	duration := GetDuration(ctx)
	assert.Greater(t, duration, int64(0))
}

func TestGetLogger(t *testing.T) {
	logger := logrus.New()
	session := &melody.Session{}
	session.Set("logger", logger.WithField("test_field", "test_value"))
	entry := GetLogger(session)
	assert.Equal(t, "test_value", entry.Data["test_field"])
}

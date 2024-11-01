package log

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
)

// @author: xftan
// @date: 2021/12/14 15:07
// @description: test config log
func TestConfigLog(t *testing.T) {
	config.Init()
	config.Conf.Log.EnableRecordHttpSql = true
	ConfigLog()
	logger := GetLogger("TST")
	logger.Info("test config log")
}

func TestIsDebug(t *testing.T) {
	config.Init()
	assert.False(t, IsDebug())
	s := GetLogNow(IsDebug())
	assert.Equal(t, s, zeroTime)
	dur := GetLogDuration(IsDebug(), s)
	assert.Equal(t, dur, zeroDuration)
	err := SetLevel("debug")
	assert.NoError(t, err)
	assert.True(t, IsDebug())
	s = GetLogNow(IsDebug())
	assert.NotEqual(t, s, zeroTime)
	dur = GetLogDuration(IsDebug(), s)
	assert.NotEqual(t, dur, zeroDuration)
}

func TestGetLogSql(t *testing.T) {
	config.Init()
	sql := GetLogSql("insert into test values(1)")
	assert.Equal(t, sql, "insert into test values(1)")
	builder := strings.Builder{}
	for i := 0; i < MaxLogSqlLength+100; i++ {
		builder.WriteString("a")
	}
	str := builder.String()
	sql = GetLogSql(str)
	assert.Equal(t, sql, str[:MaxLogSqlLength])
}

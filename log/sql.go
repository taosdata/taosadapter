package log

import (
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/tools/pool"
)

var sqlLogger = logrus.New()

type TaosSqlLogFormatter struct {
}

func (t *TaosSqlLogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.Reset()
	b.WriteString(entry.Time.Format("01/02 15:04:05.000000"))
	b.WriteByte(' ')
	b.WriteString(ServerID)
	b.WriteByte(' ')
	b.WriteString(entry.Message)
	b.WriteByte('\n')
	return b.Bytes(), nil
}

package log

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"sync"

	rotatelogs "github.com/huskar-t/file-rotatelogs/v2"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/tools/pool"
)

var logger = logrus.New()
var ServerID = randomID()
var globalLogFormatter = &TaosLogFormatter{}

type FileHook struct {
	formatter logrus.Formatter
	writer    io.Writer
	buf       *bytes.Buffer
	sync.RWMutex
}

func NewFileHook(formatter logrus.Formatter, writer io.Writer) *FileHook {
	return &FileHook{formatter: formatter, writer: writer, buf: &bytes.Buffer{}}
}

func (f *FileHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (f *FileHook) Fire(entry *logrus.Entry) error {
	data, err := f.formatter.Format(entry)
	if err != nil {
		return err
	}
	f.Lock()
	defer f.Unlock()
	f.buf.Write(data)
	if f.buf.Len() > 1024 {
		_, err = f.writer.Write(f.buf.Bytes())
		f.buf.Reset()
		if err != nil {
			return err
		}
	}
	return nil
}

func ConfigLog() {
	err := SetLevel(config.Conf.LogLevel)
	if err != nil {
		panic(err)
	}
	writer, err := rotatelogs.New(
		path.Join(config.Conf.Log.Path, "taosadapter_%Y_%m_%d_%H_%M.log"),
		rotatelogs.WithRotationCount(config.Conf.Log.RotationCount),
		rotatelogs.WithRotationTime(config.Conf.Log.RotationTime),
		rotatelogs.WithRotationSize(int64(config.Conf.Log.RotationSize)),
	)
	if err != nil {
		panic(err)
	}
	hook := NewFileHook(globalLogFormatter, writer)
	logger.AddHook(hook)
	if config.Conf.Log.EnableRecordHttpSql {
		sqlWriter, err := rotatelogs.New(
			path.Join(config.Conf.Log.Path, "httpsql_%Y_%m_%d_%H_%M.log"),
			rotatelogs.WithRotationCount(config.Conf.Log.SqlRotationCount),
			rotatelogs.WithRotationTime(config.Conf.Log.SqlRotationTime),
			rotatelogs.WithRotationSize(int64(config.Conf.Log.SqlRotationSize)),
		)
		if err != nil {
			panic(err)
		}
		sqlLogger.SetFormatter(&TaosSqlLogFormatter{})
		sqlLogger.SetOutput(sqlWriter)
	}
}

func SetLevel(level string) error {
	l, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	logger.SetLevel(l)
	return nil
}

func GetLogger(model string) *logrus.Entry {
	return logger.WithFields(logrus.Fields{"model": model})
}
func init() {
	logger.SetFormatter(globalLogFormatter)
}

func randomID() string {
	return fmt.Sprintf("%08d", os.Getpid())
}

type TaosLogFormatter struct {
}

func (t *TaosLogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.Reset()
	b.WriteString(entry.Time.Format("01/02 15:04:05.000000"))
	b.WriteByte(' ')
	b.WriteString(ServerID)
	b.WriteString(" TAOS_ADAPTER ")
	b.WriteString(entry.Level.String())
	b.WriteString(` "`)
	b.WriteString(entry.Message)
	b.WriteByte('"')
	for k, v := range entry.Data {
		b.WriteByte(' ')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(fmt.Sprintf("%v", v))
	}
	b.WriteByte('\n')
	return b.Bytes(), nil
}

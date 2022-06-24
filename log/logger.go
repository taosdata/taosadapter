package log

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"sync"

	rotatelogs "github.com/huskar-t/file-rotatelogs/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/tools/pool"
)

var logger = logrus.New()
var ServerID = randomID()
var globalLogFormatter = &TaosLogFormatter{}

var (
	TotalRequest *prometheus.GaugeVec

	FailRequest *prometheus.GaugeVec

	RequestInFlight prometheus.Gauge

	RequestSummery *prometheus.SummaryVec
)

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

var once sync.Once

func ConfigLog() {
	once.Do(func() {
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

		TotalRequest = promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "taosadapter",
				Subsystem: "restful",
				Name:      "http_request_total",
				Help:      "The total number of processed http requests",
			}, []string{"status_code", "client_ip", "request_method", "request_uri"})

		FailRequest = promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "taosadapter",
				Subsystem: "restful",
				Name:      "http_request_fail",
				Help:      "The number of failures of http request processing",
			}, []string{"status_code", "client_ip", "request_method", "request_uri"})

		RequestInFlight = promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "taosadapter",
				Subsystem: "restful",
				Name:      "http_request_in_flight",
				Help:      "Current number of in-flight http requests",
			},
		)

		RequestSummery = promauto.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace:  "taosadapter",
				Subsystem:  "restful",
				Name:       "http_request_summary_milliseconds",
				Help:       "Summary of latencies for http requests in millisecond",
				Objectives: map[float64]float64{0.1: 0.001, 0.2: 0.002, 0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
				MaxAge:     config.Conf.Monitor.WriteInterval,
			}, []string{"request_method", "request_uri"})
	})
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

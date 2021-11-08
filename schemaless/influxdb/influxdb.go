package influxdb

import (
	"crypto/md5"
	"fmt"
	"sort"
	"time"
	"unsafe"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/taosdata/taosadapter/log"
	"github.com/taosdata/taosadapter/schemaless"
	"github.com/taosdata/taosadapter/tools/pool"
)

type Result struct {
	SuccessCount int
	FailCount    int
	ErrorList    []string
}

var logger = log.GetLogger("schemaless").WithField("protocol", "influxdb")

func InsertInfluxdb(taosConnect unsafe.Pointer, data []byte, db, precision string) (*Result, error) {
	result := &Result{}
	executor, err := schemaless.NewExecutor(taosConnect)
	if err != nil {
		return result, err
	}
	points, err := models.ParsePointsWithPrecision(data, time.Now().UTC(), precision)
	if err != nil {
		return result, err
	}
	lines := make([]*schemaless.InsertLine, len(points))
	result.ErrorList = make([]string, len(points))
	b := pool.BytesPoolGet()
	for i, point := range points {
		b.Reset()
		name := point.Name()
		tags := point.Tags()
		sort.Sort(tags)
		b.Write(name)
		b.WriteByte(' ')
		b.WriteString(tags.String())
		tableName := fmt.Sprintf("_%x", md5.Sum(b.Bytes()))
		b.Reset()
		fields, err := point.Fields()
		if err != nil {
			result.ErrorList[i] = err.Error()
			continue
		}
		tagNames := tags.Keys()
		tagValues := tags.Values()
		b.WriteByte('`')
		b.Write(name)
		b.WriteByte('`')
		lines[i] = &schemaless.InsertLine{
			DB:         db,
			Ts:         point.Time(),
			TableName:  tableName,
			STableName: b.String(),
			Fields:     fields,
			TagNames:   tagNames,
			TagValues:  tagValues,
		}
	}
	pool.BytesPoolPut(b)
	for i, line := range lines {
		if line == nil {
			continue
		}
		sql, err := executor.InsertTDengine(line)
		if err != nil {
			logger.WithError(err).Errorln(sql)
			result.FailCount += 1
			result.ErrorList[i] = err.Error()
		} else {
			result.SuccessCount += 1
		}
	}
	return result, nil
}

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
	"github.com/taosdata/taosadapter/schemaless/proto"
	"github.com/taosdata/taosadapter/tools/pool"
)

var logger = log.GetLogger("schemaless").WithField("protocol", "influxdb")

func InsertInfluxdb(taosConnect unsafe.Pointer, data []byte, db, precision string) (*proto.InfluxResult, error) {
	result := &proto.InfluxResult{}
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
	tmpB := pool.BytesPoolGet()
	for i, point := range points {
		b.Reset()
		b.WriteByte('`')
		b.Write(point.Name())
		b.WriteByte('`')
		stableName := b.String()
		tags := point.Tags()
		sort.Sort(tags)
		tagNames := make([]string, tags.Len())
		tagValues := make([]string, tags.Len())
		b.Reset()
		b.WriteString(stableName)
		for i, tag := range tags {
			tmpB.Reset()

			tmpB.WriteByte('`')
			tmpB.WriteString(string(tag.Key))
			tmpB.WriteByte('`')
			name := tmpB.String()
			pool.BytesPoolPut(tmpB)
			tagNames[i] = name
			tagValues[i] = string(tag.Value)
			b.WriteByte(',')
			b.WriteString(name)
			b.WriteByte('=')
			b.WriteString(string(tag.Value))
		}
		tableName := fmt.Sprintf("t_%x", md5.Sum(b.Bytes()))
		fields, err := point.Fields()
		if err != nil {
			result.ErrorList[i] = err.Error()
			continue
		}
		lines[i] = &schemaless.InsertLine{
			DB:         db,
			Ts:         point.Time(),
			TableName:  tableName,
			STableName: stableName,
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

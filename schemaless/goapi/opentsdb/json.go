package opentsdb

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
	"unsafe"

	"github.com/taosdata/taosadapter/schemaless"
	"github.com/taosdata/taosadapter/tools/pool"
)

type PutData struct {
	Metric    string            `json:"metric"`
	Timestamp int64             `json:"timestamp"`
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags"`
}

const (
	arrayJson = iota + 1
	objectJson
)

func InsertOpentsdbJson(taosConnect unsafe.Pointer, data []byte, db string) error {
	if len(data) == 0 {
		return fmt.Errorf("empty data")
	}
	executor, err := schemaless.NewExecutor(taosConnect)
	if err != nil {
		return err
	}
	var jsonType = 0
	for _, d := range data {
		if unicode.IsSpace(rune(d)) {
			continue
		} else if d == '{' {
			jsonType = objectJson
			break
		} else if d == '[' {
			jsonType = arrayJson
			break
		} else {
			return fmt.Errorf("unavailable json data")
		}
	}
	var putData []*PutData
	switch jsonType {
	case arrayJson:
		err := json.Unmarshal(data, &putData)
		if err != nil {
			return err
		}
	case objectJson:
		var singleData PutData
		err := json.Unmarshal(data, &singleData)
		if err != nil {
			return err
		}
		putData = []*PutData{&singleData}
	default:
		return fmt.Errorf("unavailable json data")
	}
	b := pool.BytesPoolGet()
	var errList = make([]string, len(putData))
	defer pool.BytesPoolPut(b)
	haveError := false
	for pointIndex, point := range putData {
		b.Reset()
		var ts time.Time
		if point.Timestamp < 10000000000 {
			//second
			ts = time.Unix(point.Timestamp, 0)
		} else {
			//millisecond
			ts = time.Unix(0, point.Timestamp*1e6)
		}
		b.WriteByte('`')
		b.WriteString(point.Metric)
		b.WriteByte('`')
		stableName := b.String()
		tagNames := make([]string, len(point.Tags))
		tagValues := make([]string, len(point.Tags))
		index := 0
		for k := range point.Tags {
			tagNames[index] = k
			index += 1
		}
		sort.Strings(tagNames)
		for i, tagName := range tagNames {
			tagValues[i] = point.Tags[tagName]
		}
		b.Reset()
		b.WriteString(stableName)
		tmpB := pool.BytesPoolGet()
		for i, tag := range tagNames {
			tmpB.Reset()

			tmpB.WriteByte('`')
			tmpB.WriteString(tag)
			tmpB.WriteByte('`')
			name := tmpB.String()
			tagNames[i] = name
			tagValues[i] = point.Tags[tag]
			b.WriteByte(',')
			b.WriteString(name)
			b.WriteByte('=')
			b.WriteString(point.Tags[tag])
		}
		pool.BytesPoolPut(tmpB)
		tableName := fmt.Sprintf("t_%x", md5.Sum(b.Bytes()))

		sql, err := executor.InsertTDengine(&schemaless.InsertLine{
			DB:         db,
			Ts:         ts,
			TableName:  tableName,
			STableName: stableName,
			Fields: map[string]interface{}{
				valueField: point.Value,
			},
			TagNames:  tagNames,
			TagValues: tagValues,
		})
		if err != nil {
			logger.WithError(err).Errorln(sql)
			errList[pointIndex] = err.Error()
			if !haveError {
				haveError = true
			}
		}
	}
	if haveError {
		return errors.New(strings.Join(errList, ","))
	}
	return nil
}

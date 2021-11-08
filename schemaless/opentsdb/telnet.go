package opentsdb

import (
	"crypto/md5"
	"fmt"
	"unsafe"

	"github.com/taosdata/taosadapter/schemaless"
	"github.com/taosdata/taosadapter/schemaless/parser/opentsdb/telnet"
	"github.com/taosdata/taosadapter/tools/pool"
)

func InsertTelnet(taosConnect unsafe.Pointer, data, db string) error {
	executor, err := schemaless.NewExecutor(taosConnect)
	if err != nil {
		return err
	}
	point, err := telnet.Unmarshal(data)
	if err != nil {
		return err
	}
	defer telnet.CleanPoint(point)
	tagNames := make([]string, len(point.Tags))
	tagValues := make([]string, len(point.Tags))
	for i, tag := range point.Tags {
		tagNames[i] = tag.Key
		tagValues[i] = tag.Value
	}
	b := pool.BytesPoolGet()
	b.WriteString(point.Metric)
	b.WriteByte(' ')
	b.WriteString(point.Tags.String())
	tableName := fmt.Sprintf("_%x", md5.Sum(b.Bytes()))
	b.Reset()
	b.WriteByte('`')
	b.WriteString(point.Metric)
	b.WriteByte('`')
	sql, err := executor.InsertTDengine(&schemaless.InsertLine{
		DB:         db,
		Ts:         point.Ts,
		TableName:  tableName,
		STableName: b.String(),
		Fields: map[string]interface{}{
			valueField: point.Value,
		},
		TagNames:  tagNames,
		TagValues: tagValues,
	})
	if err != nil {
		logger.WithError(err).Errorln(sql)
		return err
	}
	return nil
}

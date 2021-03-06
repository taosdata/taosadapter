package prometheus

import (
	"bytes"
	"crypto/md5"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
	"unsafe"

	"github.com/prometheus/prometheus/prompb"
	"github.com/taosdata/driver-go/v2/common"
	tErrors "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/db/async"
	"github.com/taosdata/taosadapter/db/tool"
	"github.com/taosdata/taosadapter/thread"
	"github.com/taosdata/taosadapter/tools/pool"
)

func processWrite(taosConn unsafe.Pointer, req *prompb.WriteRequest, db string) error {
	start := time.Now()
	err := tool.SelectDB(taosConn, db)
	if err != nil {
		return err
	}
	logger.Debug("processWrite SelectDB cost:", time.Now().Sub(start))
	start = time.Now()
	sql, err := generateWriteSql(req.GetTimeseries())
	if err != nil {
		return err
	}
	logger.Debug("generateWriteSql cost:", time.Now().Sub(start))
	start = time.Now()
	err = async.GlobalAsync.TaosExecWithoutResult(taosConn, sql)
	logger.Debug("processWrite TaosExecWithoutResult cost:", time.Now().Sub(start))
	if err != nil {
		if tErr, is := err.(*tErrors.TaosError); is {
			start = time.Now()
			if tErr.Code == tErrors.MND_INVALID_TABLE_NAME {
				err := async.GlobalAsync.TaosExecWithoutResult(taosConn, "create stable if not exists metrics(ts timestamp,value double) tags (labels json)")
				if err != nil {
					return err
				}
				// retry
				err = async.GlobalAsync.TaosExecWithoutResult(taosConn, sql)
				if err != nil {
					logger.WithError(err).Error(sql)
					return err
				}
				logger.Debug("retry processWrite cost", time.Now().Sub(start))
				return nil
			} else {
				logger.WithError(err).Error(sql)
				return err
			}
		} else {
			logger.WithError(err).Error(sql)
			return err
		}
	}
	return nil
}

func generateWriteSql(timeseries []prompb.TimeSeries) (string, error) {
	sql := pool.StringBuilderPoolGet()
	defer pool.StringBuilderPoolPut(sql)
	sql.WriteString("insert into ")
	tmp := pool.BytesPoolGet()
	defer pool.BytesPoolPut(tmp)
	for _, timeSeriesData := range timeseries {
		tagName := make([]string, len(timeSeriesData.Labels))
		tagMap := make(map[string]string, len(timeSeriesData.Labels))
		for i, label := range timeSeriesData.GetLabels() {
			tagName[i] = label.Name
			tagMap[label.Name] = label.Value
		}
		sort.Strings(tagName)
		tmp.Reset()
		for i, s := range tagName {
			v := tagMap[s]
			tmp.WriteString(s)
			tmp.WriteByte('=')
			tmp.WriteString(v)
			if i != len(tagName)-1 {
				tmp.WriteByte(',')
			}
		}
		labelsJson, err := json.Marshal(tagMap)
		if err != nil {
			return "", err
		}
		tableName := fmt.Sprintf("t_%x", md5.Sum(tmp.Bytes()))
		sql.WriteString(tableName)
		sql.WriteString(" using metrics tags('")
		sql.Write(escapeBytes(labelsJson))
		sql.WriteString("') values")
		for _, sample := range timeSeriesData.Samples {
			sql.WriteString("('")
			sql.WriteString(time.Unix(0, sample.GetTimestamp()*1e6).UTC().Format(time.RFC3339Nano))
			sql.WriteString("',")
			if math.IsNaN(sample.GetValue()) {
				sql.WriteString("null")
			} else if math.IsInf(sample.GetValue(), 0) {
				sql.WriteString("null")
			} else {
				fmt.Fprintf(sql, "%v", sample.GetValue())
			}
			sql.WriteString(") ")
		}
	}
	return sql.String(), nil
}

func processRead(taosConn unsafe.Pointer, req *prompb.ReadRequest, db string) (resp *prompb.ReadResponse, err error) {
	start := time.Now()
	thread.Lock()
	wrapper.TaosSelectDB(taosConn, db)
	thread.Unlock()
	logger.Debug("processRead SelectDB cost:", time.Now().Sub(start))
	resp = &prompb.ReadResponse{}
	for i, query := range req.Queries {
		start = time.Now()
		sql, err := generateReadSql(query)
		logger.Debug("processRead generateReadSql cost:", time.Now().Sub(start))
		if err != nil {
			return nil, err
		}
		start = time.Now()
		data, err := async.GlobalAsync.TaosExec(taosConn, sql, func(ts int64, precision int) driver.Value {
			switch precision {
			case common.PrecisionMilliSecond:
				return ts
			case common.PrecisionMicroSecond:
				return ts / 1e3
			case common.PrecisionNanoSecond:
				return ts / 1e6
			default:
				return 0
			}
		})
		if err != nil {
			logger.WithError(err).Error(sql)
			return nil, err
		}
		logger.Debug("processRead TaosExec cost:", time.Now().Sub(start))
		//ts value labels time.Time float64 []byte
		start = time.Now()
		group := map[string]*prompb.TimeSeries{}
		for _, d := range data.Data {
			if len(d) != 4 {
				continue
			}
			if d[0] == nil || d[1] == nil || d[2] == nil || d[3] == nil {
				continue
			}
			ts := d[0].(int64)
			value := d[1].(float64)
			var tags map[string]string
			err = json.Unmarshal(d[2].(json.RawMessage), &tags)
			if err != nil {
				return nil, err
			}
			tbName := d[3].(string)
			timeSeries, exist := group[tbName]
			if exist {
				timeSeries.Samples = append(timeSeries.Samples, prompb.Sample{
					Value:     value,
					Timestamp: ts,
				})
			} else {
				timeSeries = &prompb.TimeSeries{
					Samples: []prompb.Sample{
						{
							Value:     value,
							Timestamp: ts,
						},
					},
				}
				timeSeries.Labels = make([]prompb.Label, 0, len(tags))
				for name, tagValue := range tags {
					timeSeries.Labels = append(timeSeries.Labels, prompb.Label{
						Name:  name,
						Value: tagValue,
					})
				}
				group[tbName] = timeSeries
			}
		}
		if len(group) > 0 {
			resp.Results = append(resp.Results, &prompb.QueryResult{Timeseries: make([]*prompb.TimeSeries, 0, len(group))})
		}
		for _, series := range group {
			resp.Results[i].Timeseries = append(resp.Results[i].Timeseries, series)
		}
		logger.Debug("processRead process result cost:", time.Now().Sub(start))
	}
	return resp, err
}

func generateReadSql(query *prompb.Query) (string, error) {
	sql := pool.StringBuilderPoolGet()
	defer pool.StringBuilderPoolPut(sql)
	sql.WriteString("select *,tbname from metrics where ts >= '")
	sql.WriteString(ms2Time(query.GetStartTimestampMs()))
	sql.WriteString("' and ts <= '")
	sql.WriteString(ms2Time(query.GetEndTimestampMs()))
	sql.WriteByte('\'')
	for _, matcher := range query.GetMatchers() {
		sql.WriteString(" and ")
		k := escapeString(matcher.GetName())
		v := escapeString(matcher.GetValue())
		sql.WriteString("labels->'")
		sql.WriteString(k)
		switch matcher.Type {
		case prompb.LabelMatcher_EQ:
			sql.WriteString("' = '")
		case prompb.LabelMatcher_NEQ:
			sql.WriteString("' != '")
		case prompb.LabelMatcher_RE:
			sql.WriteString("' match '")
		case prompb.LabelMatcher_NRE:
			sql.WriteString("' nmatch '")
		default:
			return "", errors.New("not support match type")
		}
		sql.WriteString(v)
		sql.WriteByte('\'')
	}
	if config.Conf.RestfulRowLimit > -1 {
		sql.WriteString(" limit ")
		fmt.Fprintf(sql, "%d", config.Conf.RestfulRowLimit)
	}

	return sql.String(), nil
}

func ms2Time(ts int64) string {
	return time.Unix(0, ts*1e6).UTC().Format(time.RFC3339Nano)
}

func escapeString(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `'`, `\'`)
}

var escapeSingleQuoteOld = []byte{'\''}
var escapeSingleQuoteNew = []byte{'\\', '\''}
var escapeBackslashOld = []byte{'\\'}
var escapeBackslashNew = []byte{'\\', '\\'}

func escapeBytes(s []byte) []byte {
	return bytes.ReplaceAll(bytes.ReplaceAll(s, escapeBackslashOld, escapeBackslashNew), escapeSingleQuoteOld, escapeSingleQuoteNew)
}

package query

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"math"
	"sort"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/taosdata/driver-go/v2/common"
	"github.com/taosdata/taosadapter/db/async"
	"github.com/taosdata/taosadapter/db/commonpool"
	"github.com/taosdata/taosadapter/log"
	"github.com/taosdata/taosadapter/tools/pool"
)

var logger = log.GetLogger("query")

var nothing = struct{}{}

type Adapter struct {
	client *commonpool.Conn
}

func ms2Time(t int64) time.Time {
	return time.Unix(t/1000, (t%1000)*1e6).UTC()
}

func (q *Adapter) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	var startT, endT time.Time
	if mint == 0 && maxt == 0 {
		endT = time.Now()
		startT = time.Now().Add(-time.Minute * 5)
	} else {
		if mint < 0 {
			mint = 0
		}
		if maxt > math.MaxInt64 {
			maxt = math.MaxInt64
		}
		startT = ms2Time(mint)
		endT = ms2Time(maxt)
	}
	return &Querier{startT: startT, endT: endT, client: q.client}, nil
}

type Querier struct {
	startT time.Time
	endT   time.Time
	client *commonpool.Conn
}

func (q *Querier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	var err error
	sqlBuilder := pool.BytesPoolGet()
	defer pool.BytesPoolPut(sqlBuilder)
	sqlBuilder.WriteString("select max(value) from metrics where ts >= '")
	sqlBuilder.WriteString(q.startT.Format(time.RFC3339Nano))
	sqlBuilder.WriteString("' and ts <= '")
	sqlBuilder.WriteString(q.endT.Format(time.RFC3339Nano))
	sqlBuilder.WriteString("' and labels contains '")
	sqlBuilder.WriteString(name)
	sqlBuilder.WriteString("' group by labels->'")
	sqlBuilder.WriteString(name)
	sqlBuilder.WriteByte('\'')
	err = generateMatcherQuery(sqlBuilder, matchers)
	if err != nil {
		return nil, nil, err
	}
	sql := sqlBuilder.String()
	data, err := async.GlobalAsync.TaosExec(q.client.TaosConnection, sql, nil)
	if err != nil {
		return nil, nil, err
	}
	values := make([]string, 0, len(data.Data))
	for _, value := range data.Data {
		values = append(values, jsoniter.Get(value[1].(json.RawMessage)).ToString())
	}
	return values, nil, nil
}

func (q *Querier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	var err error
	sqlBuilder := pool.BytesPoolGet()
	defer pool.BytesPoolPut(sqlBuilder)
	sqlBuilder.WriteString("select labels,tbname,max(value) from metrics where ts >= '")
	sqlBuilder.WriteString(q.startT.Format(time.RFC3339Nano))
	sqlBuilder.WriteString("' and ts <= '")
	sqlBuilder.WriteString(q.endT.Format(time.RFC3339Nano))
	sqlBuilder.WriteString("' group by tbname")
	err = generateMatcherQuery(sqlBuilder, matchers)
	if err != nil {
		return nil, nil, err
	}
	sql := sqlBuilder.String()
	data, err := async.GlobalAsync.TaosExec(q.client.TaosConnection, sql, nil)
	if err != nil {
		return nil, nil, err
	}

	labelKeys := map[string]struct{}{}
	for _, label := range data.Data {
		keys := jsoniter.Get(label[0].(json.RawMessage)).Keys()
		for _, key := range keys {
			labelKeys[key] = nothing
		}
	}
	keys := make([]string, 0, len(labelKeys))
	for k := range labelKeys {
		keys = append(keys, k)
	}
	return keys, nil, nil
}

func generateMatcherQuery(sqlBuilder *bytes.Buffer, matchers []*labels.Matcher) error {
	for _, matcher := range matchers {
		sqlBuilder.WriteString(" and labels->'")
		sqlBuilder.WriteString(matcher.Name)
		sqlBuilder.WriteString("' ")
		op := ""
		switch matcher.Type {
		case labels.MatchEqual:
			op = "="
		case labels.MatchNotEqual:
			op = "!="
		case labels.MatchRegexp:
			op = "match"
		case labels.MatchNotRegexp:
			op = "nmatch"
		default:
			return errors.New("not support match type")
		}
		sqlBuilder.WriteString(op)
		sqlBuilder.WriteString(" '")
		sqlBuilder.WriteString(matcher.Value)
		sqlBuilder.WriteByte('\'')
	}
	return nil
}

func (q *Querier) Close() error {
	return nil
}

func (q *Querier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	var err error
	st := q.startT
	et := q.endT
	if hints != nil {
		st = ms2Time(hints.Start)
		et = ms2Time(hints.End)
	}
	sqlBuilder := pool.BytesPoolGet()
	defer pool.BytesPoolPut(sqlBuilder)
	sqlBuilder.WriteString("select *,tbname from metrics where ts >= '")
	sqlBuilder.WriteString(st.Format(time.RFC3339Nano))
	sqlBuilder.WriteString("' and ts <= '")
	sqlBuilder.WriteString(et.Format(time.RFC3339Nano))
	sqlBuilder.WriteByte('\'')
	err = generateMatcherQuery(sqlBuilder, matchers)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	sql := sqlBuilder.String()
	data, err := async.GlobalAsync.TaosExec(q.client.TaosConnection, sql, func(ts int64, precision int) driver.Value {
		switch precision {
		case common.PrecisionNanoSecond:
			return ts / 1e6
		case common.PrecisionMicroSecond:
			return ts / 1e3
		case common.PrecisionMilliSecond:
			return ts
		default:
			panic("unknown precision")
		}
	})
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	//time series
	group := map[string]*concreteSeries{}
	for _, d := range data.Data {
		//ts,value,labels,tbname
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
			return storage.ErrSeriesSet(err)
		}
		tbName := d[3].(string)
		timeSeries, exist := group[tbName]

		if exist {
			timeSeries.samples = append(timeSeries.samples, prompb.Sample{
				Value:     value,
				Timestamp: ts,
			})
		} else {
			timeSeries = &concreteSeries{
				samples: []prompb.Sample{
					{
						Value:     value,
						Timestamp: ts,
					},
				},
			}
			timeSeries.labels = make(labels.Labels, 0, len(tags))
			for tagK, tagV := range tags {
				timeSeries.labels = append(timeSeries.labels, labels.Label{
					Name:  tagK,
					Value: tagV,
				})
			}
			sort.Sort(timeSeries.labels)
			group[tbName] = timeSeries
		}
	}
	series := make([]storage.Series, 0, len(group))
	for _, item := range group {
		series = append(series, item)
	}
	if sortSeries {
		sort.Sort(byLabel(series))
	}
	return &concreteSeriesSet{
		series: series,
	}
}

// concreteSeries implements storage.Series.
type concreteSeries struct {
	labels  labels.Labels
	samples []prompb.Sample
}

func (c *concreteSeries) Labels() labels.Labels {
	return labels.New(c.labels...)
}

func (c *concreteSeries) Iterator() chunkenc.Iterator {
	return newConcreteSeriersIterator(c)
}

// concreteSeriesIterator implements storage.SeriesIterator.
type concreteSeriesIterator struct {
	cur    int
	series *concreteSeries
}

func newConcreteSeriersIterator(series *concreteSeries) chunkenc.Iterator {
	return &concreteSeriesIterator{
		cur:    -1,
		series: series,
	}
}

// Seek implements storage.SeriesIterator.
func (c *concreteSeriesIterator) Seek(t int64) bool {
	c.cur = sort.Search(len(c.series.samples), func(n int) bool {
		return c.series.samples[n].Timestamp >= t
	})
	return c.cur < len(c.series.samples)
}

// At implements storage.SeriesIterator.
func (c *concreteSeriesIterator) At() (t int64, v float64) {
	s := c.series.samples[c.cur]
	return s.Timestamp, s.Value
}

// Next implements storage.SeriesIterator.
func (c *concreteSeriesIterator) Next() bool {
	c.cur++
	return c.cur < len(c.series.samples)
}

// Err implements storage.SeriesIterator.
func (c *concreteSeriesIterator) Err() error {
	return nil
}

// concreteSeriesSet implements storage.SeriesSet.
type concreteSeriesSet struct {
	cur    int
	series []storage.Series
}

func (c *concreteSeriesSet) Next() bool {
	c.cur++
	return c.cur-1 < len(c.series)
}

func (c *concreteSeriesSet) At() storage.Series {
	return c.series[c.cur-1]
}

func (c *concreteSeriesSet) Err() error {
	return nil
}

func (c *concreteSeriesSet) Warnings() storage.Warnings { return nil }

package query

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/taosdata/taosadapter/db/commonpool"
)

const DefaultQueryMaxSamples = 50000000

type PromDataSource struct {
	queryMaxSamples int //50000000
	maxConcurrency  int
	timeout         time.Duration
	queryEngine     *promql.Engine
}

func NewPromDataSource(queryMaxSamples int, timeout time.Duration) *PromDataSource {
	if queryMaxSamples <= 0 {
		queryMaxSamples = DefaultQueryMaxSamples
	}
	return &PromDataSource{queryMaxSamples: queryMaxSamples, timeout: timeout}
}

func (pd *PromDataSource) Init() {
	opts := promql.EngineOpts{
		Reg:                  prometheus.DefaultRegisterer,
		MaxSamples:           pd.queryMaxSamples,
		Timeout:              pd.timeout,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	}
	queryEngine := promql.NewEngine(opts)
	pd.queryEngine = queryEngine
}

func (pd *PromDataSource) NewRangeQuery(client *commonpool.Conn, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	q := &Adapter{
		client: client,
	}
	return pd.queryEngine.NewRangeQuery(q, qs, start, end, interval)
}

func (pd *PromDataSource) NewInstantQuery(client *commonpool.Conn, qs string, ts time.Time) (promql.Query, error) {
	q := &Adapter{
		client: client,
	}
	return pd.queryEngine.NewInstantQuery(q, qs, ts)
}

func (pd *PromDataSource) Querier(client *commonpool.Conn, ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	q := &Adapter{
		client: client,
	}
	return q.Querier(ctx, mint, maxt)
}

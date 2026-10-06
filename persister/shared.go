package persister

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
	"go.uber.org/zap"
)

// MetricStore is the persister's write surface. The app owns its lifetime.
type MetricStore interface {
	Metadata(context.Context, string) (store.Metadata, error)
	Create(context.Context, store.MetricConfig) (store.Metadata, error)
	UpdateMany(context.Context, string, []points.Point) error
}

func (p *Whisper) SetMetricStore(metricStore MetricStore) { p.metricStore = metricStore }

func (p *Whisper) storeShared(metric string) {
	ctx := context.Background()
	_, err := p.metricStore.Metadata(ctx, metric)
	if errors.Is(err, store.ErrNotFound) {
		if !p.createSharedMetric(ctx, metric) {
			return
		}
		err = nil
	}
	if err != nil {
		p.logger.Error("prepare shared metric", zap.String("metric", metric), zap.Error(err))
		return
	}
	values, exists := p.pop(metric)
	if !exists {
		return
	}
	if err := p.metricStore.UpdateMany(ctx, metric, values.Data); err != nil {
		p.logger.Error("write shared metric", zap.String("metric", metric), zap.Error(err))
		if p.requeue != nil {
			p.requeue(values)
		}
		return
	}
	// Periodic-sync stores can confirm updates before the next WAL sync.
	// Confirmed points are then subject to the configured crash-loss window.
	atomic.AddUint32(&p.committedPoints, uint32(len(values.Data)))
	atomic.AddUint32(&p.updateOperations, 1)
	if p.confirm != nil {
		p.confirm(values)
	}
	if p.tagsEnabled && p.taggedFn != nil && strings.Contains(metric, ";") {
		p.taggedFn(metric, false)
	}
}

func (p *Whisper) createSharedMetric(ctx context.Context, metric string) bool {
	schema, ok := p.schemas.Match(metric)
	if !ok {
		p.logger.Error("no storage schema defined for metric", zap.String("metric", metric))
		return false
	}
	aggr := p.aggregation.Match(metric)
	if aggr == nil {
		p.logger.Error("no storage aggregation defined for metric", zap.String("metric", metric))
		return false
	}
	retentions := make([]store.Retention, len(schema.Retentions))
	for i, retention := range schema.Retentions {
		retentions[i] = store.Retention{Step: retention.SecondsPerPoint(), Count: retention.NumberOfPoints()}
	}
	_, err := p.metricStore.Create(ctx, store.MetricConfig{
		Name: metric, Retentions: retentions,
		AggregationMethod: store.AggregationMethod(aggr.aggregationMethod), XFilesFactor: float32(aggr.xFilesFactor),
	})
	if err == nil {
		atomic.AddUint32(&p.created, 1)
		if p.tagsEnabled && p.taggedFn != nil && strings.Contains(metric, ";") {
			p.taggedFn(metric, true)
		}
	} else if errors.Is(err, store.ErrExists) {
		err = nil
	}
	if err != nil {
		p.logger.Error("prepare shared metric", zap.String("metric", metric), zap.Error(err))
		return false
	}
	return true
}

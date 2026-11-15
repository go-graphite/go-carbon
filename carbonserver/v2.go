package carbonserver

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/daemonstore"
)

// SetSnapshotReader installs the daemon's immutable (possibly mixed legacy)
// reader before any listener starts. It remains fixed for the listener lifetime.
func (listener *CarbonserverListener) SetSnapshotReader(reader daemonstore.Reader) {
	listener.snapshotReader = reader
}

type snapshotSeries struct{ *daemonstore.Series }

func (s snapshotSeries) FromTime() int     { return s.From }
func (s snapshotSeries) UntilTime() int    { return s.Until }
func (s snapshotSeries) Step() int         { return s.Series.Step }
func (s snapshotSeries) Values() []float64 { return s.Series.Values }

func (listener *CarbonserverListener) fetchSnapshot(ctx context.Context, name string, from, until int32) (*metricFromDisk, error) {
	started := time.Now()
	s, err := listener.snapshotReader.Fetch(ctx, name, int(from), int(until))
	if errors.Is(err, daemonstore.ErrNotFound) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if s == nil {
		return &metricFromDisk{}, nil
	}
	wait := time.Since(started)
	atomic.AddUint64(&listener.metrics.DiskRequests, 1)
	listener.prometheus.diskRequest()
	atomic.AddUint64(&listener.metrics.MetricsReturned, 1)
	listener.prometheus.returnedMetric()
	atomic.AddUint64(&listener.metrics.DiskWaitTimeNS, uint64(wait.Nanoseconds()))
	listener.prometheus.diskWaitDuration(wait)
	atomic.AddUint64(&listener.metrics.PointsReturned, uint64(len(s.Values)))
	listener.prometheus.returnedPoint(len(s.Values))
	if listener.capacityReadLatency != nil {
		listener.capacityReadLatency.Observe(wait)
	}
	return &metricFromDisk{DiskStartTime: started, Timeseries: snapshotSeries{s}, Metadata: Metadata{ConsolidationFunc: titleizeAggrMethod(s.Metric.AggregationMethod), XFilesFactor: s.Metric.XFilesFactor}}, nil
}

func snapshotMetricInfo(m daemonstore.Metric) metricInfo {
	info := metricInfo{aggregationMethod: titleizeAggrMethod(m.AggregationMethod), xFilesFactor: m.XFilesFactor}
	for _, a := range m.Archives {
		info.retentions = append(info.retentions, retentionInfo{secondsPerPoint: a.Step, numberOfPoints: a.Points})
		info.maxRetention = max(info.maxRetention, int64(a.Step)*int64(a.Points))
	}
	return info
}

func (listener *CarbonserverListener) snapshotGlob(ctx context.Context, query string) *ExpandedGlobResponse {
	matches, err := listener.snapshotReader.Find(ctx, query)
	result := &ExpandedGlobResponse{Name: query, Err: err}
	for _, m := range matches {
		result.Files = append(result.Files, m.Name)
		result.Leafs = append(result.Leafs, m.Leaf)
	}
	return result
}

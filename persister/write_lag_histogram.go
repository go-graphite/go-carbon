package persister

import (
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	whisper "github.com/go-graphite/go-whisper"
)

const writeLagHistogramBucketCount = 30

var writeLagHistogramBuckets = prometheus.ExponentialBuckets(time.Millisecond.Seconds(), 2, writeLagHistogramBucketCount)

type writeLagHistogram struct {
	desc    *prometheus.Desc
	created time.Time
	bounds  []float64

	mu      sync.Mutex
	count   uint64
	sum     float64
	buckets [writeLagHistogramBucketCount]uint64
}

func newWriteLagHistogram() *writeLagHistogram {
	return &writeLagHistogram{
		desc: prometheus.NewDesc(
			"out_of_order_write_lag_exp",
			"Lag for incoming datapoints (exponential buckets)",
			nil,
			nil,
		),
		created: time.Now(),
		bounds:  writeLagHistogramBuckets,
	}
}

func (h *writeLagHistogram) Describe(ch chan<- *prometheus.Desc) {
	ch <- h.desc
}

func (h *writeLagHistogram) Collect(ch chan<- prometheus.Metric) {
	h.mu.Lock()
	count, sum, counts := h.count, h.sum, h.buckets
	h.mu.Unlock()

	buckets := make(map[float64]uint64, len(h.bounds))
	var cumulative uint64
	for i, upperBound := range h.bounds {
		cumulative += counts[i]
		buckets[upperBound] = cumulative
	}
	ch <- prometheus.MustNewConstHistogramWithCreatedTimestamp(h.desc, count, sum, buckets, h.created)
}

func (h *writeLagHistogram) observeDuration(lag time.Duration) {
	h.observeValue(lag.Seconds())
}

func (h *writeLagHistogram) observePoints(points []*whisper.TimeSeriesPoint, now time.Time) {
	if len(points) == 0 {
		return
	}

	// Aggregate locally so each persistence batch merges once instead of using
	// shared per-point atomics; Collect snapshots the same mutex-protected state.
	var buckets [writeLagHistogramBucketCount]uint64
	minBucket, maxBucket := writeLagHistogramBucketCount, -1
	var sum float64
	for _, point := range points {
		lag := now.Sub(time.Unix(int64(point.Time), 0)).Seconds()
		sum += lag
		if bucket := sort.SearchFloat64s(h.bounds, lag); bucket < len(h.bounds) {
			buckets[bucket]++
			if bucket < minBucket {
				minBucket = bucket
			}
			if bucket > maxBucket {
				maxBucket = bucket
			}
		}
	}
	h.merge(uint64(len(points)), sum, &buckets, minBucket, maxBucket)
}

func (h *writeLagHistogram) observeValue(value float64) {
	var buckets [writeLagHistogramBucketCount]uint64
	minBucket, maxBucket := writeLagHistogramBucketCount, -1
	if bucket := sort.SearchFloat64s(h.bounds, value); bucket < len(h.bounds) {
		buckets[bucket] = 1
		minBucket, maxBucket = bucket, bucket
	}
	h.merge(1, value, &buckets, minBucket, maxBucket)
}

func (h *writeLagHistogram) merge(count uint64, sum float64, buckets *[writeLagHistogramBucketCount]uint64, minBucket, maxBucket int) {
	h.mu.Lock()
	h.count += count
	h.sum += sum
	for i := minBucket; i <= maxBucket; i++ {
		h.buckets[i] += buckets[i]
	}
	h.mu.Unlock()
}

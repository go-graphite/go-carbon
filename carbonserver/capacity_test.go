package carbonserver

import (
	"context"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/hostcapacity"
)

func TestCapacityReadLatencyWithoutPrometheus(t *testing.T) {
	l := NewCarbonserverListener(nil)
	l.SetBuckets(10)
	l.timeBuckets = make([]uint64, 11)
	var reads hostcapacity.Latency
	l.SetCapacityReadLatency(&reads)
	for _, d := range []time.Duration{30, 60, 70} {
		l.bucketRequestTimes(d * time.Millisecond)
	}
	if _, count := reads.Snapshot().P99Since(hostcapacity.LatencySnapshot{}); count != 0 {
		t.Fatal("request wall time became storage pressure", count)
	}
	// Drive the real storage fetch path without enabling Prometheus.
	fixture, db, _, names, _ := newRangeScreenFixture(t, 60, 1<<20)
	fixture.SetCapacityReadLatency(&reads)
	if _, err := fixture.fetchMetricStoreSeries(context.Background(), db, names[0], rangeScreenNow-3600, rangeScreenNow); err != nil {
		t.Fatal(err)
	}
	if _, count := reads.Snapshot().P99Since(hostcapacity.LatencySnapshot{}); count != 1 {
		t.Fatal("storage fetch missing without Prometheus", count)
	}
}

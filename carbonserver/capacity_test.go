package carbonserver

import (
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
	l.bucketRequestTimes(25 * time.Millisecond)
	p99, count := reads.Snapshot().P99Since(hostcapacity.LatencySnapshot{})
	if count != 1 || p99 < 25*time.Millisecond {
		t.Fatal("read pressure depended on metrics being enabled", p99, count)
	}
}

package carbonserver

import (
	"math"
	"sync/atomic"
	"testing"
	"time"
)

func TestThroughputFoldBounds(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct{ quota, want int64 }{
		{0, 0}, {-1, 0}, {math.MaxInt64, 0},
		{1, 1}, {200_000, 1}, // small quotas keep exact enforcement
		{159_000_000, throughputFoldMax},
		{50_000_000, 50_000_000 / (throughputFoldShare * throughputStripes)},
	} {
		got := throughputFold(tt.quota)
		if got != tt.want {
			t.Errorf("throughputFold(%d) = %d, want %d", tt.quota, got, tt.want)
		}
		if tt.want > 0 && got*throughputStripes > max(throughputStripes, tt.quota/throughputFoldShare) {
			t.Errorf("quota %d: up to %d unfolded points exceed 1%%", tt.quota, got*throughputStripes)
		}
	}
}

func TestLimitedThroughputFoldsInBatches(t *testing.T) {
	t.Parallel()
	const quota = 159_000_000
	q := newThroughputUsagePerNamespace(0, &Quota{Throughput: quota}, &QuotaUsage{})
	r := q.dpRecorder()
	for range throughputFoldMax - 1 {
		q.increase(1, 3)
	}
	if got := atomic.LoadInt64(&r.dataPoints); got != 0 {
		t.Fatalf("folded %d points before the stripe filled", got)
	}
	if got := r.total(); got != throughputFoldMax-1 {
		t.Fatalf("total = %d, want %d", got, throughputFoldMax-1)
	}
	q.increase(1, 3)
	if got := atomic.LoadInt64(&r.dataPoints); got != throughputFoldMax {
		t.Fatalf("enforced usage = %d after a full stripe, want %d", got, throughputFoldMax)
	}
	if got := atomic.LoadInt64(&r.stripes[3].n); got != 0 {
		t.Fatalf("stripe kept %d points after folding", got)
	}
	// Enforcement still sees usage: fill the quota through many stripes.
	for i := uint64(0); atomic.LoadInt64(&r.dataPoints) < quota; i++ {
		q.increase(throughputFoldMax, i)
	}
	if q.withinQuota(1, time.Hour) {
		t.Fatal("quota not enforced once folded usage reached it")
	}
}

func TestSmallThroughputQuotaStaysExact(t *testing.T) {
	t.Parallel()
	q := newThroughputUsagePerNamespace(0, &Quota{Throughput: 3}, &QuotaUsage{})
	for i := range 3 {
		if !q.withinQuota(1, time.Hour) {
			t.Fatalf("point %d rejected below the quota", i)
		}
		q.increase(1, uint64(i))
	}
	if q.withinQuota(1, time.Hour) {
		t.Fatal("small quota not enforced exactly")
	}
}

// BenchmarkLimitedThroughputAccounting is the per-point accounting of one
// limited namespace shared by every ingesting CPU, as for a level-1 quota.
func BenchmarkLimitedThroughputAccounting(b *testing.B) {
	q := newThroughputUsagePerNamespace(0, &Quota{Throughput: math.MaxInt64 - 1}, &QuotaUsage{})
	var next atomic.Uint64
	b.RunParallel(func(pb *testing.PB) {
		stripe := next.Add(1) * 7
		for pb.Next() {
			if q.withinQuota(1, time.Minute) {
				q.increase(1, stripe)
			}
		}
	})
}

func BenchmarkQuotaCounterIncrease(b *testing.B) {
	for _, tt := range []struct {
		name  string
		quota int64
	}{{"small", 3}, {"folded", 159000000}, {"unlimited", math.MaxInt64}} {
		b.Run(tt.name, func(b *testing.B) {
			q := newThroughputUsagePerNamespace(0, &Quota{Throughput: tt.quota}, &QuotaUsage{})
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				q.increase(1, uint64(i))
			}
		})
	}
}

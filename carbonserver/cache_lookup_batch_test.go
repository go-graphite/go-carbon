package carbonserver

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
)

// The ordered cache sink must preserve the real quota implementation's usage,
// dropping policy and cached-route invalidation across rule reloads.
func TestCacheLookupBatchRealQuotaParity(t *testing.T) {
	for _, policy := range []QuotaDroppingPolicy{QDPNone, QDPNew} {
		var listeners [2]*CarbonserverListener
		var tries [2]*trieIndex
		var caches [2]*cache.Cache
		for i := range caches {
			listeners[i], tries[i] = routeListener(t, &Quota{Pattern: "/", Throughput: 11}, &Quota{Pattern: "a", Throughput: 7, DroppingPolicy: policy}, &Quota{Pattern: "a.b", Throughput: 3})
			tries[i].estimateSize = func(string) (int64, int64, int64) { return 1, 1, 1 }
			c := cache.New()
			c.SetMaxSize(0)
			c.EnableInputLog()
			c.SetThrottleWithHash(listeners[i].ShouldThrottleMetricWithHash)
			caches[i] = c
		}
		names := [][]byte{[]byte("a.b.one"), []byte("a.c.two"), []byte("a.c.two"), []byte("b.one"), []byte("a.one"), []byte("a.one"), []byte("plain"), []byte("a.new")}
		for cycle := range 8 {
			if cycle == 4 {
				for _, ti := range tries {
					if _, err := ti.applyQuotas(time.Hour, &Quota{Pattern: "/", Throughput: math.MaxInt64}, &Quota{Pattern: "a", Throughput: 9, DroppingPolicy: policy}); err != nil {
						t.Fatal(err)
					}
				}
			}
			data := make([]points.Point, len(names))
			for i := range data {
				data[i] = points.Point{Value: float64(cycle), Timestamp: int64(8 - cycle)}
			}
			for i, p := range data {
				caches[0].AddPoint(names[i], p.Value, p.Timestamp)
			}
			caches[1].AddPointBatch(names, data)
			for _, name := range names {
				if !reflect.DeepEqual(caches[0].Get(string(name)), caches[1].Get(string(name))) {
					t.Fatal("quota changed accepted points or duplicate/late order")
				}
			}
			if caches[0].Size() != caches[1].Size() || caches[0].PointBytes() != caches[1].PointBytes() || caches[0].EstimatedBytes() != caches[1].EstimatedBytes() {
				t.Fatal("quota changed cache accounting")
			}
			for _, path := range []string{"/", "a", "a.b"} {
				x, y := tries[0].throughputs.load(path), tries[1].throughputs.load(path)
				if (x == nil) != (y == nil) {
					t.Fatal("quota route removal differs")
				}
				if x != nil && (x.dpRecorder().total() != y.dpRecorder().total() || x.quotaUsage().Throttled != y.quotaUsage().Throttled) {
					t.Fatal("throughput/quota counters differ")
				}
			}
		}
	}
}

package carbonserver

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

// Restore the old registration behavior as an oracle: every quota namespace
// gets a throughput recorder, even when its throughput control is disabled.
func legacyMetricOnlyAccounting(t *testing.T, ti *trieIndex, rules []*Quota) {
	t.Helper()
	pending, depth, err := ti.resolveQuotas(rules)
	if err != nil {
		t.Fatal(err)
	}
	for path, change := range pending {
		if ti.throughputs.load(path) == nil {
			ti.throughputs.store(path, newThroughputUsagePerNamespace(ti.root.gen, change.quota, ti.quotaNodes[path].usage))
		}
	}
	atomic.StoreInt64(&ti.throughputs.depth, int64(depth))
	ti.throughputs.publishRoutes(depth)
}

func TestQuotaMetricOnlyDifferential(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, policy := range []QuotaDroppingPolicy{QDPNone, QDPNew} {
			t.Run(fmt.Sprintf("cached=%t/policy=%v", cached, policy), func(t *testing.T) {
				rules := []*Quota{
					{Pattern: "/", Throughput: 31, DroppingPolicy: QDPNone},
					{Pattern: "a", Throughput: math.MaxInt64},
					{Pattern: "a.b", Metrics: 1, DroppingPolicy: policy},
					{Pattern: "a.c", Throughput: 5, DroppingPolicy: policy},
					{Pattern: "b", Throughput: -1, Metrics: 1},
				}
				listener, fast := routeListener(t, rules...)
				oldListener, old := routeListener(t, rules...)
				fast.estimateSize = func(string) (int64, int64, int64) { return 1, 1, 1 }
				old.estimateSize = fast.estimateSize
				legacyMetricOnlyAccounting(t, old, rules)
				states := []map[string]any{{}, {}}
				for cycle := range 8 {
					for i, name := range []string{"a.b.one", "a.c.two", "b.one", "a.one", "plain", "a.b.new"} {
						for _, count := range []int{0, 1, 2, 8} {
							ps := &points.Points{Metric: name, Data: make([]points.Point, count)}
							inCache := (cycle+i)%2 == 0
							call := func(l *CarbonserverListener, state map[string]any) bool {
								if !cached {
									return l.ShouldThrottleMetric(ps, inCache)
								}
								s := state[name]
								got := l.ShouldThrottleMetricWithState(ps, inCache, &s)
								state[name] = s
								return got
							}
							if got, want := call(listener, states[0]), call(oldListener, states[1]); got != want {
								t.Fatalf("cycle=%d name=%q count=%d inCache=%t: got %t want %t", cycle, name, count, inCache, got, want)
							}
							for _, path := range []string{"/", "a", "a.c"} {
								x, y := fast.throughputs.load(path), old.throughputs.load(path)
								if x.dpRecorder().total() != y.dpRecorder().total() || x.quotaUsage().Throttled != y.quotaUsage().Throttled {
									t.Fatalf("accounting differs for %q", path)
								}
							}
						}
					}
					fast.refreshUsage(fast.throughputs)
					old.refreshUsage(old.throughputs)
					if !reflect.DeepEqual(fast.qauMetrics, old.qauMetrics) {
						t.Fatal("emitted quota/usage/throttle/read statistics differ")
					}
				}
			})
		}
	}
}

func TestQuotaMetricOnlyThroughputReload(t *testing.T) {
	l, ti := routeListener(t)
	apply := func(limit int64) {
		t.Helper()
		if _, err := ti.applyQuotas(time.Hour,
			&Quota{Pattern: "a", Throughput: math.MaxInt64},
			&Quota{Pattern: "a.b", Metrics: 1, Throughput: limit, DroppingPolicy: QDPNew},
		); err != nil {
			t.Fatal(err)
		}
	}
	ps := points.OnePoint("a.b.one", 1, 1)
	var state any
	apply(0)
	if ti.throughputs.load("a.b") != nil || ti.throughputs.getDepth() != 0 {
		t.Fatal("metric-only rule contributes throughput accounting or depth")
	}
	if l.ShouldThrottleMetricWithState(ps, true, &state) {
		t.Fatal("metric-only rule rejected an existing point")
	}
	apply(1)
	if ti.throughputs.getDepth() != strings.Count("a.b", ".") {
		t.Fatal("new throughput control did not increase lookup depth")
	}
	if l.ShouldThrottleMetricWithState(ps, true, &state) || !l.ShouldThrottleMetricWithState(ps, true, &state) {
		t.Fatal("new throughput control did not start a fresh, enforced window")
	}
	apply(0)
	if ti.throughputs.load("a.b") != nil || ti.throughputs.getDepth() != 0 || l.ShouldThrottleMetricWithState(ps, true, &state) {
		t.Fatal("disabled throughput control retained accounting, depth or enforcement")
	}
	ti.refreshUsage(ti.throughputs)
	for _, metric := range ti.qauMetrics {
		if strings.Contains(metric.Metric, "throughput.a-b") {
			t.Fatal("disabled throughput control emitted throughput statistics")
		}
	}
}

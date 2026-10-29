package carbonserver

import (
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
)

func routeListener(t *testing.T, quotas ...*Quota) (*CarbonserverListener, *trieIndex) {
	t.Helper()
	ti := newTrie(".wsp", 0, nil)
	for _, name := range []string{"a.b.one", "a.c.two", "b.one", "a.one", "plain"} {
		if _, err := ti.insert("/"+strings.ReplaceAll(name, ".", "/")+".wsp", 0, 0, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ti.applyQuotas(time.Hour, quotas...); err != nil {
		t.Fatal(err)
	}
	l := NewCarbonserverListener(nil)
	l.UpdateFileIndex(&fileIndex{trieIdx: ti})
	return l, ti
}

func TestThroughputRoutesMatchUncached(t *testing.T) {
	for _, limit := range []int64{0, 1, 3, math.MaxInt64, 159_000_000} {
		for _, policy := range []QuotaDroppingPolicy{QDPNone, QDPNew} {
			quotas := []*Quota{
				{Pattern: "/", Throughput: 11, DroppingPolicy: QDPNone},
				{Pattern: "a", Throughput: limit, DroppingPolicy: policy},
				{Pattern: "a.b", Throughput: 3, DroppingPolicy: QDPNew},
			}
			l, fast := routeListener(t, quotas...)
			_, old := routeListener(t, quotas...)
			states := map[string]any{}
			for cycle := range 8 {
				if cycle == 4 { // Reset counters while retaining every cached route.
					for _, path := range []string{"/", "a", "a.b"} {
						fast.throughputs.load(path).offset()
						old.throughputs.load(path).offset()
					}
				}
				for _, name := range []string{"a.b.one", "a.c.two", "b.one", "a.one", "plain", "a.b", "a", ""} {
					for _, count := range []int{0, 1, 2, 32} {
						ps := &points.Points{Metric: name, Data: make([]points.Point, count)}
						state := states[name]
						got := l.ShouldThrottleMetricWithState(ps, true, &state)
						states[name] = state
						if want := old.throughputThrottle(ps); got != want {
							t.Fatalf("limit=%d policy=%v cycle=%d %q count=%d: %v, want %v", limit, policy, cycle, name, count, got, want)
						}
						for _, path := range []string{"/", "a", "a.b"} {
							a, b := fast.throughputs.load(path), old.throughputs.load(path)
							if a.dpRecorder().total() != b.dpRecorder().total() || a.quotaUsage().Throttled != b.quotaUsage().Throttled {
								t.Fatalf("%q accounting differs after %q", path, name)
							}
						}
					}
				}
			}
		}
	}
}

func TestThroughputRoutesInvalidateOnReloadAndIndexSwap(t *testing.T) {
	l, ti := routeListener(t, &Quota{Pattern: "a", Throughput: math.MaxInt64})
	ps := points.OnePoint("a.b.one", 1, 1)
	var state any
	for range 3 {
		if l.ShouldThrottleMetricWithState(ps, true, &state) {
			t.Fatal("unlimited route rejected a point")
		}
	}
	before := state.(*throughputRoute)
	if _, err := ti.applyQuotas(time.Hour, &Quota{Pattern: "a", Throughput: 3}); err != nil {
		t.Fatal(err)
	}
	if !l.ShouldThrottleMetricWithState(ps, true, &state) || state == before {
		t.Fatal("reload did not invalidate the route and preserve usage")
	}
	if state.(*throughputRoute).owner.generation <= before.owner.generation {
		t.Fatal("generation did not advance")
	}
	if _, err := ti.applyQuotas(time.Hour, &Quota{Pattern: "["}); err == nil {
		t.Fatal("invalid quota accepted")
	}
	same := state
	if !l.ShouldThrottleMetricWithState(ps, true, &state) || state != same {
		t.Fatal("failed reload changed the route")
	}
	if _, err := ti.applyQuotas(time.Hour); err != nil {
		t.Fatal(err)
	}
	if l.ShouldThrottleMetricWithState(ps, true, &state) {
		t.Fatal("removed rule is still enforced")
	}
	_, fresh := routeListener(t, &Quota{Pattern: "/", Throughput: 1})
	l.UpdateFileIndex(&fileIndex{trieIdx: fresh})
	if l.ShouldThrottleMetricWithState(ps, true, &state) || !l.ShouldThrottleMetricWithState(ps, true, &state) {
		t.Fatal("index swap reused a route from another manager")
	}
}

func TestThroughputRoutesDelayedAndDisabledReset(t *testing.T) {
	for _, reset := range []time.Duration{0, time.Hour} {
		l, ti := routeListener(t, &Quota{Pattern: "/", Throughput: 10})
		ti.setResetFrequency(reset)
		r := ti.throughputs.load("/").dpRecorder()
		r.resetAt = time.Now().Add(-2 * time.Hour)
		var state any
		ps := points.OnePoint("a.b.one", 1, 1)
		for range 21 {
			if l.ShouldThrottleMetricWithState(ps, true, &state) {
				t.Fatal("delayed or disabled reset enforced the nominal window")
			}
		}
		if got := l.ShouldThrottleMetricWithState(ps, true, &state); got != (reset != 0) {
			t.Fatal("delayed/disabled reset differs from the original boundary")
		}
	}
}

func TestThroughputRoutesConcurrentReload(t *testing.T) {
	l, ti := routeListener(t, &Quota{Pattern: "/", Throughput: 100000})
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 4 {
		wg.Go(func() {
			<-start
			var state any
			ps := points.OnePoint("a.b.one", 1, 1)
			for range 1000 {
				l.ShouldThrottleMetricWithState(ps, true, &state)
			}
		})
	}
	close(start)
	for i := range 20 {
		var quotas []*Quota
		if i%2 == 0 {
			quotas = []*Quota{{Pattern: "/", Throughput: 100000}, {Pattern: "a", Throughput: 10000}}
		}
		if _, err := ti.applyQuotas(time.Hour, quotas...); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}

func TestThroughputRoutesBatchesKeepPointBoundaries(t *testing.T) {
	for _, policy := range []QuotaDroppingPolicy{QDPNone, QDPNew} {
		for _, reset := range []bool{false, true} {
			quotas := []*Quota{{Pattern: "/", Throughput: 11, DroppingPolicy: QDPNone}, {Pattern: "a", Throughput: 3, DroppingPolicy: policy}}
			oldListener, oldTrie := routeListener(t, quotas...)
			listener, ti := routeListener(t, quotas...)
			old, batched := cache.New(), cache.New()
			old.SetMaxSize(0)
			batched.SetMaxSize(0)
			oldCalls, calls := 0, 0
			old.SetThrottle(func(ps *points.Points, inCache bool) bool {
				oldCalls++
				if reset && oldCalls%4 == 0 {
					oldTrie.throughputs.load("a").offset()
				}
				return oldListener.ShouldThrottleMetric(ps, inCache)
			})
			batched.SetThrottleWithState(func(ps *points.Points, inCache bool, state *any) bool {
				calls++
				if reset && calls%4 == 0 {
					ti.throughputs.load("a").offset()
				}
				return listener.ShouldThrottleMetricWithState(ps, inCache, state)
			})
			data := make([]points.Point, 32)
			for i := range data {
				data[i] = points.Point{Value: float64(i), Timestamp: int64(i)}
				old.AddPoint([]byte("a.b.one"), float64(i), int64(i))
			}
			batched.AddPoints([]byte("a.b.one"), data)
			if !reflect.DeepEqual(old.Get("a.b.one"), batched.Get("a.b.one")) || oldCalls != calls {
				t.Fatalf("policy=%v reset=%v: batch changed quota boundary", policy, reset)
			}
			for _, path := range []string{"/", "a"} {
				a, b := oldTrie.throughputs.load(path), ti.throughputs.load(path)
				if a.dpRecorder().total() != b.dpRecorder().total() || a.quotaUsage().Throttled != b.quotaUsage().Throttled {
					t.Fatal("batch changed usage")
				}
			}
		}
	}
}

package carbonserver

import (
	"math"
	"math/rand"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2life"
	"github.com/go-graphite/go-carbon/points"
)

func TestV2LifeQuotaDifferential(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		estimate := func(string) (int64, int64, int64) { return 1000, 0, 60 }
		scenarios := [][]*Quota{
			{{Pattern: "/", Metrics: 3}}, {{Pattern: "service", Metrics: 2}}, {{Pattern: "/", Namespaces: 2}},
			{{Pattern: "service", DataPoints: 120}}, {{Pattern: "service", LogicalSize: 2000}},
			{{Pattern: "service", Metrics: 1, DroppingPolicy: QDPNone}}, {{Pattern: "service", Throughput: 20}},
			{{Pattern: "*", Throughput: 100}, {Pattern: "service.*", Throughput: 10}},
			{{Pattern: "/", Metrics: 3, Throughput: 100}, {Pattern: "service", DataPoints: 60, DroppingPolicy: QDPNone}},
			{{Pattern: "service", Throughput: math.MaxInt64}}, nil,
		}
		decisions := 0
		for si, rules := range scenarios {
			makeIndex := func() *trieIndex {
				ti := newTrie(".wsp", 0, estimate)
				for _, name := range []string{"service.a.cpu", "service.b.cpu", "other.a.cpu"} {
					if _, err := ti.insert(metricStoreVirtualPath(name), 1000, 0, 60, 0); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := ti.applyQuotas(time.Minute, rules...); err != nil {
					t.Fatal(err)
				}
				ti.refreshUsage(ti.throughputs)
				return ti
			}
			oracle, host := makeIndex(), makeIndex()
			listener := NewCarbonserverListener(nil)
			listener.UpdateFileIndex(&fileIndex{trieIdx: host})
			gate, err := v2life.NewQuotas(listener, func() bool { return listener.CurrentFileIndex() != nil })
			if err != nil {
				t.Fatal(err)
			}
			rng := rand.New(rand.NewSource(100 + int64(si)))
			for event := range 1000 {
				name := []string{"service.a.cpu", "service.b.cpu", "other.a.cpu", "service.a.new", "service.c.new", "new.namespace.metric"}[rng.Intn(6)]
				ps := &points.Points{Metric: name, Data: make([]points.Point, rng.Intn(8))}
				cached := rng.Intn(2) == 0
				want, got := oracle.throttle(ps, cached), gate.Throttle(ps, cached)
				if want != got {
					t.Fatalf("scenario %d event %d: %s cached=%v got=%v want=%v", si, event, name, cached, got, want)
				}
				decisions++
				for _, ti := range []*trieIndex{oracle, host} {
					if event == 300 {
						ti.refreshUsage(ti.throughputs)
					}
					if event == 600 {
						if _, err := ti.applyQuotas(time.Minute, &Quota{Pattern: "service", Metrics: 1, Throughput: 11}); err != nil {
							t.Fatal(err)
						}
					}
					if event == 800 {
						if _, err := ti.applyQuotas(time.Minute); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
		}
		t.Logf("%d quota decisions identical", decisions)
	})
}

func TestV2LifeQuotaExactLimitAndResume(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ti := newTrie(".wsp", 0, func(string) (int64, int64, int64) { return 1000, 0, 60 })
		insert := func(name string) {
			if _, err := ti.insert(metricStoreVirtualPath(name), 1000, 0, 60, 0); err != nil {
				t.Fatal(err)
			}
		}
		insert("service.a.cpu")
		_, err := ti.applyQuotas(time.Minute, &Quota{Pattern: "service", Metrics: 2, DataPoints: 120, LogicalSize: 2000, Throughput: 2})
		if err != nil {
			t.Fatal(err)
		}
		ti.refreshUsage(ti.throughputs)
		listener := NewCarbonserverListener(nil)
		listener.UpdateFileIndex(&fileIndex{trieIdx: ti})
		gate, err := v2life.NewQuotas(listener, func() bool { return listener.CurrentFileIndex() != nil })
		if err != nil {
			t.Fatal(err)
		}
		if gate.Throttle(points.OnePoint("service.b.cpu", 1, 1), false) {
			t.Fatal("exact limit rejected")
		}
		insert("service.b.cpu")
		ti.refreshUsage(ti.throughputs)
		if !gate.Throttle(points.OnePoint("service.c.cpu", 1, 1), false) {
			t.Fatal("above limit admitted")
		}
		if gate.Throttle(points.OnePoint("service.a.cpu", 1, 1), false) {
			t.Fatal("existing name rejected by storage limit")
		}
		if !gate.Throttle(points.OnePoint("service.a.cpu", 1, 1), true) {
			t.Fatal("cache bypassed throughput")
		}
		// Drop the expired catalog name, preserving the host's throughput manager.
		next := newTrie(".wsp", 0, ti.estimateSize)
		next.throughputs = ti.throughputs
		if _, err := next.insert(metricStoreVirtualPath("service.a.cpu"), 1000, 0, 60, 0); err != nil {
			t.Fatal(err)
		}
		if _, err := next.applyQuotas(time.Minute, &Quota{Pattern: "service", Metrics: 2, Throughput: 2}); err != nil {
			t.Fatal(err)
		}
		listener.UpdateFileIndex(&fileIndex{trieIdx: next})
		if !gate.Throttle(points.OnePoint("service.b.cpu", 1, 1), false) {
			t.Fatal("catalog expiry reset throughput")
		}
		next.refreshUsage(next.throughputs)
		if gate.Throttle(points.OnePoint("service.b.cpu", 1, 1), false) {
			t.Fatal("resumed expired name could not use released storage quota")
		}
	})
}

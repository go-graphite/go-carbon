package persister

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
)

// This probe uses the real cache, persister locks and Pebble continuation codec.
// A blocked first update places restored history in flight when input opens.
type restoreGateProbeStore struct {
	*store.Store
	once             sync.Once
	entered, release chan struct{}
	fail             bool
}

func (s *restoreGateProbeStore) UpdateMany(ctx context.Context, name string, values []points.Point) error {
	first := false
	s.once.Do(func() {
		first = true
		close(s.entered)
		<-s.release
	})
	if first && s.fail {
		return errors.New("research: transient write failure")
	}
	return s.Store.UpdateMany(ctx, name, values)
}

func TestResearchRestoreGateLoadComplete(t *testing.T) {
	for _, direct := range []bool{false, true} {
		for _, mode := range []string{"queued", "inflight", "retry"} {
			t.Run(fmt.Sprintf("direct=%v/%s", direct, mode), func(t *testing.T) {
				ctx := context.Background()
				now := time.Unix(1790000040, 0)
				open := func() *store.Store {
					db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, TimeBuckets: true,
						Continuation: &whisperio.ContinuationCodec{DirectLateWrites: direct},
						Now:          func() time.Time { return now }, SyncInterval: time.Hour,
						CacheSize: 1 << 20, MemTableSize: 1 << 20})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := db.Close(); err != nil {
							t.Error(err)
						}
					})
					for _, name := range []string{"history", "unrelated"} {
						m, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 3600}, {Step: 60, Count: 1440}}, AggregationMethod: store.Average, XFilesFactor: .5})
						if err != nil || !m.Continuation {
							t.Fatalf("must exercise continuation: %+v %v", m, err)
						}
					}
					return db
				}
				actual, reference := open(), open()
				core := cache.New()
				core.SetMaxSize(0)
				core.SetRecoveryDrain(true)
				p := NewWhisper("", nil, NewWhisperAggregation(), nil, core.PopNotConfirmed, core.Confirm, core.Pop)
				p.SetRequeue(core.Requeue)
				hook := &restoreGateProbeStore{Store: actual, entered: make(chan struct{}), release: make(chan struct{}), fail: mode == "retry"}
				p.SetMetricStore(hook)
				old := &points.Points{Metric: "history"}
				for i := int64(0); i < 600; i++ {
					old.Data = append(old.Data, points.Point{Timestamp: now.Unix() - 1200 + i, Value: float64(i) + .125})
				}
				live := &points.Points{Metric: "history", Data: []points.Point{{Timestamp: now.Unix() - 1, Value: 999.125}, {Timestamp: now.Unix() - 10, Value: 998.125}, {Timestamp: now.Unix() - 601, Value: 777.125}}}
				core.AddRestoredBeforeInput(old)
				core.AddRestoredBeforeInput(points.OnePoint("unrelated", 42.125, now.Unix()-700))
				core.TrackRestorePrefix()
				if err := reference.UpdateMany(ctx, "history", old.Data); err != nil {
					t.Fatal(err)
				}
				if err := reference.UpdateMany(ctx, "history", live.Data); err != nil {
					t.Fatal(err)
				}
				if mode == "queued" {
					close(hook.release)
					core.Add(live)
					p.store("history")
				} else {
					done := make(chan struct{})
					go func() { p.store("history"); close(done) }()
					select {
					case <-hook.entered:
					case <-time.After(5 * time.Second):
						t.Fatal("no in-flight update")
					}
					core.Add(live)
					if core.IsEmpty() {
						t.Fatal("unpersisted history incorrectly empty")
					}
					second := make(chan struct{})
					go func() { p.store("history"); close(second) }()
					close(hook.release)
					for _, ch := range []chan struct{}{done, second} {
						select {
						case <-ch:
						case <-time.After(5 * time.Second):
							t.Fatal("persister did not finish")
						}
					}
				}
				if core.IsEmpty() {
					t.Fatal("global cache should retain unrelated restored history")
				}
				if core.RestorePending("history") || !core.RestorePending("unrelated") {
					t.Fatal("per-metric read readiness did not follow confirmations")
				}
				got, err := actual.Snapshot(ctx, "history")
				if err != nil {
					t.Fatal(err)
				}
				want, err := reference.Snapshot(ctx, "history")
				if err != nil {
					t.Fatal(err)
				}
				// Compare every returned timestamp and every IEEE754 bit, across all archives.
				if len(got.Archives) != len(want.Archives) {
					t.Fatal("archive count")
				}
				for i, a := range got.Archives {
					b := want.Archives[i]
					if !reflect.DeepEqual(a.Retention, b.Retention) || len(a.Points) != len(b.Points) {
						t.Fatalf("archive %d shape: got %d want %d", i, len(a.Points), len(b.Points))
					}
					for j, v := range a.Points {
						w := b.Points[j]
						if v.Timestamp != w.Timestamp || math.Float64bits(v.Value) != math.Float64bits(w.Value) {
							t.Fatalf("archive %d point %d: got %+v want %+v", i, j, v, w)
						}
					}
				}
				p.store("unrelated")
				if !core.IsEmpty() || core.RestoreOutstanding() != 0 {
					t.Fatal("points remain after final drain")
				}
				t.Log("all archive timestamps/value bits match fully-drained reference with unrelated history still queued at input time")
			})
		}
	}
}

// Late-write support cannot infer which equal-timestamp value is newer input.
// Opening before a metric's saved history is claimed needs a source-order fence.
func TestResearchRestoreBeforeLoadOverwritesLive(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1790000040, 0)
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, TimeBuckets: true,
		Continuation: &whisperio.ContinuationCodec{DirectLateWrites: true},
		Now:          func() time.Time { return now }, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Create(ctx, store.MetricConfig{Name: "metric", Retentions: []store.Retention{{Step: 1, Count: 3600}}, AggregationMethod: store.Last})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []float64{2.125, 1.125} {
		if err := db.UpdateMany(ctx, "metric", []store.Point{{Timestamp: now.Unix() - 10, Value: v}}); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := db.Snapshot(ctx, "metric")
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Archives[0].Points) != 1 || snapshot.Archives[0].Points[0].Value != 1.125 {
		t.Fatalf("unexpected overwritten snapshot: %+v", snapshot.Archives)
	}
	t.Log("new live value 2.125 was replaced by subsequently loaded old value 1.125: per-metric source ordering still required")
}

package downsize

import (
	"context"
	"fmt"
	"math"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Both pin callbacks must start before either completes. This exercises the
// shared-controller lifetime, rather than merely running an HTTP mock in parallel.
func TestSyncWindowConcurrentDonorPins(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	backend := newSyncLiveBackend(syncLiveOptions{})
	finish := backend.beginWindowCopy()
	defer finish()
	var donors []*httptest.Server
	for range 2 {
		source := nativeControllerStore(t)
		addPoints(t, source, "metric", 1)
		source.SetNativePendingSnapshot(func() (map[string]struct{}, error) {
			arrived <- struct{}{}
			select {
			case <-release:
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
		donor := httptest.NewServer(nativeControllerService(t, source))
		defer donor.Close()
		donors = append(donors, donor)
	}
	done := make(chan error, 2)
	for _, donor := range donors {
		go func() {
			_, err := backend.WindowPresence(ctx, donor.URL, []string{"metric"}, syncRange{From: testMetricNow - 20, Until: testMetricNow})
			done <- err
		}()
	}
	for range 2 {
		select {
		case <-arrived:
		case <-ctx.Done():
			t.Fatal("donor pins serialized")
		}
	}
	once.Do(func() { close(release) })
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

// Gaps, exact float bits, independently populated coarse archives, pending
// names, target overlap and reused receipts all go through the deployed exporter.
func TestSyncWindowEnvelopeSelectionDifferential(t *testing.T) {
	ctx := context.Background()
	source := nativeControllerStore(t)
	targets := []*store.Store{nativeControllerStore(t), nativeControllerStore(t)}
	base := testMetricNow/60*60 - 180
	windows := syncRanges{{From: base, Until: base + 10}, {From: base + 120, Until: base + 130}}
	names := make([]string, 64)
	for i := range names {
		n := fmt.Sprintf("envelope.%02d", i)
		names[i] = n
		ret := []store.Retention{{Step: 10, Count: 128}, {Step: 60, Count: 128}}
		ts := base + int64(i%4)*60
		value := float64(i) + 0.125
		if i%7 == 0 {
			value = math.Copysign(0, -1)
		}
		if i%11 == 0 {
			value = math.SmallestNonzeroFloat64
		}
		snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: n, Retentions: ret, AggregationMethod: store.Sum}}, Archives: []store.Archive{{Retention: ret[0], Points: []store.Point{{Timestamp: ts, Value: value}}}, {Retention: ret[1], Points: []store.Point{{Timestamp: ts, Value: value}}}}}
		if _, err := source.CreateFromSnapshot(ctx, snap); err != nil {
			t.Fatal(err)
		}
		if i%3 == 0 {
			snap.Archives[0].Points[0].Value = 42
			snap.Archives[1].Points[0].Value = 43
			for _, dst := range targets {
				if _, err := dst.CreateFromSnapshot(ctx, snap); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	source.SetNativePendingSnapshot(func() (map[string]struct{}, error) { return map[string]struct{}{names[0]: {}}, nil })
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	b := newSyncLiveBackend(syncLiveOptions{})
	var wanted []string
	for _, n := range names {
		for _, w := range windows {
			d, err := b.WindowPresence(ctx, donor.URL, []string{n}, w)
			if err != nil {
				t.Fatal(err)
			}
			positive := false
			for _, a := range d[n] {
				if a.Count > 0 || a.Hash == "window-incomplete" {
					positive = true
				}
			}
			if positive {
				wanted = append(wanted, n)
				break
			}
		}
	}
	got, err := b.WindowPresenceRanges(ctx, donor.URL, names, windows)
	if err != nil {
		t.Fatal(err)
	}
	var selected []string
	for _, n := range names {
		positive := false
		for _, a := range got[n] {
			if a.Count > 0 || a.Hash == "window-incomplete" {
				positive = true
			}
		}
		if positive {
			selected = append(selected, n)
		}
	}
	if !reflect.DeepEqual(wanted, selected) || got[names[0]][0].Hash != syncWindowUnresolved {
		t.Fatal("selection differs", wanted, selected, got[names[0]])
	}
	for i, dst := range targets {
		server := httptest.NewServer(nativeControllerService(t, dst))
		path := filepath.Join(t.TempDir(), "copy.jsonl")
		for range 2 {
			res, err := b.CopyWindow(ctx, donor.URL, server.URL, selected, path, windows)
			if err != nil || res.Copied+len(res.Failed) != len(selected) {
				t.Fatal(res, err)
			}
		}
		server.Close()
		if i == 1 {
			for _, n := range selected {
				a, ea := targets[0].Snapshot(ctx, n)
				q, eq := dst.Snapshot(ctx, n)
				if ea != nil || eq != nil || !reflect.DeepEqual(a.Metadata.MetricConfig, q.Metadata.MetricConfig) {
					t.Fatal(n, ea, eq)
				}
				for ai, archive := range a.Archives {
					other := q.Archives[ai]
					if len(archive.Points) != len(other.Points) {
						t.Fatal("point count", n)
					}
					for pi, p := range archive.Points {
						v := other.Points[pi]
						if p.Timestamp != v.Timestamp || math.Float64bits(p.Value) != math.Float64bits(v.Value) {
							t.Fatal("point bits", n)
						}
					}
				}
			}
		}
	}
}

type concurrentWindowSelection struct {
	*fakeSync
	arrived chan string
	release chan struct{}
	calls   atomic.Int64
}

func (b *concurrentWindowSelection) WindowPresenceRanges(ctx context.Context, source string, names []string, windows syncRanges) (map[string][]syncDigest, error) {
	b.calls.Add(1)
	b.arrived <- source
	select {
	case <-b.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return b.Digest(ctx, source, names, windows[0].From, windows[0].Until)
}
func TestSyncWindowDriverConcurrentSelectionOrderedCopy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	b := &concurrentWindowSelection{fakeSync: newFakeSync(), arrived: make(chan string, 4), release: make(chan struct{})}
	w := syncRange{From: syncTestNow - 600, Until: syncTestNow - 120}
	for i, source := range []string{syncD1, syncD2} {
		b.store(source).archives["metric"] = []syncArchive{{Step: 60, Points: 10000}}
		b.store(source).points["metric"] = map[int64]map[int64]float64{60: {w.From: float64(i + 1)}}
	}
	o := syncOptions{to: syncT, from: []string{syncD1, syncD2}, state: t.TempDir(), batch: 2, streams: 2, maxRounds: 1, windows: syncRanges{w}, includeShort: true, now: func() time.Time { return time.Unix(syncTestNow, 0) }}
	done := make(chan error, 1)
	go func() { done <- runSync(ctx, o, b) }()
	for range 2 {
		select {
		case <-b.arrived:
		case <-ctx.Done():
			close(b.release)
			t.Fatal("selection driver serialized")
		}
	}
	close(b.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if b.store(syncT).points["metric"][60][w.From] != 1 {
		t.Fatal("source order changed")
	}
	calls := b.calls.Load()
	if err := runSync(ctx, o, b); err != nil {
		t.Fatal(err)
	}
	if b.calls.Load() != calls {
		t.Fatal("resume repeated completed selections")
	}
}

package downsize

import (
	"context"
	"errors"
	"math"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestSyncWindowMissingSelectionAndResume(t *testing.T) {
	f := newFakeSync()
	w := syncRange{Name: "gap", From: syncTestNow - 600, Until: syncTestNow - 120}
	schema := []syncArchive{{Step: 60, Points: 10000}}
	put := func(host, name string, points map[int64]float64) {
		s := f.store(host)
		s.archives[name] = schema
		s.points[name] = map[int64]map[int64]float64{60: points}
	}
	put(syncD1, "new", map[int64]float64{w.From: math.Copysign(0, -1), w.Until: 17})
	put(syncD1, "outside", map[int64]float64{w.From - 60: 10, w.Until: 20})
	put(syncD1, "overlap", map[int64]float64{w.From: 1, w.From + 60: 2, w.Until: 3})
	put(syncD1, "later", map[int64]float64{w.From - 60: 1})
	put(syncD2, "later", map[int64]float64{w.From: 9})
	put(syncD2, "new", map[int64]float64{w.From: 99, w.From + 60: math.SmallestNonzeroFloat64})
	put(syncT, "overlap", map[int64]float64{w.From: 42, w.From - 60: 7})
	o := syncOptions{to: syncT, from: []string{syncD1, syncD2}, state: t.TempDir(), batch: 2, streams: 2, maxRounds: 3, windows: syncRanges{w}, sampleEvery: 1, now: func() time.Time { return time.Unix(syncTestNow, 0) }}
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
	dst := f.store(syncT)
	if dst.points["later"][60][w.From] != 9 {
		t.Fatal("outside-only first source hid later source")
	}
	if _, ok := dst.archives["outside"]; ok {
		t.Fatal("created outside-only metric")
	}
	if math.Float64bits(dst.points["new"][60][w.From]) != math.Float64bits(math.Copysign(0, -1)) {
		t.Fatal("first source bits lost")
	}
	if dst.points["new"][60][w.From+60] != math.SmallestNonzeroFloat64 {
		t.Fatal("later source not filled")
	}
	if dst.points["overlap"][60][w.From] != 42 {
		t.Fatal("target overlap overwritten")
	}
	if dst.points["overlap"][60][w.From+60] != 2 {
		t.Fatal("window hole not filled")
	}
	for _, n := range []string{"new", "overlap"} {
		if _, ok := dst.points[n][60][w.Until]; ok {
			t.Fatal("copied upper boundary", n)
		}
	}
	before := len(f.copies)
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
	if len(f.copies) != before {
		t.Fatal("resume repeated copies")
	}
	o.batch = 3
	if err := runSync(context.Background(), o, f); err == nil {
		t.Fatal("changed batch reused window selections")
	}
	o.batch = 2
	o.fullHistory = true
	if err := runSync(context.Background(), o, f); err == nil {
		t.Fatal("full-history reused window journal")
	}
}

func TestSyncWindowNativeDifferential(t *testing.T) {
	ctx := context.Background()
	source, target, oracle := nativeControllerStore(t), nativeControllerStore(t), nativeControllerStore(t)
	for _, n := range []string{"new", "overlap"} {
		addPoints(t, source, n, math.Copysign(0, -1))
	}
	for _, s := range []*store.Store{target, oracle} {
		addPoints(t, s, "overlap", 42)
	}
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	survivor := httptest.NewServer(nativeControllerService(t, target))
	defer survivor.Close()
	records, err := source.ExportTransfer(ctx, []string{"new", "overlap"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = oracle.ImportTransfer(ctx, records); err != nil {
		t.Fatal(err)
	}
	backend := newSyncLiveBackend(syncLiveOptions{})
	w := syncRange{Name: "gap", From: testMetricNow - 10, Until: testMetricNow}
	presence, err := backend.WindowPresence(ctx, donor.URL, []string{"new", "overlap"}, w)
	if err != nil || presence["new"][0].Count == 0 {
		t.Fatal(presence, err)
	}
	path := filepath.Join(t.TempDir(), "copy.jsonl")
	for range 2 {
		res, err := backend.CopyWindow(ctx, donor.URL, survivor.URL, []string{"new", "overlap"}, path, syncRanges{w})
		if err != nil || res.Copied != 2 {
			t.Fatal(res, err)
		}
	}
	for _, n := range []string{"new", "overlap"} {
		want, err := oracle.Snapshot(ctx, n)
		if err != nil {
			t.Fatal(err)
		}
		got, err := target.Snapshot(ctx, n)
		if err != nil {
			t.Fatal(err)
		}
		for ai, a := range want.Archives {
			for _, p := range a.Points {
				if p.Timestamp < w.From || p.Timestamp >= w.Until {
					continue
				}
				found := false
				for _, q := range got.Archives[ai].Points {
					if p.Timestamp == q.Timestamp {
						found = true
						if math.Float64bits(p.Value) != math.Float64bits(q.Value) {
							t.Fatal("value bits differ", n, p, q)
						}
					}
				}
				if !found {
					t.Fatal("missing oracle point", n, p)
				}
			}
		}
		if n == "new" {
			for _, a := range got.Archives {
				for _, p := range a.Points {
					if p.Timestamp < w.From || p.Timestamp >= w.Until {
						t.Fatal("copied outside window", p)
					}
				}
			}
		}
	}
}

func TestSyncWindowRequiresSourceDigest(t *testing.T) {
	f, o, _ := threeSources(t)
	o.windows = syncRanges{{Name: "gap", From: syncTestNow - 86400, Until: syncTestNow - 86000}}
	f.noDigest[syncD1] = true
	if err := runSync(context.Background(), o, f); !errors.Is(err, errSyncNoDigest) {
		t.Fatal(err)
	}
	if len(f.copies) != 0 {
		t.Fatal("copied full history after missing digest")
	}
}

func TestSyncWindowIncompleteCoarseBoundaryIsResidual(t *testing.T) {
	ctx := context.Background()
	source, target := nativeControllerStore(t), nativeControllerStore(t)
	base := testMetricNow/60*60 - 120
	snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: "boundary", AggregationMethod: store.Sum, Retentions: []store.Retention{{Step: 10, Count: 128}, {Step: 60, Count: 128}}}}, Archives: []store.Archive{
		{Retention: store.Retention{Step: 10, Count: 128}, Points: []store.Point{{Timestamp: base, Value: 1}, {Timestamp: base + 10, Value: 2}}},
		{Retention: store.Retention{Step: 60, Count: 128}, Points: []store.Point{{Timestamp: base, Value: 3}}},
	}}
	if _, err := source.CreateFromSnapshot(ctx, snap); err != nil {
		t.Fatal(err)
	}
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	survivor := httptest.NewServer(nativeControllerService(t, target))
	defer survivor.Close()
	backend := newSyncLiveBackend(syncLiveOptions{})
	res, err := backend.CopyWindow(ctx, donor.URL, survivor.URL, []string{"boundary"}, filepath.Join(t.TempDir(), "copy.jsonl"), syncRanges{{Name: "gap", From: base, Until: base + 20}})
	if err != nil || res.Copied != 0 || len(res.Failed) != 1 {
		t.Fatal("incomplete coarse bucket reported as success", res, err)
	}
}

func TestSyncWindowPresenceIncludesCoarseOnlyPoints(t *testing.T) {
	ctx := context.Background()
	source := nativeControllerStore(t)
	base := testMetricNow/60*60 - 120
	snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: "coarse", AggregationMethod: store.Sum, Retentions: []store.Retention{{Step: 10, Count: 128}, {Step: 60, Count: 128}}}}, Archives: []store.Archive{
		{Retention: store.Retention{Step: 10, Count: 128}},
		{Retention: store.Retention{Step: 60, Count: 128}, Points: []store.Point{{Timestamp: base, Value: 42}}},
	}}
	if _, err := source.CreateFromSnapshot(ctx, snap); err != nil {
		t.Fatal(err)
	}
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	backend := newSyncLiveBackend(syncLiveOptions{})
	got, err := backend.WindowPresence(ctx, donor.URL, []string{"coarse"}, syncRange{From: base, Until: base + 60})
	if err != nil || len(got["coarse"]) != 2 || got["coarse"][0].Count != 0 || got["coarse"][1].Count != 1 {
		t.Fatal(got, err)
	}
	outside, err := backend.WindowPresence(ctx, donor.URL, []string{"coarse"}, syncRange{From: base - 120, Until: base - 60})
	if err != nil || outside["coarse"][1].Count != 0 {
		t.Fatal(outside, err)
	}
}

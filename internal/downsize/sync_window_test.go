package downsize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
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
	presence, err := backend.WindowPresence(ctx, donor.URL, []string{"overlap", "new"}, w)
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

func TestSyncWindowPresenceRetainsPendingResidual(t *testing.T) {
	source := nativeControllerStore(t)
	addPoints(t, source, "pending", 42)
	addPoints(t, source, "healthy", 7)
	source.SetNativePendingSnapshot(func() (map[string]struct{}, error) { return map[string]struct{}{"pending": {}}, nil })
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	backend := newSyncLiveBackend(syncLiveOptions{})
	got, err := backend.WindowPresence(context.Background(), donor.URL, []string{"pending", "healthy"}, syncRange{From: testMetricNow - 10, Until: testMetricNow})
	if err != nil || len(got["pending"]) != 1 || got["pending"][0].Hash != syncWindowUnresolved || len(got["healthy"]) == 0 || got["healthy"][0].Count == 0 {
		t.Fatal("pending record aborted batch or became empty history", got, err)
	}
}

type unresolvedWindowBackend struct{ *fakeSync }

func (b unresolvedWindowBackend) WindowPresence(ctx context.Context, source string, names []string, w syncRange) (map[string][]syncDigest, error) {
	got, err := b.Digest(ctx, source, names, w.From, w.Until)
	for _, n := range names {
		if n == "pending" {
			got[n] = []syncDigest{{Hash: syncWindowUnresolved}}
		}
	}
	return got, err
}

func TestSyncWindowSelectionResidualContinuesAndResumes(t *testing.T) {
	f := newFakeSync()
	w := syncRange{Name: "gap", From: syncTestNow - 600, Until: syncTestNow - 120}
	for _, n := range []string{"healthy", "pending"} {
		f.store(syncD1).archives[n] = []syncArchive{{Step: 60, Points: 10000}}
		f.store(syncD1).points[n] = map[int64]map[int64]float64{60: {w.From: 42}}
	}
	o := syncOptions{to: syncT, from: []string{syncD1}, state: t.TempDir(), batch: 2, streams: 1, maxRounds: 1, windows: syncRanges{w}, includeShort: true, now: func() time.Time { return time.Unix(syncTestNow, 0) }}
	for range 2 {
		if err := runSync(context.Background(), o, unresolvedWindowBackend{f}); !errors.Is(err, ErrSyncResidual) {
			t.Fatal("unresolved coverage reported success", err)
		}
		if f.store(syncT).points["healthy"][60][w.From] != 42 {
			t.Fatal("healthy peer was not copied")
		}
		data, err := os.ReadFile(filepath.Join(o.state, "residual.json"))
		if err != nil || !bytes.Contains(data, []byte("source window selection unresolved")) {
			t.Fatal("selection residual lost on resume", string(data), err)
		}
	}
	if _, ok := f.store(syncT).archives["pending"]; ok {
		t.Fatal("unresolved name was copied")
	}
}

// Compare fresh-pin selection/copy with the scoped-pin path on the same
// 2,048-name fixture. Include target-wins overlap and an unresolved source.
func TestSyncWindowSharedPinDifferential2048(t *testing.T) {
	ctx := context.Background()
	source := nativeControllerStore(t)
	targets := []*store.Store{nativeControllerStore(t), nativeControllerStore(t)}
	names := make([]string, 2048)
	for i := range names {
		names[i] = fmt.Sprintf("metric.%04d", i)
		value := float64(i) + 0.125
		if i%17 == 0 {
			value = math.Copysign(0, -1)
		}
		if i%19 == 0 {
			value = math.SmallestNonzeroFloat64
		}
		addPoints(t, source, names[i], value)
		if i%3 == 0 {
			for _, target := range targets {
				addPoints(t, target, names[i], 42)
			}
		}
	}
	var pins atomic.Int64
	source.SetNativePendingSnapshot(func() (map[string]struct{}, error) {
		pins.Add(1)
		return map[string]struct{}{names[0]: {}}, nil
	})
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	w := syncRange{From: testMetricNow - 10, Until: testMetricNow}
	var reference []map[string][]syncDigest
	for mode, target := range targets {
		survivor := httptest.NewServer(nativeControllerService(t, target))
		backend := newSyncLiveBackend(syncLiveOptions{})
		finish := func() {}
		if mode == 1 {
			finish = backend.beginWindowCopy()
		}
		path := t.TempDir()
		before := pins.Load()
		for start := 0; start < len(names); start += 128 {
			batch := names[start : start+128]
			presence, err := backend.WindowPresence(ctx, donor.URL, batch, w)
			if err != nil {
				t.Fatal(err)
			}
			if mode == 0 {
				reference = append(reference, presence)
			} else if !reflect.DeepEqual(reference[start/128], presence) {
				t.Fatal("selection differs", start)
			}
			var selected []string
			for _, n := range batch {
				if presence[n][0].Hash != syncWindowUnresolved {
					selected = append(selected, n)
				}
			}
			result, err := backend.CopyWindow(ctx, donor.URL, survivor.URL, selected, filepath.Join(path, fmt.Sprintf("batch-%d", start)), syncRanges{w})
			if err != nil || result.Copied != len(selected) {
				t.Fatal(result, err)
			}
		}
		finish()
		survivor.Close()
		wantPins := int64(32)
		if mode == 1 {
			wantPins = 1
		}
		if got := pins.Load() - before; got != wantPins {
			t.Fatalf("mode %d: pins %d, want %d", mode, got, wantPins)
		}
	}
	for _, name := range names {
		a, ea := targets[0].Snapshot(ctx, name)
		b, eb := targets[1].Snapshot(ctx, name)
		if !errors.Is(ea, eb) || !reflect.DeepEqual(a.Metadata.MetricConfig, b.Metadata.MetricConfig) || len(a.Archives) != len(b.Archives) {
			t.Fatal("metadata differs", name, ea, eb)
		}
		for ai, archive := range a.Archives {
			other := b.Archives[ai]
			if archive.Retention != other.Retention || len(archive.Points) != len(other.Points) {
				t.Fatal("archive differs", name)
			}
			for pi, point := range archive.Points {
				q := other.Points[pi]
				if point.Timestamp != q.Timestamp || math.Float64bits(point.Value) != math.Float64bits(q.Value) {
					t.Fatal("point bits differ", name, point, q)
				}
			}
		}
	}
}

func TestSyncWindowSharedPinRevisionAndRotation(t *testing.T) {
	ctx := context.Background()
	source := nativeControllerStore(t)
	addPoints(t, source, "changing", 1)
	var pins atomic.Int64
	source.SetNativePendingSnapshot(func() (map[string]struct{}, error) { pins.Add(1); return nil, nil })
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	backend := newSyncLiveBackend(syncLiveOptions{})
	finish := backend.beginWindowCopy()
	defer finish()
	w := syncRange{From: testMetricNow - 10, Until: testMetricNow}
	check := func(unresolved bool) {
		t.Helper()
		got, err := backend.WindowPresence(ctx, donor.URL, []string{"changing"}, w)
		if err != nil || len(got["changing"]) == 0 || (got["changing"][0].Hash == syncWindowUnresolved) != unresolved {
			t.Fatal(got, err)
		}
	}
	check(false)
	if err := source.UpdateMany(ctx, "changing", []store.Point{{Timestamp: testMetricNow - 5, Value: 2}}); err != nil {
		t.Fatal(err)
	}
	check(true)
	backend.windowPinned = time.Now().Add(-6 * time.Minute)
	check(false)
	if pins.Load() != 2 {
		t.Fatal("pin was not rotated", pins.Load())
	}
}

func TestSyncCopyWindowsUnion(t *testing.T) {
	s := &syncer{o: syncOptions{now: func() time.Time { return time.Unix(1000, 0) }, settle: 100 * time.Second,
		windows: syncRanges{{Name: "nested", From: 200, Until: 300}, {Name: "gap", From: 700, Until: 1100}, {Name: "broad", From: 100, Until: 400}, {Name: "overlap", From: 350, Until: 500}, {Name: "adjacent", From: 500, Until: 600}, {Name: "future", From: 950, Until: 1000}}}}
	original := append(syncRanges(nil), s.o.windows...)
	got := s.copyWindows()
	want := syncRanges{{Name: "broad", From: 100, Until: 600}, {Name: "gap", From: 700, Until: 900}}
	if !reflect.DeepEqual(got, want) {
		t.Fatal(got, want)
	}
	if !reflect.DeepEqual(s.o.windows, original) {
		t.Fatal("verification windows mutated")
	}
}

func TestSyncCopyWindowsUnionNativeDifferential(t *testing.T) {
	ctx := context.Background()
	source, target, oracle := nativeControllerStore(t), nativeControllerStore(t), nativeControllerStore(t)
	for _, n := range []string{"new", "overlap"} {
		addPoints(t, source, n, math.Copysign(0, -1))
	}
	for _, dst := range []*store.Store{target, oracle} {
		addPoints(t, dst, "overlap", 42)
	}
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	actual := httptest.NewServer(nativeControllerService(t, target))
	defer actual.Close()
	reference := httptest.NewServer(nativeControllerService(t, oracle))
	defer reference.Close()
	original := syncRanges{{Name: "broad", From: testMetricNow - 20, Until: testMetricNow}, {Name: "nested", From: testMetricNow - 10, Until: testMetricNow}}
	s := &syncer{o: syncOptions{windows: original, now: func() time.Time { return time.Unix(testMetricNow+100, 0) }}}
	names := []string{"new", "overlap"}
	for mode, windows := range []syncRanges{original, s.copyWindows()} {
		endpoint := reference.URL
		if mode == 1 {
			endpoint = actual.URL
		}
		b := newSyncLiveBackend(syncLiveOptions{})
		path := filepath.Join(t.TempDir(), "copy.jsonl")
		for range 2 { // A second run must reuse the same native receipts.
			result, err := b.CopyWindow(ctx, donor.URL, endpoint, names, path, windows)
			if err != nil || result.Copied != len(names) {
				t.Fatal(result, err)
			}
		}
	}
	for _, name := range names {
		a, ea := target.Snapshot(ctx, name)
		b, eb := oracle.Snapshot(ctx, name)
		if ea != nil || eb != nil || !reflect.DeepEqual(a.Metadata.MetricConfig, b.Metadata.MetricConfig) || len(a.Archives) != len(b.Archives) {
			t.Fatal(name, ea, eb)
		}
		for i, archive := range a.Archives {
			other := b.Archives[i]
			if archive.Retention != other.Retention || len(archive.Points) != len(other.Points) {
				t.Fatal("archive differs", name)
			}
			for j, p := range archive.Points {
				q := other.Points[j]
				if p.Timestamp != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
					t.Fatal("point bits differ", name)
				}
			}
		}
	}
}

func TestSyncWindowTruncatedSelectionSplitsWithoutPartialResults(t *testing.T) {
	source := nativeControllerStore(t)
	names := []string{"a", "bad", "c", "d"}
	for _, name := range names {
		addPoints(t, source, name, math.Copysign(0, -1))
	}
	service := nativeControllerService(t, source)
	good := httptest.NewServer(service)
	defer good.Close()
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/native/export" {
			service.ServeHTTP(w, r)
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var req struct{ Names []string }
		if err = json.Unmarshal(raw, &req); err != nil {
			t.Error(err)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		response := httptest.NewRecorder()
		service.ServeHTTP(response, r)
		data := response.Body.Bytes()
		if response.Code == 200 && (len(req.Names) > 1 || req.Names[0] == "bad") {
			data = data[:len(data)/2]
		}
		w.WriteHeader(response.Code)
		_, _ = w.Write(data)
	}))
	defer broken.Close()
	ctx := context.Background()
	window := syncRange{From: testMetricNow - 10, Until: testMetricNow}
	baseline := newSyncLiveBackend(syncLiveOptions{})
	want, err := baseline.WindowPresence(ctx, good.URL, names, window)
	if err != nil {
		t.Fatal(err)
	}
	want["bad"] = []syncDigest{{Hash: syncWindowUnresolved}}
	backend := newSyncLiveBackend(syncLiveOptions{})
	got, err := backend.WindowPresence(ctx, broken.URL, names, window)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("truncated results leaked or healthy names lost", got, want, err)
	}
}

func TestSyncWindowConflictSelectionIsUnresolved(t *testing.T) {
	source := nativeControllerStore(t)
	names := []string{"a", "bad", "c", "d"}
	for _, name := range names {
		addPoints(t, source, name, math.Copysign(0, -1))
	}
	service := nativeControllerService(t, source)
	good := httptest.NewServer(service)
	defer good.Close()
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/native/export" {
			service.ServeHTTP(w, r)
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var req struct{ Names []string }
		if err = json.Unmarshal(raw, &req); err != nil {
			t.Error(err)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		response := httptest.NewRecorder()
		service.ServeHTTP(response, r)
		data := response.Body.Bytes()
		if response.Code == 200 && (len(req.Names) > 1 || req.Names[0] == "bad") {
			http.Error(w, "source archive has newer ring owners beyond window retention", http.StatusConflict)
			return
		}
		w.WriteHeader(response.Code)
		_, _ = w.Write(data)
	}))
	defer broken.Close()
	ctx := context.Background()
	window := syncRange{From: testMetricNow - 10, Until: testMetricNow}
	baseline := newSyncLiveBackend(syncLiveOptions{})
	want, err := baseline.WindowPresence(ctx, good.URL, names, window)
	if err != nil {
		t.Fatal(err)
	}
	want["bad"] = []syncDigest{{Hash: syncWindowUnresolved}}
	backend := newSyncLiveBackend(syncLiveOptions{})
	got, err := backend.WindowPresence(ctx, broken.URL, names, window)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("truncated results leaked or healthy names lost", got, want, err)
	}
}

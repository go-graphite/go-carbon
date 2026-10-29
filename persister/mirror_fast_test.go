package persister

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
)

func TestMonotonicMirrorProofAgainstSourceWindows(t *testing.T) {
	now := int(time.Now().Unix() / 5 * 5)
	for _, tc := range []struct {
		name, retention string
		input           []*whisper.TimeSeriesPoint
		seed, sidecar   bool
		want            bool
	}{
		{"fresh", "1s:2m", []*whisper.TimeSeriesPoint{{Time: now - 10, Value: .1}, {Time: now - 9, Value: math.Copysign(0, -1)}, {Time: now - 8, Value: 1e100}}, true, false, true},
		{"aligned-coarse", "5s:10m", []*whisper.TimeSeriesPoint{{Time: now - 9, Value: 1}, {Time: now - 4, Value: 2}}, true, false, true},
		{"late", "1s:2m", []*whisper.TimeSeriesPoint{{Time: now - 40, Value: 1}}, true, false, false},
		{"duplicate", "1s:2m", []*whisper.TimeSeriesPoint{{Time: now - 10, Value: 1}, {Time: now - 10, Value: 2}}, true, false, true},
		{"same-aligned-slot", "5s:10m", []*whisper.TimeSeriesPoint{{Time: now - 9, Value: 1}, {Time: now - 8, Value: 2}}, true, false, true},
		{"descending", "1s:2m", []*whisper.TimeSeriesPoint{{Time: now - 9, Value: 1}, {Time: now - 10, Value: 2}}, true, false, true},
		{"coarse-reordered-winner", "5s:10m", []*whisper.TimeSeriesPoint{{Time: now - 8, Value: 1}, {Time: now - 9, Value: 2}, {Time: now - 9, Value: 3}}, true, false, true},
		{"reordered-sidecar", "1s:2m", []*whisper.TimeSeriesPoint{{Time: now - 8, Value: .1}, {Time: now - 10, Value: math.Copysign(0, -1)}, {Time: now - 9, Value: 1e100}}, true, true, true},
		{"future", "1s:2m", []*whisper.TimeSeriesPoint{{Time: now + 10, Value: 1}}, true, false, false},
		{"nan", "1s:2m", []*whisper.TimeSeriesPoint{{Time: now - 10, Value: math.NaN()}}, true, false, false},
		{"infinity", "1s:2m", []*whisper.TimeSeriesPoint{{Time: now - 10, Value: math.Inf(1)}}, true, false, false},
		{"retention-edge", "1s:2m", []*whisper.TimeSeriesPoint{{Time: now - 119, Value: 1}}, false, false, false},
		{"sidecar", "1s:2m", []*whisper.TimeSeriesPoint{{Time: now - 10, Value: 1}}, true, true, true},
		{"sidecar-coarse", "5s:10m", []*whisper.TimeSeriesPoint{{Time: now - 9, Value: 1}, {Time: now - 4, Value: 2}}, true, true, true},
		{"rollups", "1s:2m,10s:10m", []*whisper.TimeSeriesPoint{{Time: now - 10, Value: 1}}, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "source.wsp")
			w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs(tc.retention), whisper.Average, 0, &whisper.Options{Compressed: true, OutOfOrder: true})
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			if tc.seed {
				if err = w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: now - 30, Value: 3.25}}); err != nil {
					t.Fatal(err)
				}
			}
			if tc.sidecar {
				if err = w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: now - 50, Value: 7.75}}); err != nil {
					t.Fatal(err)
				}
			}
			if got := w.CanMirrorMonotonicBatch(tc.input, int64(now)); got != tc.want {
				t.Fatalf("proof=%v want=%v sidecar=%q", got, tc.want, w.OutOfOrderPath())
			}
			if !tc.want {
				return
			}
			if err = w.UpdateMany(tc.input); err != nil {
				t.Fatal(err)
			}
			step := w.Retentions()[0].SecondsPerPoint()
			// The writer sorts in place before choosing the last aligned-slot
			// winner. The fast mirror consumes that same post-write order.
			expected := make(map[int]float64)
			for _, p := range tc.input {
				stamp := p.Time - p.Time%step
				expected[stamp] = p.Value
			}
			for stamp, value := range expected {
				series, err := w.FetchArchiveWindow(whisper.ArchiveWindow{Archive: 0, From: stamp, Until: stamp + step})
				if err != nil {
					t.Fatal(err)
				}
				if len(series.Values()) != 1 || math.Float64bits(series.Values()[0]) != math.Float64bits(value) {
					t.Fatalf("source window differs from proven input at%d: %v", stamp, series.Values())
				}
			}
		})
	}
}

func TestMonotonicMirrorChecksSidecarOwners(t *testing.T) {
	now := int(time.Now().Unix())
	for _, tc := range []struct {
		name  string
		delta int
		value float64
		want  bool
	}{
		{"unrelated", -50, 7.75, true},
		{"unrelated-nan", -50, math.NaN(), true},
		{"overlap", -10, 7.75, false},
		{"overlap-nan", -10, math.NaN(), false},
		{"future-alias", 110, 7.75, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "source.wsp")
			retentions := whisper.MustParseRetentionDefs("1s:2m")
			options := &whisper.Options{Compressed: true, OutOfOrder: true}
			w, err := whisper.CreateWithOptions(path, retentions, whisper.Average, 0, options)
			if err != nil {
				t.Fatal(err)
			}
			if err = w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: now - 30, Value: 3.25}}); err != nil {
				t.Fatal(err)
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			// A retained or externally written sidecar may contain an owner
			// beyond the current main watermark. Do not infer its contents from
			// the main-file bounds or from a float-only Fetch of an explicit NaN.
			side, err := whisper.CreateWithOptions(whisper.OutOfOrderSidecarPath(path), retentions, whisper.Average, 0, &whisper.Options{Sparse: true})
			if err != nil {
				t.Fatal(err)
			}
			if err = side.UpdateMany([]*whisper.TimeSeriesPoint{{Time: now + tc.delta, Value: tc.value}}); err != nil {
				t.Fatal(err)
			}
			if err = side.Close(); err != nil {
				t.Fatal(err)
			}
			w, err = whisper.OpenWithOptions(path, options)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			input := []*whisper.TimeSeriesPoint{{Time: now - 10, Value: math.Copysign(0, -1)}, {Time: now - 9, Value: 1.125}}
			if got := w.CanMirrorMonotonicBatch(input, int64(now)); got != tc.want {
				t.Fatalf("proof=%v want=%v sidecar=%q", got, tc.want, w.OutOfOrderPath())
			}
			if !tc.want {
				return
			}
			if err = w.UpdateMany(input); err != nil {
				t.Fatal(err)
			}
			for _, point := range input {
				series, err := w.FetchArchiveWindow(whisper.ArchiveWindow{Archive: 0, From: point.Time, Until: point.Time + 1})
				if err != nil {
					t.Fatal(err)
				}
				if len(series.Values()) != 1 || math.Float64bits(series.Values()[0]) != math.Float64bits(point.Value) {
					t.Fatalf("source window differs from proven input at%d: %v", point.Time, series.Values())
				}
			}
		})
	}
}

func TestMirrorRejectedOlderOwnerPreservesFuture(t *testing.T) {
	fixed := time.Now()
	oldNow := whisper.Now
	whisper.Now = func() time.Time { return fixed }
	defer func() { whisper.Now = oldNow }()
	for _, ooo := range []bool{false, true} {
		t.Run(map[bool]string{false: "rejected", true: "out-of-order"}[ooo], func(t *testing.T) {
			root := t.TempDir()
			original := filepath.Join(root, "original")
			if err := os.MkdirAll(original, 0700); err != nil {
				t.Fatal(err)
			}
			source := filepath.Join(original, "metric.wsp")
			now := int(time.Now().Unix())
			w, err := whisper.CreateWithOptions(source, whisper.MustParseRetentionDefs("1s:2m"), whisper.Average, 0, &whisper.Options{Compressed: true, OutOfOrder: ooo})
			if err != nil {
				t.Fatal(err)
			}
			if err = w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: now + 80, Value: 3.25}}); err != nil {
				t.Fatal(err)
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			before, err := whisperio.ReadSnapshot(ctx, "metric", source)
			if err != nil {
				t.Fatal(err)
			}
			db, err := openTestStore(t, filepath.Join(root, "exact"), store.Options{ExactBlocks: true})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err = db.CreateFromSnapshot(ctx, before); err != nil {
				t.Fatal(err)
			}
			cache := &fakeCache{}
			p := newOOOTestPersister(t, original, cache)
			p.outOfOrder.enabled = ooo
			p.SetMetricStore(db)
			p.SetMirrorFiles(true)
			p.SetMirrorState(&MirrorState{ScratchDir: root})
			cache.add("metric", int64(now-40), 7.75)
			p.store("metric")
			if cache.confirmedPoints() != 1 {
				t.Fatal("batch not confirmed")
			}
			want, err := whisperio.ReadSnapshot(ctx, "metric", source)
			if err != nil {
				t.Fatal(err)
			}
			got, err := db.Snapshot(ctx, "metric")
			if err != nil {
				t.Fatal(err)
			}
			if err = store.EqualSnapshots(want, got); err != nil {
				t.Fatal(err)
			}
			if err = db.Compact(); err != nil {
				t.Fatal(err)
			}
			got, err = db.Snapshot(ctx, "metric")
			if err != nil {
				t.Fatal(err)
			}
			if err = store.EqualSnapshots(want, got); err != nil {
				t.Fatal(err)
			}
		})
	}
}

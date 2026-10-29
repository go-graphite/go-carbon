package whisperio

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

func TestContinuationNewMetricFormatAndMutationGuards(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Unix(1792432631, 0)
	options := store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{}, Now: func() time.Time { return now }}
	db, err := openTestStore(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	config := store.MetricConfig{Name: "new", Retentions: []store.Retention{{Step: 1, Count: 600}}, AggregationMethod: store.Average, XFilesFactor: .5}
	m, err := db.Create(ctx, config)
	if err != nil || !m.Continuation {
		t.Fatalf("new metric skipped continuation: %+v %v", m, err)
	}
	values := []float64{math.Copysign(0, -1), math.Float64frombits(0x7ff800000000042a), math.Inf(1), math.Inf(-1), .1, 1e-300, 1e100, 9007199254740991}
	var input []store.Point
	for i, v := range values {
		input = append(input, store.Point{Timestamp: now.Unix() - 30 + int64(i), Value: v})
	}
	if err := db.UpdateMany(ctx, "new", input); err != nil {
		t.Fatal(err)
	}
	want, err := db.Snapshot(ctx, "new")
	if err != nil {
		t.Fatal(err)
	}
	if len(want.Archives[0].Points) != len(values) {
		t.Fatal("new metric lost points")
	}
	for i, p := range want.Archives[0].Points {
		if math.Float64bits(p.Value) != math.Float64bits(values[i]) {
			t.Fatalf("new metric changed float bits %d", i)
		}
	}
	for _, mutate := range []func() error{
		func() error { _, err := db.Replace(ctx, want); return err },
		func() error { _, err := db.Fill(ctx, want); return err },
		func() error { _, err := db.ReconcileSnapshot(ctx, want); return err },
		func() error {
			return db.UpdateManyWithArchiveValues(ctx, "new", nil, []store.ArchiveValue{{Archive: 0, Timestamp: input[0].Timestamp, Value: 99, Present: true}})
		},
	} {
		if err := mutate(); err == nil {
			t.Fatal("flattened operation accepted for continuation metric")
		}
		got, err := db.Snapshot(ctx, "new")
		if err != nil {
			t.Fatal(err)
		}
		if err := store.EqualSnapshots(want, got); err != nil {
			t.Fatal("rejected mutation changed state", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	for _, wrong := range []store.Options{{ExactBlocks: true}, {}} {
		if s, err := openTestStore(dir, wrong); !errors.Is(err, store.ErrFormat) {
			if s != nil {
				_ = s.Close()
			}
			t.Fatalf("accepted v6 without codec: %v", err)
		}
	}
	db, err = openTestStore(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.Snapshot(ctx, "new")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(want, got); err != nil {
		t.Fatal("restart changed state", err)
	}
	if err := db.Delete(ctx, "new"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Fetch(ctx, "new", int(now.Unix()-60), int(now.Unix())); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("deleted metric remains visible: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	legacyDir := t.TempDir()
	old, err := openTestStore(legacyDir, store.Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(legacyDir, "CHUNKSTORE"))
	if err != nil {
		t.Fatal(err)
	}
	if upgraded, err := openTestStore(legacyDir, options); !errors.Is(err, store.ErrFormat) {
		if upgraded != nil {
			_ = upgraded.Close()
		}
		t.Fatal("silently upgraded a v4 store")
	}
	after, err := os.ReadFile(filepath.Join(legacyDir, "CHUNKSTORE"))
	if err != nil || string(before) != string(after) {
		t.Fatal("rejected format change modified the marker")
	}
}

func TestContinuationClassicSourceKeepsCircularSlots(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(source, "classic.wsp")
	now := time.Unix(1792432631, 0)
	clock := func() time.Time { return now }
	w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1s:20s,5s:2m,30s:10m"), whisper.Average, .5, &whisper.Options{Now: clock})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(now.Unix() - 15), Value: .1}, {Time: int(now.Unix() - 4), Value: 3}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := openTestStore(filepath.Join(root, "shared"), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{}, IncrementalSource: source, Now: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	capture, err := ReadSnapshot(ctx, "classic", path)
	if err != nil {
		t.Fatal(err)
	}
	staged, err := db.StageFileSnapshot(ctx, capture)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.PublishFileOwnership(ctx, capture, staged); err != nil {
		t.Fatal(err)
	}
	for seq := 0; seq < 70; seq++ {
		now = now.Add(3 * time.Second)
		input := []store.Point{{Timestamp: now.Unix() - 2, Value: float64(seq) + .1}, {Timestamp: now.Unix() - 17, Value: float64(seq) - .5}}
		w, err := whisper.OpenWithOptions(path, &whisper.Options{Now: clock})
		if err != nil {
			t.Fatal(err)
		}
		points := make([]*whisper.TimeSeriesPoint, len(input))
		for i, p := range input {
			points[i] = &whisper.TimeSeriesPoint{Time: int(p.Timestamp), Value: p.Value}
		}
		if err := w.UpdateMany(points); err != nil {
			t.Fatal(err)
		}
		if err := db.UpdateMany(ctx, "classic", input); err != nil {
			t.Fatal(err)
		}
		for _, age := range []int{8, 20, 100, 240} {
			from, until := int(now.Unix())-age, int(now.Unix())-1
			want, err := w.Fetch(from, until)
			if err != nil {
				t.Fatal(err)
			}
			got, err := db.Fetch(ctx, "classic", from, until)
			if err != nil {
				t.Fatal(err)
			}
			if want.Step() != got.Step || len(want.Values()) != len(got.Values) {
				t.Fatal("classic fetch shape differs")
			}
			for i, v := range want.Values() {
				if math.IsNaN(v) && math.IsNaN(got.Values[i]) {
					continue
				}
				if math.Float64bits(v) != math.Float64bits(got.Values[i]) {
					t.Fatalf("classic round%d age%d differs", seq, age)
				}
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

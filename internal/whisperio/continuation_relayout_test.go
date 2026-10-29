package whisperio

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// Relayout of a store converted in v6 must not change any read. A v6 control
// store and an upgraded, relaid-out store receive identical conversions and
// updates; their fetches must match before and after the move and after
// further updates that rotate blocks.
func TestRelayoutMatchesV6Control(t *testing.T) {
	ctx := context.Background()
	const base = int64(1792368000)
	now := time.Unix(base, 0)
	clock := func() time.Time { return now }
	root := t.TempDir()
	const metrics = 12
	for _, dir := range []string{"a", "b"} {
		if err := os.MkdirAll(filepath.Join(root, dir, "source"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < metrics; i++ {
		path := filepath.Join(root, "a", "source", fmt.Sprintf("m%02d.wsp", i))
		w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1h:40d,1d:2y"), whisper.Average, 0,
			&whisper.Options{Compressed: true, OutOfOrder: true, Now: clock, PointsPerBlock: 6})
		if err != nil {
			t.Fatal(err)
		}
		var points []*whisper.TimeSeriesPoint
		for ts := base - 600*86400; ts < base; ts += 3600 {
			points = append(points, &whisper.TimeSeriesPoint{Time: int(ts), Value: math.Sin(float64(ts)/977+float64(i)) * 1e5})
		}
		if err := w.UpdateMany(points); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "b", "source", fmt.Sprintf("m%02d.wsp", i)), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	open := func(dir string, buckets bool) *store.Store {
		t.Helper()
		db, err := openMemoryTestStore(t, filepath.Join(root, dir, "shared"), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{},
			IncrementalSource: filepath.Join(root, dir, "source"), TimeBuckets: buckets, Now: clock, SyncInterval: time.Hour,
			CacheSize: 16 << 20, MemTableSize: 4 << 20})
		if err != nil {
			t.Fatal(err)
		}
		return db
	}
	a, b := open("a", false), open("b", false)
	defer func() { a.Close(); b.Close() }()
	for _, side := range []struct {
		dir string
		db  *store.Store
	}{{"a", a}, {"b", b}} {
		for i := 0; i < metrics; i++ {
			name := fmt.Sprintf("m%02d", i)
			capture, err := ReadSnapshotIn(ctx, name, filepath.Join(root, side.dir, "source", name+".wsp"), filepath.Join(root, side.dir))
			if err != nil {
				t.Fatal(err)
			}
			staged, err := side.db.StageFileSnapshot(ctx, capture)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := side.db.PublishFileOwnership(ctx, capture, staged); err != nil {
				t.Fatal(err)
			}
		}
	}
	update := func(hours int) {
		t.Helper()
		for h := 0; h < hours; h++ {
			now = now.Add(time.Hour)
			for i := 0; i < metrics; i++ {
				ts := now.Unix() - 60
				input := []store.Point{{Timestamp: ts, Value: math.Sin(float64(ts)/977+float64(i)) * 1e5}}
				if h%17 == 5 {
					// A late point rewrites an older block.
					input = append(input, store.Point{Timestamp: ts - 30*3600, Value: float64(h)})
				}
				for _, db := range []*store.Store{a, b} {
					if err := db.UpdateMany(ctx, fmt.Sprintf("m%02d", i), input); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
	}
	same := func(when string) {
		t.Helper()
		for i := 0; i < metrics; i++ {
			name := fmt.Sprintf("m%02d", i)
			for _, age := range []int{86400, 10 * 86400, 39 * 86400, 700 * 86400} {
				from, until := int(now.Unix())-age, int(now.Unix())
				want, err := a.Fetch(ctx, name, from, until)
				if err != nil {
					t.Fatal(err)
				}
				got, err := b.Fetch(ctx, name, from, until)
				if err != nil {
					t.Fatalf("%s %s age %d: %v", when, name, age, err)
				}
				if want.FromTime != got.FromTime || want.Step != got.Step || len(want.Values) != len(got.Values) {
					t.Fatalf("%s %s age %d: shape differs", when, name, age)
				}
				for j, v := range want.Values {
					if math.Float64bits(v) != math.Float64bits(got.Values[j]) && !(math.IsNaN(v) && math.IsNaN(got.Values[j])) {
						t.Fatalf("%s %s age %d slot %d: v6 %g relaid %g", when, name, age, j, v, got.Values[j])
					}
				}
			}
		}
	}
	update(30)
	same("before upgrade")
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b = open("b", true)
	if err := b.RelayoutContinuation(ctx, 0); err != nil {
		t.Fatal(err)
	}
	stats := b.RelayoutStats()
	if !stats.Done || stats.Metrics != metrics || stats.Blocks < metrics || stats.Skipped != 0 {
		t.Fatalf("relayout stats %+v", stats)
	}
	same("after relayout")
	update(24 * 12)
	same("after further updates")
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b = open("b", true)
	if err := b.RelayoutContinuation(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if s := b.RelayoutStats(); !s.Done || s.Metrics != 0 {
		t.Fatalf("finished relayout ran again: %+v", s)
	}
	same("after reopen")
}

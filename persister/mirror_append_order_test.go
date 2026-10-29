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

type capturedArchiveValuesStore struct {
	*store.Store
	values []store.ArchiveValue
}

func (s *capturedArchiveValuesStore) UpdateManyWithArchiveValues(ctx context.Context, name string, input []store.Point, values []store.ArchiveValue) error {
	s.values = append([]store.ArchiveValue(nil), values...)
	return s.Store.UpdateManyWithArchiveValues(ctx, name, input, values)
}

func TestMirrorAppendOrderMatchesCompleteSource(t *testing.T) {
	fixed := time.Now().Truncate(time.Minute).Add(-15 * time.Second)
	previous := whisper.Now
	whisper.Now = func() time.Time { return fixed }
	t.Cleanup(func() { whisper.Now = previous })
	for _, retention := range []string{"1s:2m", "5s:10m", "60s:2h"} {
		for _, order := range []string{"duplicates", "reordered", "coarse-winner"} {
			t.Run(retention+"/"+order, func(t *testing.T) {
				ctx := context.Background()
				root := t.TempDir()
				original := filepath.Join(root, "original")
				if err := os.MkdirAll(original, 0700); err != nil {
					t.Fatal(err)
				}
				retentions := whisper.MustParseRetentionDefs(retention)
				step := retentions[0].SecondsPerPoint()
				base := fixed.Unix()/int64(step)*int64(step) - 7*int64(step)
				path := filepath.Join(original, "metric.wsp")
				w, err := whisper.CreateWithOptions(path, retentions, whisper.Average, 0, &whisper.Options{Compressed: true, OutOfOrder: true})
				if err != nil {
					t.Fatal(err)
				}
				err = w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(base) - step, Value: 3.25}})
				closeErr := w.Close()
				if err != nil || closeErr != nil {
					t.Fatal(err, closeErr)
				}
				snapshot, err := whisperio.ReadSnapshot(ctx, "metric", path)
				if err != nil {
					t.Fatal(err)
				}
				databasePath := filepath.Join(root, "exact")
				opts := store.Options{ExactBlocks: true, Now: func() time.Time { return fixed }}
				db, err := openTestStore(t, databasePath, opts)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if db != nil {
						_ = db.Close()
					}
				})
				if _, err = db.CreateFromSnapshot(ctx, snapshot); err != nil {
					t.Fatal(err)
				}
				cache := &fakeCache{}
				p := newOOOTestPersister(t, original, cache)
				spy := &capturedArchiveValuesStore{Store: db}
				p.SetMetricStore(spy)
				p.SetMirrorFiles(true)
				p.SetMirrorState(&MirrorState{ScratchDir: root})
				stamps := []int64{base, base, base + int64(step), base + int64(step)}
				values := []float64{.1, math.Copysign(0, -1), 1e-300, 1e100}
				if order == "reordered" {
					stamps = []int64{base + int64(step), base, base + 2*int64(step), base}
				}
				if order == "coarse-winner" && step > 1 {
					stamps = []int64{base + 1, base, base, base + int64(step) - 1}
				}
				for i, stamp := range stamps {
					cache.add("metric", stamp, values[i])
				}
				p.store("metric")
				if cache.confirmedPoints() != len(stamps) {
					t.Fatalf("confirmed=%d want=%d", cache.confirmedPoints(), len(stamps))
				}
				// The source-read fallback deduplicates slots. Retaining every
				// input correction proves this used the newly admitted fast path.
				if len(spy.values) != len(stamps) {
					t.Fatalf("fast path not used: corrections=%d input=%d", len(spy.values), len(stamps))
				}
				want, err := whisperio.ReadSnapshot(ctx, "metric", path)
				if err != nil {
					t.Fatal(err)
				}
				check := func() {
					t.Helper()
					got, err := db.Snapshot(ctx, "metric")
					if err != nil {
						t.Fatal(err)
					}
					if err := store.EqualSnapshots(want, got); err != nil {
						t.Fatal(err)
					}
				}
				check()
				if err := db.Compact(); err != nil {
					t.Fatal(err)
				}
				check()
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db = nil
				db, err = openTestStore(t, databasePath, opts)
				if err != nil {
					t.Fatal(err)
				}
				check()
			})
		}
	}
}

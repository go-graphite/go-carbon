package whisperio

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

func TestConvertedActivityUsesFinestPhysicalArchive(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		compressed, ingest, buckets bool
	}{
		{"classic", false, false, false}, {"compressed", true, false, false}, {"ingested", true, true, false}, {"time_buckets", true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compressed := tc.compressed
			ctx := context.Background()
			root := t.TempDir()
			source := filepath.Join(root, "source")
			if err := os.Mkdir(source, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(source, "metric.wsp")
			conversion := time.Unix(1790000100, 0)
			clock := func() time.Time { return conversion }
			w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1s:2m,60s:1h"), whisper.Average, .5, &whisper.Options{Compressed: compressed, OutOfOrder: compressed, Now: clock, PointsPerBlock: 16})
			if err != nil {
				t.Fatal(err)
			}
			if err := w.UpdateManyForArchive([]*whisper.TimeSeriesPoint{{Time: 1790000001, Value: 42}, {Time: 1790000002, Value: 43}}, 120); err != nil {
				t.Fatal(err)
			}
			if err := w.UpdateManyForArchive([]*whisper.TimeSeriesPoint{{Time: 1790000040, Value: 99}}, 3600); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.Chtimes(path, conversion, conversion); err != nil {
				t.Fatal(err)
			}
			db, err := store.Open(filepath.Join(root, "store"), store.Options{ExactBlocks: true, IngestFileStages: tc.ingest, IngestLinger: time.Millisecond, TimeBuckets: tc.buckets, Continuation: &ContinuationCodec{}, IncrementalSource: source, Now: clock, SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			snapshot, err := ReadSnapshot(ctx, "metric", path)
			if err != nil {
				t.Fatal(err)
			}
			// Leave LastUpdate unset to exercise the store fallback used by both
			// ordinary and SST-ingested stages, before any source can be reclaimed.
			m, err := db.StageFileSnapshot(ctx, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if !m.LastUpdate.Equal(conversion) {
				t.Fatalf("mtime %s", m.LastUpdate)
			}
			m, err = db.PublishFileOwnership(ctx, snapshot, m)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.CompleteFileCleanup(ctx, m); err != nil {
				t.Fatal(err)
			}
			m, err = db.Metadata(ctx, "metric")
			if err != nil {
				t.Fatal(err)
			}
			// Even after the finest retention expires, its physical points are the
			// activity proxy. The newer coarse point must never select this activity.
			conversion = conversion.Add(24 * time.Hour)
			baseline := store.ActivityBaseline{Name: m.Name, ID: m.ID, Generation: m.Generation, Revision: m.Revision, LastUpdate: m.LastUpdate}
			newest, err := db.BackfillActivity(ctx, baseline, m.LastUpdate.Add(-time.Minute), m.LastUpdate.Add(time.Minute), true)
			if err != nil || newest.Unix() != 1790000002 {
				t.Fatalf("newest=%s error=%v", newest, err)
			}
			m, err = db.Metadata(ctx, "metric")
			if err != nil {
				t.Fatal(err)
			}
			outcomes, err := db.DeleteBatchIfUnchanged(ctx, []store.Metadata{m})
			if err != nil || outcomes[0] != nil {
				t.Fatalf("batch continuation delete %v %v", outcomes, err)
			}
			if _, err := db.Snapshot(ctx, "metric"); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("deleted continuation survived: %v", err)
			}
		})
	}
}

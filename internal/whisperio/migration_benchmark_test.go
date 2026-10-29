package whisperio

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// Benchmarks only staging/committing physical writer state, with the same frozen
// source captures and database budgets for both implementations. Source capture,
// readback verification, ownership publication and reclamation are outside this
// measurement; the API/restart tests cover those gates.
func BenchmarkContinuationStageCapturedCorpus(b *testing.B) {
	benchmarkContinuationCapturedCorpus(b, false)
}

// Includes physical readback, exact snapshot comparison and synchronous durable
// ownership publication. Frozen inputs stay in place for matched repeated runs.
func BenchmarkContinuationPublishCapturedCorpus(b *testing.B) {
	benchmarkContinuationCapturedCorpus(b, true)
}

func benchmarkContinuationCapturedCorpus(b *testing.B, publish bool) {
	corpus := os.Getenv("GO_CARBON_MIGRATION_CORPUS")
	if corpus == "" {
		b.Skip("set GO_CARBON_MIGRATION_CORPUS to a frozen capture directory containing COHORT.json and files/")
	}
	manifest, err := os.ReadFile(filepath.Join(corpus, "COHORT.json"))
	if err != nil {
		b.Fatal(err)
	}
	var cohort struct {
		Records []struct{ Metric, Path string }
	}
	if err := json.Unmarshal(manifest, &cohort); err != nil || len(cohort.Records) == 0 {
		b.Fatalf("invalid corpus: %v", err)
	}
	ctx := context.Background()
	root := b.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		b.Fatal(err)
	}
	var snapshots []store.Snapshot
	for _, record := range cohort.Records {
		snapshot, err := ReadSnapshotIn(ctx, record.Metric, filepath.Join(corpus, "files", record.Path), root)
		if err != nil {
			b.Fatal(err)
		}
		snapshots = append(snapshots, snapshot)
	}
	db, err := store.Open(filepath.Join(root, "shared"), store.Options{
		ExactBlocks: true, Continuation: &ContinuationCodec{}, IncrementalSource: source,
		CacheSize: 2 << 30, MemTableSize: 64 << 20, SyncInterval: time.Second,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshot := snapshots[i%len(snapshots)]
		snapshot.Metadata.Name = fmt.Sprintf("stage_captured.metric_%d", i)
		m, err := db.StageFileSnapshot(ctx, snapshot)
		if err != nil {
			b.Fatal(err)
		}
		if publish {
			if _, err := db.PublishFileOwnership(ctx, snapshot, m); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
}

// BenchmarkContinuationConvertDense covers capture, physical import, independent
// readback, visible comparison and durable publication. Each iteration starts a
// new metric; the immutable dense source is shared, as by a conversion worker.
func BenchmarkContinuationConvertDense(b *testing.B) {
	ctx := context.Background()
	root := b.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		b.Fatal(err)
	}
	path := filepath.Join(source, "dense.wsp")
	now := time.Unix(1793200000, 0)
	w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1s:2d,60s:28d,3600s:2y"), whisper.Average, .5,
		&whisper.Options{Compressed: true, OutOfOrder: true, PointsPerBlock: 7200, Now: func() time.Time { return now }})
	if err != nil {
		b.Fatal(err)
	}
	random := uint64(19)
	for archive := 2; archive >= 0; archive-- {
		step := []int{1, 60, 3600}[archive]
		count := []int{172000, 40000, 17000}[archive]
		points := make([]whisper.TimeSeriesPoint, count)
		input := make([]*whisper.TimeSeriesPoint, count)
		for i := range points {
			random ^= random << 13
			random ^= random >> 7
			random ^= random << 17
			points[i] = whisper.TimeSeriesPoint{Time: (int(now.Unix())/step - count + i) * step, Value: float64((i/17)%23) + .1}
			if random%997 == 0 {
				points[i].Value += float64(random%17) / 8
			}
			input[i] = &points[i]
		}
		if err := w.UpdateManyForArchive(input, w.Retentions()[archive].MaxRetention()); err != nil {
			b.Fatal(err)
		}
	}
	physical, err := w.ContinuationLayout()
	if err != nil {
		b.Fatal(err)
	}
	encoded, stored := 0, 0
	for _, block := range physical.Blocks {
		encoded += block.Size
		stored += block.Count
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("dense source: %d physical points in %d active block bytes; %d logical file bytes", stored, encoded, info.Size())
	db, err := store.Open(filepath.Join(root, "shared"), store.Options{
		ExactBlocks: true, Continuation: &ContinuationCodec{DirectLateWrites: true}, IncrementalSource: source,
		TimeBuckets: true, IngestFileStages: true, IngestLinger: time.Nanosecond,
		CacheSize: 32 << 20, MemTableSize: 8 << 20, SyncInterval: time.Hour,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx := WithConversionSource(ctx)
		snapshot, err := ReadSnapshotIn(ctx, fmt.Sprintf("dense.metric_%d", i), path, root)
		if err != nil {
			b.Fatal(err)
		}
		if len(snapshot.Archives[0].Points) < 10000 {
			b.Fatal("dense fixture is empty")
		}
		m, err := db.StageFileSnapshot(ctx, snapshot)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := db.PublishFileOwnership(ctx, snapshot, m); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

package persister

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// Run on the source filesystem with a frozen production capture. Each operation
// includes snapshot capture, physical/logical verification, durable publication,
// original inode archival and source reclamation. Preparing private hardlinks
// and checking their identity are outside the timer. The capture is never opened
// for writing; use a private frozen corpus, never the serving Whisper directory.
func BenchmarkIncrementalArchiveCapturedCorpus(b *testing.B) {
	corpus := os.Getenv("GO_CARBON_MIGRATION_CORPUS")
	if corpus == "" {
		b.Skip("set GO_CARBON_MIGRATION_CORPUS to a private frozen production capture")
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
	root := b.TempDir()
	source, destination := filepath.Join(root, "source"), filepath.Join(root, "shared")
	if err := os.Mkdir(source, 0700); err != nil {
		b.Fatal(err)
	}
	db, err := store.Open(destination, store.Options{
		ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: source,
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
	p := &Whisper{rootPath: source, flock: true}
	p.SetMetricStore(db)
	if err := p.SetIncrementalMigration(db, destination); err != nil {
		b.Fatal(err)
	}
	archive := filepath.Join(root, "archive")
	if err := p.SetIncrementalMigrationArchive(archive); err != nil {
		b.Fatal(err)
	}
	var cpuMicros, archivedBytes int64
	cpu := func() int64 {
		var usage syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
			b.Fatal(err)
		}
		return usage.Utime.Sec*1e6 + int64(usage.Utime.Usec) + usage.Stime.Sec*1e6 + int64(usage.Stime.Usec)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		record := cohort.Records[i%len(cohort.Records)]
		if !filepath.IsLocal(record.Path) {
			b.Fatal("noncanonical corpus path")
		}
		prefix := fmt.Sprintf("round_%d", i/len(cohort.Records))
		path := filepath.Join(source, prefix, record.Path)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			b.Fatal(err)
		}
		images := make(map[string]os.FileInfo)
		for j, original := range originalPaths(filepath.Join(corpus, "files", record.Path)) {
			info, err := os.Lstat(original)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil || !info.Mode().IsRegular() {
				b.Fatalf("invalid frozen source: %v", err)
			}
			private := originalPaths(path)[j]
			if err := os.Link(original, private); err != nil {
				b.Fatal(err)
			}
			images[private] = info
		}
		b.StartTimer()
		started := cpu()
		result, err := p.ConvertFile(context.Background(), prefix+"."+record.Metric)
		cpuMicros += cpu() - started
		b.StopTimer()
		if err != nil || result.PreviouslyOwned || result.ArchivedBytes <= 0 || result.UnlinkedBytes != 0 {
			b.Fatalf("archive conversion: %+v %v", result, err)
		}
		archivedBytes += result.ArchivedBytes
		for private, info := range images {
			if _, err := os.Lstat(private); !os.IsNotExist(err) {
				b.Fatalf("source was not reclaimed: %v", err)
			}
			relative, _ := filepath.Rel(source, private)
			got, err := os.Lstat(filepath.Join(archive, relative))
			if err != nil || !os.SameFile(info, got) || info.Size() != got.Size() || !info.ModTime().Equal(got.ModTime()) {
				b.Fatalf("original inode changed: %v", err)
			}
		}
	}
	b.ReportMetric(float64(cpuMicros)*1e3/float64(b.N), "cpu-ns/op")
	b.ReportMetric(float64(archivedBytes)/float64(b.N), "archived-B/op")
}

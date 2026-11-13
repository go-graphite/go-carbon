package chunkstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// Opt-in disk benchmark. Prepare once with CREATE_SCALE_DIR=/path go test
// -run '^TestPrepareCreateScale$' ./internal/chunkstore, then use the same
// variable with -run '^$' -bench BenchmarkCreateLargeCatalog -benchtime=20000x.
// Every measured trial uses a checkpoint, leaving the fixture unchanged.
func TestPrepareCreateScale(t *testing.T) {
	dir := os.Getenv("CREATE_SCALE_DIR")
	if dir == "" {
		t.Skip("set CREATE_SCALE_DIR for the 2M-name disk fixture")
	}
	s, err := Open(dir, createScaleOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestStore(t, s)
	high, err := s.CatalogHighWatermark(context.Background())
	if err != nil || high != 0 {
		t.Fatalf("need empty fixture: %d %v", high, err)
	}
	m := Metadata{MetricConfig: MetricConfig{Retentions: []Retention{{Step: 60, Count: 1440}}, AggregationMethod: Average}, Generation: 1, Revision: 1, LastUpdate: time.Unix(1790000000, 0)}
	for first := 0; first < 2_000_000; first += 2048 {
		batch := s.db.NewBatch()
		for i := first; i < min(first+2048, 2_000_000); i++ {
			m.ID, m.Name = uint64(i+1), createScaleName(uint64(i))
			value, err := s.encodeCatalog(m)
			if err != nil {
				t.Fatal(err)
			}
			if err := batch.Set(catalogKey(m.Name), value, nil); err != nil {
				t.Fatal(err)
			}
			if err := batch.Set(revisionKey(m), revisionBytes(m), nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := batch.Set(sequenceKey(), uint64Bytes(m.ID), nil); err != nil {
			t.Fatal(err)
		}
		if err := s.commit(batch); err != nil {
			t.Fatal(err)
		}
		if err := batch.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	t.Logf("prepared 2M names: %s", s.db.Metrics().String())
}

func createScaleOptions() Options {
	return Options{ExactBlocks: true, CacheSize: 64 << 10, MemTableSize: 4 << 20, TargetFileSize: 1 << 20, SyncInterval: time.Hour, CompactionWorkers: 2}
}
func createScaleName(i uint64) string {
	return fmt.Sprintf("catalog.metric.%016x", i*11400714819323198485)
}

func BenchmarkCreateLargeCatalog(b *testing.B) {
	dir := os.Getenv("CREATE_SCALE_DIR")
	if dir == "" {
		b.Skip("set CREATE_SCALE_DIR to the prepared 2M-name fixture")
	}
	for _, workers := range []int{1, 96} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			base, err := Open(dir, createScaleOptions())
			if err != nil {
				b.Fatal(err)
			}
			trial := filepath.Join(b.TempDir(), "trial")
			err = base.db.Checkpoint(trial)
			if err == nil {
				var marker []byte
				marker, err = os.ReadFile(filepath.Join(dir, "CHUNKSTORE"))
				if err == nil {
					err = os.WriteFile(filepath.Join(trial, "CHUNKSTORE"), marker, 0644)
				}
			}
			if closeErr := base.Close(); err != nil || closeErr != nil {
				b.Fatalf("checkpoint: %v close: %v", err, closeErr)
			}
			s, err := Open(trial, createScaleOptions())
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			high, err := s.CatalogHighWatermark(context.Background())
			if err != nil || high != 2_000_000 {
				b.Fatalf("need 2M names: %d %v", high, err)
			}
			// Build the same membership snapshot used by migrated production stores.
			// This also permits measuring the exact same harness on older builds.
			s.catalogMembership = newCatalogMembership(high)
			s.loadCatalogMembership(s.db.NewSnapshot(), s.catalogMembership)
			metrics := s.db.Metrics()
			b.Logf("fixture table count=%d block-cache=%d bytes", metrics.TableCache.Count, metrics.BlockCache.Size)
			var next atomic.Uint64
			var wg sync.WaitGroup
			failures := make(chan error, workers)
			var before, after syscall.Rusage
			b.ResetTimer()
			_ = syscall.Getrusage(syscall.RUSAGE_SELF, &before)
			for range workers {
				wg.Go(func() {
					config := MetricConfig{Retentions: []Retention{{Step: 60, Count: 1440}}, AggregationMethod: Average}
					for {
						i := next.Add(1) - 1
						if i >= uint64(b.N) {
							return
						}
						config.Name = createScaleName(high + i)
						if _, err := s.Create(context.Background(), config); err != nil {
							failures <- err
							return
						}
					}
				})
			}
			wg.Wait()
			_ = syscall.Getrusage(syscall.RUSAGE_SELF, &after)
			b.StopTimer()
			close(failures)
			for err := range failures {
				b.Fatal(err)
			}
			cpu := func(r syscall.Rusage) float64 {
				return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
			}
			b.ReportMetric((cpu(after)-cpu(before))/float64(b.N), "cpu-s/create")
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "creates/s")
			end := s.db.Metrics()
			b.ReportMetric(float64(end.BlockCache.Misses-metrics.BlockCache.Misses)/float64(b.N), "block-misses/create")
		})
	}
}

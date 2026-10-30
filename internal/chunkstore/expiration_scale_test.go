package chunkstore

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// Explicitly opt-in, incremental fixture preparation keeps each invocation
// below three minutes. The measured sweeps open the resulting real local LSM.
func TestPrepareExpirationScaleStore(t *testing.T) {
	dir := os.Getenv("EXPIRATION_SCALE_DIR")
	if dir == "" {
		t.Skip("set EXPIRATION_SCALE_DIR and EXPIRATION_SCALE_ADD")
	}
	count, err := strconv.Atoi(os.Getenv("EXPIRATION_SCALE_ADD"))
	if err != nil || count < 0 || count > 1_000_000 {
		t.Fatal("add must be 0..1000000")
	}
	s, err := Open(dir, Options{CacheSize: 32 << 20, MemTableSize: 16 << 20, SyncInterval: time.Hour, CompactionWorkers: 2, ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestStore(t, s)
	first, err := s.CatalogHighWatermark(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := Metadata{MetricConfig: MetricConfig{Retentions: []Retention{{Step: 60, Count: 1440}, {Step: 3600, Count: 8760}}, AggregationMethod: Average}, Generation: 1, Revision: 1, LastUpdate: time.Unix(1790000000, 0)}
	var c chunk
	c.set((1789999980/60%1440)%chunkSlots, Point{Timestamp: 1789999980, Value: 42})
	encoded := s.encodeChunk(&c)
	started := time.Now()
	for offset := 0; offset < count; {
		b := s.db.NewBatch()
		for range min(2048, count-offset) {
			offset++
			m.ID = first + uint64(offset)
			m.Name = fmt.Sprintf("aggregations.secondly.expiration_benchmark.by_host.host%09d.requests", m.ID*15485863%4294967296)
			catalog, err := s.encodeCatalog(m)
			if err != nil {
				t.Fatal(err)
			}
			for _, kv := range []struct{ k, v []byte }{{catalogKey(m.Name), catalog}, {revisionKey(m), revisionBytes(m)}, {chunkKey(m, 0, (1790000000/60%1440)/chunkSlots), encoded}} {
				if err := b.Set(kv.k, kv.v, nil); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := b.Set(sequenceKey(), uint64Bytes(m.ID), nil); err != nil {
			t.Fatal(err)
		}
		if err := b.Commit(pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		b.Close()
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	t.Logf("catalog names=%d added=%d fixture time=%s", first+uint64(count), count, time.Since(started))
}

func BenchmarkExpirationScaleDelete(b *testing.B) {
	dir := os.Getenv("EXPIRATION_SCALE_DIR")
	if dir == "" {
		b.Skip("set EXPIRATION_SCALE_DIR to a prepared million-name store")
	}
	interval := time.Hour
	if os.Getenv("EXPIRATION_SCALE_SYNC") == "1" {
		interval = 0
	}
	s, err := Open(dir, Options{CacheSize: 32 << 20, MemTableSize: 16 << 20, SyncInterval: interval, CompactionWorkers: 2, ExactBlocks: true})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	count, err := s.CatalogHighWatermark(context.Background())
	if err != nil || count < 1_000_000 {
		b.Fatalf("need >=1M-name store: %d %v", count, err)
	}
	size, _ := strconv.Atoi(os.Getenv("EXPIRATION_SCALE_BATCH"))
	if size == 0 {
		size = 128
	}
	workers, _ := strconv.Atoi(os.Getenv("EXPIRATION_SCALE_WORKERS"))
	if workers == 0 {
		workers = 1
	}
	deletes, _ := strconv.Atoi(os.Getenv("EXPIRATION_SCALE_DELETES"))
	if deletes == 0 {
		deletes = 20_000
	}
	_, cursor, _, err := s.CatalogChanges(context.Background(), 0)
	if err != nil {
		b.Fatal(err)
	}
	_ = cursor
	var before, after syscall.Rusage
	metrics := s.db.Metrics()
	var scanTime time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &before)
	for range b.N {
		ctx, cancel := context.WithCancel(context.Background())
		jobs := make(chan []Metadata, workers)
		failures := make(chan error, workers)
		var wg sync.WaitGroup
		for range workers {
			wg.Go(func() {
				for page := range jobs {
					outcomes, err := s.DeleteBatchIfUnchanged(ctx, page)
					if err == nil {
						for _, outcome := range outcomes {
							if outcome != nil {
								err = outcome
								break
							}
						}
					}
					if err != nil {
						failures <- err
						cancel()
						return
					}
				}
			})
		}
		afterName := ""
		for left := deletes; left > 0; {
			start := time.Now()
			page, err := s.ListPage(ctx, "", afterName, min(size, left))
			scanTime += time.Since(start)
			if err != nil || len(page) == 0 {
				cancel()
				close(jobs)
				wg.Wait()
				b.Fatalf("page: %d %v", len(page), err)
			}
			afterName = page[len(page)-1].Name
			select {
			case jobs <- page:
			case <-ctx.Done():
			}
			left -= len(page)
		}
		close(jobs)
		wg.Wait()
		cancel()
		close(failures)
		for err := range failures {
			b.Fatal(err)
		}
	}
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &after)
	b.StopTimer()
	elapsed := b.Elapsed()
	if err := s.Compact(); err != nil {
		b.Fatal(err)
	}
	end := s.db.Metrics()
	cpu := func(r syscall.Rusage) float64 {
		return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
	}
	b.ReportMetric((cpu(after)-cpu(before))/float64(b.N*deletes), "cpu-s/delete")
	b.ReportMetric(float64(end.Compact.Count-metrics.Compact.Count), "compactions")
	var written uint64
	for i := range end.Levels {
		written += end.Levels[i].BytesCompacted - metrics.Levels[i].BytesCompacted
	}
	b.ReportMetric(float64(written), "compaction-bytes")
	b.ReportMetric(scanTime.Seconds(), "scan-seconds")
	b.ReportMetric(float64(b.N*deletes)/elapsed.Seconds(), "deletes/s")
	b.ReportMetric(elapsed.Seconds()/float64(b.N), "seconds/sweep")
}

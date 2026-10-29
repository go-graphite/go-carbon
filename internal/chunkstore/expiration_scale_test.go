package chunkstore

import (
	"context"
	"fmt"
	"os"
	"strconv"
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
	if err != nil || count < 1 || count > 1_000_000 {
		t.Fatal("add must be 1..1000000")
	}
	s, err := Open(dir, Options{CacheSize: 32 << 20, MemTableSize: 16 << 20, SyncInterval: time.Hour, CompactionWorkers: 2})
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
	c.set(0, Point{Timestamp: 1790000000, Value: 42})
	encoded := encodeChunk(&c)
	started := time.Now()
	for offset := 0; offset < count; {
		b := s.db.NewBatch()
		for range min(2048, count-offset) {
			offset++
			m.ID = first + uint64(offset)
			m.Name = fmt.Sprintf("aggregations.secondly.expiration_benchmark.by_host.host%09d.requests", m.ID)
			catalog, err := s.encodeCatalog(m)
			if err != nil {
				t.Fatal(err)
			}
			for _, kv := range []struct{ k, v []byte }{{catalogKey(m.Name), catalog}, {revisionKey(m), revisionBytes(m)}, {chunkKey(m, 0, 0), encoded}} {
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
	if err := s.db.Flush(); err != nil {
		t.Fatal(err)
	}
	t.Logf("catalog names=%d added=%d fixture time=%s", first+uint64(count), count, time.Since(started))
}

func BenchmarkExpirationScaleDelete(b *testing.B) {
	dir := os.Getenv("EXPIRATION_SCALE_DIR")
	if dir == "" {
		b.Skip("set EXPIRATION_SCALE_DIR to a >=10M-name store")
	}
	s, err := Open(dir, Options{CacheSize: 32 << 20, MemTableSize: 16 << 20, SyncInterval: time.Hour, CompactionWorkers: 2})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	count, err := s.CatalogHighWatermark(context.Background())
	if err != nil || count < 10_000_000 {
		b.Fatalf("need >=10M-name store: %d %v", count, err)
	}
	const deletes = 10_000
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		left := deletes
		for left > 0 {
			page, err := s.ListPage(context.Background(), "", "", min(128, left))
			if err != nil || len(page) == 0 {
				b.Fatalf("page: %v %v", len(page), err)
			}
			outcomes, err := s.DeleteBatchIfUnchanged(context.Background(), page)
			if err != nil {
				b.Fatal(err)
			}
			for _, err := range outcomes {
				if err != nil {
					b.Fatal(err)
				}
			}
			left -= len(page)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N*deletes)/b.Elapsed().Seconds(), "deletes/s")
	b.ReportMetric(b.Elapsed().Seconds()/float64(b.N), "seconds/10000-deletes")
}

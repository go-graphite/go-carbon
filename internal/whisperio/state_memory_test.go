package whisperio

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// A manual retained-heap measurement using real continuation headers and tails.
func TestStateCacheMemory(t *testing.T) {
	if os.Getenv("GO_CARBON_STATE_MEMORY") == "" {
		t.Skip("set GO_CARBON_STATE_MEMORY=1")
	}
	const n = 65536
	ctx := context.Background()
	now := time.Unix(1790000000, 0)
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, TimeBuckets: true,
		Continuation: &ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}, Now: func() time.Time { return now },
		SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20, MetadataCacheSize: n * 8192})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("memory.service-%03d.namespace.host-%05d.requests.count", i%997, i)
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for _, name := range names {
		if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 60, Count: 20160}, {Step: 1800, Count: 35040}},
			AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
			t.Fatal(err)
		}
	}
	var input [128]store.Point
	base := now.Unix() / 60 * 60
	now = time.Unix(base+129*60, 0)
	for i, name := range names {
		for j := range input {
			input[j] = store.Point{Timestamp: base + int64(j)*60, Value: float64((i+j%32)%97) + .125}
		}
		if err := db.UpdateMany(ctx, name, input[:]); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)
	s := db.Stats().StateCache
	if s.Entries != n || s.Evictions != 0 {
		t.Fatalf("cache lost states: %+v", s)
	}
	t.Logf("MEMORY metrics=%d heap-B/metric=%.1f payload-B/metric=%.1f arena-B/metric=%.1f", n,
		float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/n, float64(s.Bytes)/n, float64(s.ArenaBytes)/n)
	runtime.KeepAlive(names)
	runtime.KeepAlive(db)
}

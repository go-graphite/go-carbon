package v2core

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func TestRollupWindowCacheCommitAndExpiry(t *testing.T) {
	r := newCoreRollups()
	_, definition := rollupTestPolicy(t, "cache.metric", "1s:1h,1m:1d", sealrollup.Sum, 0)
	m, err := r.metric(1, definition)
	if err != nil {
		t.Fatal(err)
	}
	// Seed entries through a reader whose committed empty history needs no I/O.
	m.meta.Expired = true
	var cache rollupWindowCache
	for _, start := range []int64{120, 180} {
		if _, err = cache.rangeSamples(&rollupHistoryReader{}, m, 0, start, start+60); err != nil {
			t.Fatal(err)
		}
	}
	m.meta.Expired = false
	record := sealrollup.Record{Delta: sealrollup.Delta{Archive: 0, Slot: 121, Order: 1, Sample: sealrollup.Sample{Time: 121, Bits: 1 << 63, Sequence: 1}}}
	cache.commit(m, []sealrollup.Record{record}, false)
	samples, err := cache.rangeSamples(nil, m, 0, 120, 180)
	if err != nil || len(samples) != 1 || samples[0].Bits != 1<<63 {
		t.Fatalf("cached replacement: %v %v", samples, err)
	}
	record.Sample = sealrollup.Sample{Time: 3721, Bits: math.Float64bits(math.Inf(1)), Sequence: 2}
	cache.commit(m, []sealrollup.Record{record}, false)
	samples, err = cache.rangeSamples(nil, m, 0, 120, 180)
	if err != nil || len(samples) != 0 {
		t.Fatalf("physical epoch did not mask old time: %v %v", samples, err)
	}
	cache.commit(m, nil, true)
	if cache.bytes != 0 || cache.lru.Len() != 0 || len(cache.byMetric) != 0 || len(cache.entries) != 0 {
		t.Fatal("expired cache retained state")
	}
}
func TestRollupWindowCacheBound(t *testing.T) {
	r := newCoreRollups()
	_, definition := rollupTestPolicy(t, "cache.metric", "1s:1h,1m:1d", sealrollup.Sum, 0)
	m, err := r.metric(1, definition)
	if err != nil {
		t.Fatal(err)
	}
	m.meta.Expired = true
	var cache rollupWindowCache
	for i := int64(0); i < 2000; i++ {
		if _, err = cache.rangeSamples(&rollupHistoryReader{}, m, 0, i*60, i*60+60); err != nil {
			t.Fatal(err)
		}
	}
	if cache.lru.Len() != rollupWindowCacheEntries || cache.bytes > rollupWindowCacheBytes {
		t.Fatal("cache bound exceeded")
	}
	// Growth on commit must also evict by retained capacity, rather than merely
	// respecting the entry count. A large source window may exceed the budget.
	newest := cache.lru.Front().Value.(*rollupWindow)
	newest.samples = make([]sealrollup.Sample, 70000)
	cache.commit(m, nil, false)
	if cache.bytes > rollupWindowCacheBytes || cache.entries[newest.key] != nil {
		t.Fatal("oversized cached window survived commit")
	}

}

func TestRollupColdReadFailureDoesNotPublish(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Shards = 1
	cfg.Producers = 1
	cfg.Rollups = true
	cfg.MaxRows = 1024
	root := t.TempDir()
	e, err := Open(root, cfg, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	_, definition := rollupTestPolicy(t, "failure.metric", "1s:1h,1m:1d", sealrollup.Sum, 0)
	const now = int64(1794700800)
	rows := make([]Row, 120)
	for i := range rows {
		rows[i] = Row{ID: 1, Time: now - 1800 + int64(i)*10, Bits: math.Float64bits(1), Seq: uint64(i + 1)}
	}
	if err = e.AppendBatches(0, []Batch{{ID: 1, Policy: definition, Order: 1, Clock: now, Points: rows}}, 1, true); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(root, "owner-000", "parts", "*.sst"))
	if err != nil || len(files) != 1 {
		t.Fatalf("parts %v %v", files, err)
	}
	path := files[0]
	if err = os.Rename(path, path+".held"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Rename(path+".held", path); err != nil {
			t.Error(err)
		}
	}()
	err = e.AppendBatches(0, []Batch{{ID: 1, Policy: definition, Order: 2, Clock: now, Points: []Row{{ID: 1, Time: now - 1700, Bits: math.Float64bits(2), Seq: 1}}}}, 2, true)
	if err == nil {
		t.Fatal("missing durable source was accepted")
	}
	if e.owners[0].rollups.metrics[1].meta.Frontier.Order != 1 {
		t.Fatal("failed source read advanced reducer frontier")
	}
}

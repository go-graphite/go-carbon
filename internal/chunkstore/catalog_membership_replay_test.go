package chunkstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// This optional diagnostic replays real names against a private disk image.
// Its explicit mixes measure metadata routing, not production ingestion or
// full read/write performance. Normal test runs have no external dependency.
func TestProductionCatalogMembershipReplay(t *testing.T) {
	runProductionCatalogReplay(t, false)
}

func TestProductionCatalogMissingFetchReplay(t *testing.T) {
	runProductionCatalogReplay(t, true)
}

func runProductionCatalogReplay(t *testing.T, fetchMissing bool) {
	t.Helper()
	root := os.Getenv("GO_CARBON_CATALOG_PROOF_DIR")
	if root == "" {
		t.Skip("set GO_CARBON_CATALOG_PROOF_DIR to a private replay image")
	}
	if !filepath.IsAbs(root) || filepath.Base(root) != "catalog-membership-stage-proof-20261023" {
		t.Fatal("require the private proof directory, never a serving database")
	}
	data, err := os.ReadFile(filepath.Join(root, "corpus.json"))
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct{ Rows []struct{ Metric string } }
	if err := json.Unmarshal(data, &corpus); err != nil || len(corpus.Rows) < 100 {
		t.Fatalf("invalid captured production corpus: %v", err)
	}
	// The metadata-only replay must never call a point codec. Embedding the
	// interface deliberately fails if a future change introduces such a call.
	metadataOnly := &struct{ ContinuationCodec }{}
	s, err := Open(filepath.Join(root, "database"), Options{ExactBlocks: true, Continuation: metadataOnly, IncrementalSource: filepath.Join(root, "empty-source"), CacheSize: 128 << 20, MemTableSize: 16 << 20, CompactionWorkers: 1, MaxOpenFiles: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestStore(t, s)
	select {
	case <-s.catalogMembership.done:
	case <-time.After(3 * time.Minute):
		t.Fatal("catalog membership scan did not finish")
	}
	if !s.catalogMembership.ready.Load() {
		t.Fatal("catalog membership scan failed")
	}
	ctx := context.Background()
	type lookup struct {
		name string
		id   uint64
	}
	var absent, owned []lookup
	for _, row := range corpus.Rows {
		_, err := s.Metadata(ctx, row.Metric)
		if errors.Is(err, ErrNotFound) {
			absent = append(absent, lookup{name: row.Metric})
		} else if err != nil {
			t.Fatal(err)
		}
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("m/"), UpperBound: []byte("m0")})
	if err != nil {
		t.Fatal(err)
	}
	for valid := iter.First(); valid && len(owned) < 4096; valid = iter.Next() {
		name := string(iter.Key()[2:])
		m, err := s.Metadata(ctx, name)
		if err == nil {
			owned = append(owned, lookup{name: name, id: m.ID})
		} else if !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
	}
	if err := errors.Join(iter.Error(), iter.Close()); err != nil {
		t.Fatal(err)
	}
	if len(absent) < 100 || len(owned) < 100 {
		t.Fatalf("insufficient real-name cohorts: absent=%d owned=%d", len(absent), len(owned))
	}
	mixed := make([]lookup, 0, 2*len(absent))
	for i, row := range absent {
		mixed = append(mixed, row, owned[i%len(owned)])
	}
	type result struct {
		Cohort                                                         string
		Pair                                                           int
		Filter                                                         bool
		Names, Operations                                              int
		CPUSeconds, WallSeconds, BytesPerOperation, AllocsPerOperation float64
	}
	var results []result
	const operations, workers = 65536, 96
	cpu := func() float64 {
		var usage syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
			t.Fatal(err)
		}
		return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
	}
	cohorts := []struct {
		name string
		rows []lookup
	}{{"original-only", absent}, {"shared-only", owned}, {"half-each", mixed}}
	if fetchMissing {
		cohorts = cohorts[:1]
	}
	matches := func(row lookup) bool {
		if fetchMissing {
			_, err := s.Fetch(ctx, row.name, 0, 1)
			return row.id == 0 && errors.Is(err, ErrNotFound)
		}
		m, err := s.Metadata(ctx, row.name)
		return row.id == 0 && errors.Is(err, ErrNotFound) || row.id != 0 && err == nil && m.ID == row.id
	}
	for _, cohort := range cohorts {
		for pair := range 3 {
			order := []bool{false, true}
			if pair%2 != 0 {
				order = []bool{true, false}
			}
			for _, enabled := range order {
				s.catalogMembership.ready.Store(enabled)
				for _, row := range cohort.rows {
					_ = matches(row)
				}
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				startCPU, start := cpu(), time.Now()
				var wg sync.WaitGroup
				var failures atomic.Int64
				for worker := range workers {
					wg.Go(func() {
						for i := worker; i < operations; i += workers {
							row := cohort.rows[(i*7919)%len(cohort.rows)]
							if !matches(row) {
								failures.Add(1)
							}
						}
					})
				}
				wg.Wait()
				elapsed, used := time.Since(start).Seconds(), cpu()-startCPU
				runtime.ReadMemStats(&after)
				if failures.Load() != 0 {
					t.Fatalf("metadata replies differed: %d", failures.Load())
				}
				results = append(results, result{cohort.name, pair, enabled, len(cohort.rows), operations, used, elapsed, float64(after.TotalAlloc-before.TotalAlloc) / operations, float64(after.Mallocs-before.Mallocs) / operations})
			}
		}
	}
	s.catalogMembership.ready.Store(true)
	output, err := json.Marshal(map[string]any{"passed": true, "scope": "Catalog component only; captured real names with explicit mixes, not a matched production read/write replay", "fetch_missing": fetchMissing, "filter_bytes": s.Stats().CatalogFilterBytes, "cache_shards": s.Stats().CacheShards, "cache_budget_bytes": 128 << 20, "execution_threads": runtime.GOMAXPROCS(0), "results": results})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("CATALOG_PROOF_JSON=%s\n", output)
}

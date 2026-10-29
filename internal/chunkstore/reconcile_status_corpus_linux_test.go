package chunkstore_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// Measure only verification of unchanged images. Source decoding and seeding
// are outside each measurement; this is not a steady-state traffic replay.
func TestNativeUnchangedVerificationCorpus(t *testing.T) {
	source, manifest, output := os.Getenv("GRAPHITE_VERIFY_SOURCE"), os.Getenv("GRAPHITE_VERIFY_MANIFEST"), os.Getenv("GRAPHITE_VERIFY_OUTPUT")
	if source == "" || manifest == "" || output == "" {
		t.Skip("requires retained private byte-weighted production corpus")
	}
	if !strings.HasSuffix(filepath.Clean(source), "/full-baseline/cwhisper-copy") || !strings.Contains(filepath.Clean(output), "/production-replay-lab-v1/") {
		t.Fatal("requires private corpus paths")
	}
	var corpus struct {
		Rows []struct{ Metric, Path string }
	}
	raw, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &corpus); err != nil || len(corpus.Rows) != 3597 {
		t.Fatal("unexpected weighted cohort", err)
	}
	if err = os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(filepath.Join(output, "exact"), store.Options{ExactBlocks: true, CacheSize: 256 << 20, MemTableSize: 64 << 20, SyncInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	type measurement struct {
		CPUSeconds     float64
		ElapsedSeconds float64
		AllocatedBytes uint64
		Allocations    uint64
		Metrics        int
		Points         uint64
	}
	results := map[string]*measurement{"before": {}, "after": {}}
	ctx := context.Background()
	for i, row := range corpus.Rows {
		want, err := whisperio.ReadSnapshotIn(ctx, row.Metric, filepath.Join(source, row.Path), output)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.Replace(ctx, want); err != nil {
			t.Fatal(err)
		}
		order := []string{"before", "after"}
		if i%2 != 0 {
			order[0], order[1] = order[1], order[0]
		}
		for _, mode := range order {
			var cpuBefore, cpuAfter syscall.Rusage
			var memBefore, memAfter runtime.MemStats
			runtime.ReadMemStats(&memBefore)
			if err = syscall.Getrusage(syscall.RUSAGE_SELF, &cpuBefore); err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			if mode == "before" {
				_, err = db.ReconcileSnapshot(ctx, want)
				if err == nil {
					var actual store.Snapshot
					actual, err = db.Snapshot(ctx, row.Metric)
					if err == nil {
						err = store.EqualSnapshots(want, actual)
					}
				}
			} else {
				var unchanged bool
				_, unchanged, err = db.ReconcileSnapshotStatus(ctx, want)
				if err == nil && !unchanged {
					t.Fatal("canonical production image was not certified")
				}
			}
			elapsed := time.Since(started).Seconds()
			if err != nil {
				t.Fatal(err)
			}
			if err = syscall.Getrusage(syscall.RUSAGE_SELF, &cpuAfter); err != nil {
				t.Fatal(err)
			}
			runtime.ReadMemStats(&memAfter)
			m := results[mode]
			m.ElapsedSeconds += elapsed
			m.CPUSeconds += float64(cpuAfter.Utime.Nano()+cpuAfter.Stime.Nano()-cpuBefore.Utime.Nano()-cpuBefore.Stime.Nano()) / 1e9
			m.AllocatedBytes += memAfter.TotalAlloc - memBefore.TotalAlloc
			m.Allocations += memAfter.Mallocs - memBefore.Mallocs
			m.Metrics++
			for _, a := range want.Archives {
				m.Points += uint64(len(a.Points))
			}
		}
	}
	if err = db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	result := map[string]any{"passed": true, "results": results, "scope": "Unchanged-image verification only, same retained byte-weighted production histories, exact Snapshot oracle for every source, alternating before/after order; excludes source decoding, seeding, steady-state traffic and compaction cost."}
	raw, err = json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(output, "RESULT.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

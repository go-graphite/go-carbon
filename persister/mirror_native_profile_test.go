package persister

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/helper"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
)

type profileMirrorStore struct {
	*store.Store
	replacements atomic.Uint64
}

func (s *profileMirrorStore) Replace(ctx context.Context, snapshot store.Snapshot) (store.Metadata, error) {
	s.replacements.Add(1)
	return s.Store.Replace(ctx, snapshot)
}

// This diagnostic is explicitly enabled and operates only on private copies.
// Its CPU profiles distinguish source-window, store-update and clock-repair
// work; its timing is not a full-node or steady-state qualification.
func TestNativeProductionMirrorProfile(t *testing.T) {
	source, manifest, output := os.Getenv("GRAPHITE_MIRROR_CORPUS"), os.Getenv("GRAPHITE_MIRROR_MANIFEST"), os.Getenv("GRAPHITE_MIRROR_OUTPUT")
	if source == "" || manifest == "" || output == "" {
		t.Skip("requires retained production corpus, manifest, and owned output")
	}
	var corpus struct {
		Rows []struct{ Metric, Path string }
	}
	raw, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	if len(corpus.Rows) == 0 {
		t.Fatal("empty corpus")
	}
	if err = os.MkdirAll(output, 0700); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	oldNow := whisper.Now
	defer func() { whisper.Now = oldNow }()
	ctx := context.Background()
	const rounds = 20
	workers := 0
	if raw := os.Getenv("GRAPHITE_MIRROR_GROUPED_WORKERS"); raw != "" {
		workers, err = strconv.Atoi(raw)
		if err != nil || workers < 1 || workers > 128 {
			t.Fatal("invalid grouped worker count")
		}
	}
	reconcileWorkers := 0
	if raw := os.Getenv("GRAPHITE_MIRROR_RECONCILE_WORKERS"); raw != "" {
		reconcileWorkers, err = strconv.Atoi(raw)
		if err != nil || reconcileWorkers < 1 || reconcileWorkers > 32 || workers == 0 {
			t.Fatal("invalid concurrent reconciliation worker count")
		}
	}
	var summaries []map[string]any
	var sourceBytes int64
	for _, row := range corpus.Rows {
		info, e := os.Stat(filepath.Join(source, row.Path))
		if e != nil {
			t.Fatal(e)
		}
		sourceBytes += info.Size()
	}
	for _, mode := range []string{"files-fixed", "mirror-fixed", "mirror-real"} {
		dir := filepath.Join(output, mode)
		if err = os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		original := filepath.Join(dir, "original")
		cache := &fakeCache{}
		p := newOOOTestPersister(t, original, cache)
		p.EnableOutOfOrder(100000, 1<<30)
		p.outOfOrder.ticker.Stop()
		p.outOfOrder.ticker = nil
		var db *profileMirrorStore
		if mode != "files-fixed" {
			opened, e := store.Open(filepath.Join(dir, "exact"), store.Options{ExactBlocks: true, CacheSize: 64 << 20, MemTableSize: 16 << 20, SyncInterval: time.Second, Now: func() time.Time {
				if mode == "mirror-real" {
					return time.Now()
				}
				return now
			}})
			if e != nil {
				t.Fatal(e)
			}
			db = &profileMirrorStore{Store: opened}
			p.SetMetricStore(db)
			p.SetMirrorFiles(true)
			p.SetMirrorState(&MirrorState{ScratchDir: dir})
		}
		type record struct {
			metric string
			path   string
			step   int64
			values []float64
			batch  int
		}
		records := make([]record, 0, len(corpus.Rows))
		alias := 0
		for _, row := range corpus.Rows {
			if !filepath.IsLocal(row.Path) {
				t.Fatal("invalid source path")
			}
			metric, path := row.Metric, row.Path
			if workers != 0 {
				for {
					metric = fmt.Sprintf("queued.production.metric.%d", alias)
					alias++
					if helper.HashString(metric)&1023 == 0 {
						break
					}
				}
				path = filepath.FromSlash(strings.ReplaceAll(metric, ".", "/") + ".wsp")
			}
			from, to := filepath.Join(source, row.Path), filepath.Join(original, path)
			if err = os.MkdirAll(filepath.Dir(to), 0700); err != nil {
				t.Fatal(err)
			}
			for _, suffix := range []string{"", ".ooo"} {
				b, e := os.ReadFile(from + suffix)
				if os.IsNotExist(e) && suffix != "" {
					continue
				}
				if e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(to+suffix, b, 0600); e != nil {
					t.Fatal(e)
				}
			}
			snapshot, e := whisperio.ReadSnapshotIn(ctx, metric, to, dir)
			if e != nil {
				t.Fatal(e)
			}
			if db != nil {
				if _, e = db.CreateFromSnapshot(ctx, snapshot); e != nil {
					t.Fatal(e)
				}
			}
			var values []float64
			for _, a := range snapshot.Archives {
				for i := len(a.Points) - 1; i >= 0 && len(values) < 8; i-- {
					values = append(values, a.Points[i].Value)
				}
			}
			if len(values) == 0 {
				t.Fatalf("empty metric %s", row.Metric)
			}
			step := int64(snapshot.Metadata.Retentions[0].Step)
			records = append(records, record{metric, path, step, values, min(128, max(1, 130/int(step)))})
		}
		if mode == "mirror-real" {
			whisper.Now = oldNow
		} else {
			whisper.Now = func() time.Time { return now }
		}
		expectedPoints := 0
		for _, rec := range records {
			expectedPoints += rounds * rec.batch
		}
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		prof, e := os.Create(filepath.Join(dir, "cpu.pprof"))
		if e != nil {
			t.Fatal(e)
		}
		if e = pprof.StartCPUProfile(prof); e != nil {
			t.Fatal(e)
		}
		began := time.Now()
		var repairs sync.WaitGroup
		repairErrors := make(chan error, len(records))
		backgroundReconciliations := 0
		if db != nil && reconcileWorkers != 0 {
			tasks := make(chan record, len(records))
			for _, rec := range records {
				tasks <- rec
			}
			close(tasks)
			backgroundReconciliations = len(records)
			for range reconcileWorkers {
				repairs.Go(func() {
					for rec := range tasks {
						if _, _, err := p.ReconcileMirrorMetric(ctx, rec.metric, 0); err != nil {
							repairErrors <- err
						}
					}
				})
			}
		}
		for round := 0; round < rounds; round++ {
			var group sync.WaitGroup
			var jobs chan record
			if workers != 0 {
				jobs = make(chan record)
				for range workers {
					group.Go(func() {
						for rec := range jobs {
							p.store(rec.metric)
						}
					})
				}
			}
			for _, rec := range records {
				// Same real values and monotonically increasing timestamp batches. Include
				// each source policy; steps outside a short primary retention route as usual.
				base := now.Unix()/rec.step*rec.step - int64(rounds*rec.batch)*rec.step - 130/rec.step*rec.step
				for i := 0; i < rec.batch; i++ {
					cache.add(rec.metric, base+int64(round*rec.batch+i)*rec.step, rec.values[i%len(rec.values)])
				}
				if workers == 0 {
					p.store(rec.metric)
				} else {
					jobs <- rec
				}
			}
			if jobs != nil {
				close(jobs)
				group.Wait()
			}
		}
		repairs.Wait()
		close(repairErrors)
		for err := range repairErrors {
			if err != nil {
				t.Fatal(err)
			}
		}
		elapsed := time.Since(began)
		pprof.StopCPUProfile()
		if e = prof.Close(); e != nil {
			t.Fatal(e)
		}
		runtime.ReadMemStats(&after)
		if cache.confirmedPoints() != expectedPoints {
			t.Fatalf("unconfirmed replay: %s %d", mode, cache.confirmedPoints())
		}
		replacements := uint64(0)
		if db != nil {
			replacements = db.replacements.Load()
			// Use manifest paths rather than inferring filesystem names from metrics.
			for _, row := range records {
				want, e := whisperio.ReadSnapshotIn(ctx, row.metric, filepath.Join(original, row.path), dir)
				if e != nil {
					t.Fatal(e)
				}
				got, e := db.Snapshot(ctx, row.metric)
				if e != nil {
					t.Fatal(e)
				}
				if e = store.EqualSnapshots(want, got); e != nil {
					t.Fatalf("%s oracle %s: %v", mode, row.metric, e)
				}
			}
			if e = db.Compact(); e != nil {
				t.Fatal(e)
			}
			for _, row := range records {
				want, e := whisperio.ReadSnapshotIn(ctx, row.metric, filepath.Join(original, row.path), dir)
				if e != nil {
					t.Fatal(e)
				}
				got, e := db.Snapshot(ctx, row.metric)
				if e != nil {
					t.Fatal(e)
				}
				if e = store.EqualSnapshots(want, got); e != nil {
					t.Fatalf("post-compaction %s: %v", row.metric, e)
				}
			}
			if e = db.Close(); e != nil {
				t.Fatal(e)
			}
		}
		summaries = append(summaries, map[string]any{"mode": mode, "workers": workers, "same_cache_shard": workers != 0, "reconcile_workers": reconcileWorkers, "background_reconciliations": backgroundReconciliations, "seconds": elapsed.Seconds(), "metrics": len(records), "batches": rounds * len(records), "points": expectedPoints, "corpus_bytes": sourceBytes, "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "allocations": after.Mallocs - before.Mallocs, "full_replacements": replacements, "oracle_and_compaction_passed": db != nil})
		encoded, _ := json.Marshal(summaries)
		if e = os.WriteFile(filepath.Join(output, "RESULT.json"), encoded, 0600); e != nil {
			t.Fatal(e)
		}
	}
}

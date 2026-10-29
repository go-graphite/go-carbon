package persister

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	carboncache "github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/helper"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
)

// Component proof on the unchanged byte-weighted production histories. It does
// not claim full-application steady throughput or replay the complete read mix.
func TestNativeByteWeightedReconciliation(t *testing.T) {
	dir, source, manifest, output := os.Getenv("GRAPHITE_RECONCILE_DB"), os.Getenv("GRAPHITE_RECONCILE_SOURCE"), os.Getenv("GRAPHITE_RECONCILE_MANIFEST"), os.Getenv("GRAPHITE_RECONCILE_OUTPUT")
	if dir == "" || source == "" || manifest == "" || output == "" {
		t.Skip("requires private byte-weighted corpus and database")
	}
	if !strings.Contains(filepath.Clean(dir), "/production-replay-lab-v1/") || !strings.Contains(filepath.Clean(output), "/production-replay-lab-v1/") || filepath.Clean(source) != "/var/lib/carbon/storage-exact-canary-20261019/full-baseline/cwhisper-copy" {
		t.Fatal("requires private corpus paths")
	}
	var corpus struct {
		Rows            []fullMirrorRow
		OriginalIDLimit uint64
	}
	b, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(b, &corpus); err != nil {
		t.Fatal(err)
	}
	if len(corpus.Rows) != 3597 || corpus.OriginalIDLimit == 0 {
		t.Fatal("unexpected weighted cohort")
	}
	if err = os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	original := filepath.Join(output, "original")
	for _, row := range corpus.Rows {
		for _, suffix := range []string{"", ".ooo"} {
			err = cloneFullMirrorFile(filepath.Join(source, row.Path)+suffix, filepath.Join(original, row.Path)+suffix)
			if err != nil && !(suffix != "" && os.IsNotExist(err)) {
				t.Fatal(err)
			}
		}
	}
	oldProcs := runtime.GOMAXPROCS(192)
	defer runtime.GOMAXPROCS(oldProcs)
	oldNow := whisper.Now
	whisper.Now = func() time.Time { return time.Unix(1792532411, 0) }
	defer func() { whisper.Now = oldNow }()
	opts := store.Options{ExactBlocks: true, CacheSize: 256 << 20, MemTableSize: 64 << 20, SyncInterval: time.Second, CompactionWorkers: 8, L0CompactionConcurrency: 2, MaxOpenFiles: 16384, Now: whisper.Now}
	db, err := store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()
	for _, row := range corpus.Rows {
		m, e := db.Metadata(ctx, row.Metric)
		if e != nil || m.ID != row.BaselineID || m.ID > corpus.OriginalIDLimit || m.Generation != 1 || m.Revision != 1 {
			t.Fatal("baseline differs from corpus")
		}
	}
	c := carboncache.New()
	var confirmed atomic.Uint64
	p := newOOOTestPersister(t, original, &fakeCache{})
	p.pop = c.PopNotConfirmed
	p.popConfirm = c.Pop
	p.confirm = func(v *points.Points) { c.Confirm(v); confirmed.Add(uint64(len(v.Data))) }
	p.SetRequeue(c.Requeue)
	p.SetFLock(true)
	p.outOfOrder.ticker.Stop()
	p.EnableOutOfOrder(500, 1<<30)
	p.outOfOrder.ticker = helper.NewHardThrottleTicker(500)
	defer p.outOfOrder.ticker.Stop()
	reconciler := NewWhisper(original, p.schemas, p.aggregation, nil, nil, nil, nil)
	reconciler.SetMetricStore(db)
	reconciler.SetMirrorFiles(true)
	reconciler.SetMirrorState(&MirrorState{ScratchDir: output})
	initial := db.Stats()
	var before, after syscall.Rusage
	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)
	if err = syscall.Getrusage(syscall.RUSAGE_SELF, &before); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	jobs := make(chan int)
	failures := make(chan error, 96)
	var group sync.WaitGroup
	for range 96 {
		group.Go(func() {
			for index := range jobs {
				row := corpus.Rows[index]
				if len(row.Points) == 0 {
					continue
				}
				value := math.Float64frombits(row.Points[0].Bits)
				c.Add(&points.Points{Metric: row.Metric, Data: []points.Point{{Timestamp: 1792532409, Value: value}, {Timestamp: 1792532406, Value: value}, {Timestamp: 1792532408, Value: value}}})
				p.store(row.Metric)
				_, _, e := reconciler.ReconcileMirrorMetric(ctx, row.Metric, 0)
				if e != nil {
					select {
					case failures <- e:
					default:
					}
				}
			}
		})
	}
	for i := range corpus.Rows {
		jobs <- i
	}
	close(jobs)
	group.Wait()
	close(failures)
	for e := range failures {
		t.Error(e)
	}
	if t.Failed() {
		t.FailNow()
	}
	if confirmed.Load() != uint64(len(corpus.Rows)*3) {
		t.Fatal("file input was not confirmed")
	}
	if err = db.Flush(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(120 * time.Second)
	for db.Stats().CompactionsInProgress != 0 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if err = syscall.Getrusage(syscall.RUSAGE_SELF, &after); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&memAfter)
	final := db.Stats()
	elapsed := time.Since(began).Seconds()
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	var exactPoints uint64
	for _, row := range corpus.Rows {
		want, e := whisperio.ReadSnapshotIn(ctx, row.Metric, filepath.Join(original, row.Path), output)
		if e != nil {
			t.Fatal(e)
		}
		got, e := db.Snapshot(ctx, row.Metric)
		if e != nil {
			t.Fatal(e)
		}
		if e = store.EqualSnapshots(want, got); e != nil {
			t.Fatal(e)
		}
		for _, archive := range want.Archives {
			exactPoints += uint64(len(archive.Points))
		}
	}
	fullMirrorWriteJSON(t, filepath.Join(output, "RESULT.json"), map[string]any{"passed": true, "metrics": len(corpus.Rows), "confirmed_points": confirmed.Load(), "exact_history_points_after_reopen": exactPoints, "elapsed_seconds_including_compaction": elapsed, "cpu_seconds": pressureCPU(after) - pressureCPU(before), "allocated_bytes": memAfter.TotalAlloc - memBefore.TotalAlloc, "peak_rss_bytes_linux": after.Maxrss * 1024, "initial_stats": initial, "final_stats": final, "scope": "Matched component replay on unchanged byte-weighted corpus and full DB. Each private original receives the same three translated timestamps and captured float bits, then coherent reconciliation with full source verification; all histories checked after natural compaction and reopen. Fixed source clock; not complete production traffic or steady-state capacity."})
}

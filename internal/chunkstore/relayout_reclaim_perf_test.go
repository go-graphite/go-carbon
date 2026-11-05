//go:build darwin || linux

package chunkstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

// This opaque codec exercises real planning reads, per-key deletion, WAL
// publication and ingestion, without conflating codec compression with reclaim.
type reclaimPerfCodec struct {
	relayoutCodec
	value []byte
	skew  bool
}

func (c reclaimPerfCodec) count(m Metadata) int {
	if c.skew && m.ID <= 205 {
		return 128
	}
	return 4
}

func (c reclaimPerfCodec) Create(_ context.Context, io ContinuationIO, m Metadata, _ func() time.Time) error {
	if err := io.Set([]byte("b"), []byte("v6")); err != nil {
		return err
	}
	if err := io.Set([]byte("live"), []byte("tail")); err != nil {
		return err
	}
	for i := 0; i < c.count(m); i++ {
		if err := io.Set(spaceGroupKey(i), c.value); err != nil {
			return err
		}
	}
	return nil
}

func (c reclaimPerfCodec) RelayoutPlan(_ context.Context, io ContinuationIO) (RelayoutPlan, error) {
	old, err := io.Get([]byte("b"))
	if err != nil || string(old) != "v6" {
		return RelayoutPlan{}, err
	}
	b := RelayoutBlock{Descriptor: []byte("b"), Old: old, New: []byte("v7")}
	p := RelayoutPlan{}
	for i := 0; ; i++ {
		k := spaceGroupKey(i)
		v, err := io.Get(k)
		if errors.Is(err, ErrNotFound) {
			break
		}
		if err != nil {
			return p, err
		}
		b.Removed = append(b.Removed, k)
		b.Copies = append(b.Copies, RelayoutKV{Key: ContinuationBucketKey(0, k), Value: v})
		p.Bytes += len(k) + len(v)
	}
	p.Blocks = []RelayoutBlock{b}
	return p, nil
}

// Opt-in local measurements; no timing assertions. Each subtest owns a fresh
// database, with a 65 MiB reclaim budget and multiple 8 MiB L6 files.
func TestRelayoutReclaimPerformance(t *testing.T) {
	if os.Getenv("RELAYOUT_RECLAIM_PERF") == "" {
		t.Skip("opt-in on-disk measurement")
	}
	for _, shape := range []string{"skew", "small"} {
		for _, mode := range []string{"compact-false", "compact-true", "K1", "K4", "K8", "K4-overlap"} {
			t.Run(shape+"/"+mode, func(t *testing.T) {
				dir := t.TempDir()
				usage, err := vfs.Default.GetDiskUsage(dir)
				if err != nil || usage.AvailBytes < 25_000_000_000 {
					t.Fatalf("disk floor: %v", err)
				}
				value := make([]byte, 9<<10)
				r := rand.New(rand.NewPCG(1, 2))
				for i := range value {
					value[i] = byte(r.Uint32())
				}
				codec := reclaimPerfCodec{value: value, skew: shape == "skew"}
				workers := 1
				if mode == "K4" || mode == "K4-overlap" {
					workers = 4
				}
				if mode == "K8" {
					workers = 8
				}
				limit := uint64(65 << 20)
				if mode == "compact-false" || mode == "compact-true" {
					limit = 1 << 40
				}
				s, _ := openTestStore(t, dir, Options{fs: noSyncFS{FS: vfs.Default}, ExactBlocks: true, TimeBuckets: true, Continuation: codec,
					RelayoutWorkers: workers, RelayoutReclaimBytes: limit, RelayoutMinFreeBytes: 1, RelayoutMaxCompactionDebt: 1 << 40,
					TargetFileSize: 8 << 20, L0TargetFileSize: 128 << 10, MemTableSize: 4 << 20, CacheSize: 8 << 20, CompactionWorkers: 4, SyncInterval: time.Hour})
				defer closeTestStore(t, s)
				metrics := 4096
				if codec.skew {
					metrics = 2048
				}
				metas := make([]Metadata, metrics)
				for i := range metas {
					metas[i] = createTestMetric(t, s, fmt.Sprintf("perf.%05d", i))
				}
				if err := s.db.Compact([]byte{0}, []byte{255}, false); err != nil {
					t.Fatal(err)
				}
				for deadline := time.Now().Add(time.Minute); ; {
					m := s.db.Metrics()
					if m.Table.ObsoleteCount == 0 && m.Table.ZombieCount == 0 && m.Compact.NumInProgress == 0 {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("fixture did not settle")
					}
					time.Sleep(time.Millisecond)
				}
				tables, err := s.db.SSTables()
				if err != nil {
					t.Fatal(err)
				}
				if len(tables[6]) < 4 {
					t.Fatalf("need several L6 files: %d", len(tables[6]))
				}
				if mode == "compact-false" || mode == "compact-true" {
					for lo := 0; lo < len(metas); lo += 32 {
						var items []relayoutItem
						for _, m := range metas[lo:min(lo+32, len(metas))] {
							plan, err := codec.RelayoutPlan(context.Background(), s.newContinuationIO(context.Background(), s.db, nil, m))
							if err != nil {
								t.Fatal(err)
							}
							items = append(items, relayoutItem{m: m, plan: plan})
						}
						if err := s.relayoutGroup(context.Background(), items); err != nil {
							t.Fatal(err)
						}
					}
				}
				ctx := context.Background()
				_, spans, _, err := s.readRelayoutReclaims(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if raw := os.Getenv("RELAYOUT_PERF_COMPACTION_RATE"); raw != "" {
					rate, err := strconv.ParseInt(raw, 10, 64)
					if err != nil {
						t.Fatal(err)
					}
					if err := s.SetCompactionRate(rate); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "K4-overlap" {
					// Explicit copy/reclaim pipeline at a fixed 80 MiB/s copy
					// ceiling, while the same aggregate output cap applies.
					if err := s.SetRelayoutRate(80 << 20); err != nil {
						t.Fatal(err)
					}
				}
				before := s.db.Metrics()
				ioBefore := s.fsStats.Stats()
				peak := before.DiskSpaceUsage()
				var peakPending uint64
				var overlapSamples, lastCopied uint64
				peakL0 := before.Levels[0].NumFiles
				stop, stopped := make(chan struct{}), make(chan struct{})
				go func() {
					defer close(stopped)
					ticker := time.NewTicker(5 * time.Millisecond)
					defer ticker.Stop()
					for {
						select {
						case <-stop:
							return
						case <-ticker.C:
							m, st := s.db.Metrics(), s.RelayoutStats()
							peak = max(peak, m.DiskSpaceUsage())
							peakL0 = max(peakL0, m.Levels[0].NumFiles)
							peakPending = max(peakPending, st.UnreclaimedBytes)
							if m.Compact.NumInProgress > 0 && st.Bytes > lastCopied {
								overlapSamples++
							}
							lastCopied = st.Bytes
						}
					}
				}()
				var once sync.Once
				stopSample := func() { once.Do(func() { close(stop); <-stopped }) }
				defer stopSample()
				var prof *os.File
				if path := os.Getenv("RELAYOUT_PERF_PROFILE"); path != "" {
					prof, err = os.Create(path)
					if err != nil {
						t.Fatal(err)
					}
					if err = pprof.StartCPUProfile(prof); err != nil {
						t.Fatal(err)
					}
				}
				runtime.GC()
				var memBefore, memAfter runtime.MemStats
				runtime.ReadMemStats(&memBefore)
				cpu, start := spaceCPU(), time.Now()
				if mode == "compact-false" || mode == "compact-true" {
					for _, kind := range []byte{'u', 't'} {
						if span := spans[kind]; span != nil {
							if err = s.db.Compact(span.start, append(append([]byte(nil), span.end...), 0), mode == "compact-true"); err != nil {
								t.Fatal(err)
							}
						}
					}
				} else if err = s.RelayoutContinuation(ctx, 0); err != nil {
					t.Fatal(err)
				}
				wall, cpuSeconds := time.Since(start).Seconds(), (spaceCPU() - cpu).Seconds()
				runtime.ReadMemStats(&memAfter)
				if prof != nil {
					pprof.StopCPUProfile()
					_ = prof.Close()
				}
				stopSample()
				after := s.db.Metrics()
				ioAfter := s.fsStats.Stats()
				var writes, reads, ingested, flushed uint64
				for i, l := range after.Levels {
					writes += l.BytesCompacted - before.Levels[i].BytesCompacted
					reads += l.BytesRead - before.Levels[i].BytesRead
					ingested += l.BytesIngested - before.Levels[i].BytesIngested
					flushed += l.BytesFlushed - before.Levels[i].BytesFlushed
				}
				st := s.RelayoutStats()
				out := map[string]any{"shape": shape, "mode": mode, "wall_s": wall, "cpu_s": cpuSeconds, "copied": st.Bytes, "metrics": st.Metrics, "reclaims": st.Reclaims, "L6_files": len(tables[6]), "compact_write": writes, "compact_read": reads, "ingested": ingested, "flushed": flushed, "table_write": ioAfter.Bytes[FSWrite][FSTable] - ioBefore.Bytes[FSWrite][FSTable], "peak_growth": peak - before.DiskSpaceUsage(), "peak_pending": peakPending, "alloc_bytes": memAfter.TotalAlloc - memBefore.TotalAlloc, "allocs": memAfter.Mallocs - memBefore.Mallocs, "flush_count": after.Flush.Count - before.Flush.Count, "L0_before": before.Levels[0].NumFiles, "L0_after": after.Levels[0].NumFiles, "L0_peak": peakL0, "overlap_samples": overlapSamples}
				encoded, _ := json.Marshal(out)
				t.Log("PERF " + string(encoded))
				if mode == "K4-overlap" && overlapSamples == 0 {
					t.Fatal("no copy publication overlapped compaction")
				}
				if peakPending > 2*limit {
					t.Fatalf("more than two batches pending: %d > %d", peakPending, 2*limit)
				}
			})
		}
	}
}

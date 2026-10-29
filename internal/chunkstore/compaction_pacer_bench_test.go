package chunkstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// GO_CARBON_BENCH_COMPACTION_PACER=1 runs a short real-Pebble A/B. It models
// dense's repeated hot block rewrites and occasional sparse block writes using
// fixed, incompressible blocks, without CWhisper encoding or production data.
// The memory filesystem isolates write shape from host sync latency. Each
// case has the same 3.2 s input schedule and 0.8 s drain window; filesystem
// table-byte deltas include in-flight compactions, unlike completion counters.
func TestCompactionPacerWriteShape(t *testing.T) {
	if os.Getenv("GO_CARBON_BENCH_COMPACTION_PACER") != "1" {
		t.Skip("set GO_CARBON_BENCH_COMPACTION_PACER=1")
	}
	const (
		metrics = 4096
		hot     = 512
		rounds  = 32
		period  = 100 * time.Millisecond
		window  = 4 * time.Second
		sample  = 50 * time.Millisecond
	)
	keys, values := make([][]byte, metrics), make([][]byte, metrics)
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range metrics {
		keys[i] = fmt.Appendf(nil, "p%08d.block", i)
		values[i] = make([]byte, 1024)
		for j := 8; j < len(values[i]); j += 8 {
			binary.LittleEndian.PutUint64(values[i][j:], rng.Uint64())
		}
	}
	type result struct {
		compaction, tables, peak, cv, debt, endDebt, wait float64
		l0, endL0                                         int64
		stalls                                            uint64
		stallSeconds                                      float64
	}
	run := func(t *testing.T, memtable uint64, threshold int, rate int64) result {
		db, err := Open(t.TempDir(), Options{fs: vfs.NewMem(), ExactBlocks: true, MemTableSize: memtable, CacheSize: 8 << 20,
			LBaseMaxBytes: 8 << 20, TargetFileSize: 1 << 20, L0CompactionThreshold: threshold,
			L0StopWritesThreshold: 32, CompactionWorkers: 4, L0CompactionConcurrency: 2})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		write := func(ids []int, generation uint64) {
			b := db.db.NewBatch()
			defer b.Close()
			for _, i := range ids {
				value := bytes.Clone(values[i])
				binary.LittleEndian.PutUint64(value, generation)
				if err := b.Set(keys[i], value, nil); err != nil {
					t.Fatal(err)
				}
			}
			if err := b.Commit(pebble.NoSync); err != nil {
				t.Fatal(err)
			}
		}
		all := make([]int, metrics)
		for i := range all {
			all[i] = i
		}
		write(all, 0)
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := db.Compact(); err != nil {
			t.Fatal(err)
		}
		if err := db.SetCompactionRate(rate); err != nil {
			t.Fatal(err)
		}
		before := db.Stats()
		start := time.Now()
		var wg sync.WaitGroup
		var rates []float64
		var out result
		wg.Go(func() {
			lastBytes := before.FS.Bytes[FSWrite][FSTable]
			lastTime := start
			for tick := 1; tick <= int(window/sample); tick++ {
				time.Sleep(time.Until(start.Add(time.Duration(tick) * sample)))
				st := db.Stats()
				now := time.Now()
				count := st.FS.Bytes[FSWrite][FSTable]
				rates = append(rates, float64(count-lastBytes)/now.Sub(lastTime).Seconds())
				lastBytes, lastTime = count, now
				out.debt = max(out.debt, float64(st.CompactionDebt))
				out.l0 = max(out.l0, st.L0Sublevels)
			}
		})
		latest := make([]uint64, metrics)
		for round := range rounds {
			time.Sleep(time.Until(start.Add(time.Duration(round) * period)))
			// Spread the hot keys across the keyspace, making each flush
			// overlap cold history as it does on the shared dense store.
			ids := make([]int, hot)
			for i := range ids {
				ids[i] = i * (metrics / hot)
			}
			if round%8 == 0 {
				for i := range metrics {
					if i%8 != 0 {
						ids = append(ids, i)
					}
				}
			}
			write(ids, uint64(round+1))
			for _, i := range ids {
				latest[i] = uint64(round + 1)
			}
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		after := db.Stats()
		if elapsed := time.Since(start); elapsed > window+period {
			t.Fatalf("input/drain exceeded window: %v", elapsed)
		}
		out.compaction = float64(after.CompactionWriteBytes-before.CompactionWriteBytes) / window.Seconds()
		out.tables = float64(after.FS.Bytes[FSWrite][FSTable]-before.FS.Bytes[FSWrite][FSTable]) / window.Seconds()
		out.endDebt, out.endL0 = float64(after.CompactionDebt), after.L0Sublevels
		out.stalls, out.stallSeconds = after.WriteStalls-before.WriteStalls, float64(after.WriteStallNanos-before.WriteStallNanos)/1e9
		out.wait = float64(after.CompactionPacerWaitNanos-before.CompactionPacerWaitNanos) / 1e9
		var mean, variance float64
		for _, v := range rates {
			mean += v / float64(len(rates))
			out.peak = max(out.peak, v)
		}
		for _, v := range rates {
			variance += (v - mean) * (v - mean) / float64(len(rates))
		}
		out.cv = math.Sqrt(variance) / mean
		out.peak /= mean
		// Every case checks all final blocks against the same write oracle.
		for i := range metrics {
			got, closer, err := db.db.Get(keys[i])
			if err != nil {
				t.Fatal(err)
			}
			want := bytes.Clone(values[i])
			binary.LittleEndian.PutUint64(want, latest[i])
			equal := bytes.Equal(got, want)
			_ = closer.Close()
			if !equal {
				t.Fatalf("metric %d changed", i)
			}
		}
		t.Logf("rate=%d memtable=%d threshold=%d table_MiB_s=%.3f compaction_MiB_s=%.3f peak_mean=%.3f cv=%.3f max_debt_MiB=%.3f end_debt_MiB=%.3f max_l0=%d end_l0=%d stalls=%d stall_s=%.3f wait_s=%.3f paced_MiB=%.3f samples=%v",
			rate, memtable, threshold, out.tables/(1<<20), out.compaction/(1<<20), out.peak, out.cv, out.debt/(1<<20), out.endDebt/(1<<20), out.l0, out.endL0, out.stalls, out.stallSeconds, out.wait, float64(after.CompactionPacerBytes-before.CompactionPacerBytes)/(1<<20), rates)
		return out
	}
	var baseline result
	t.Run("baseline", func(t *testing.T) { baseline = run(t, 2<<20, 2, 0) })
	if baseline.compaction == 0 {
		t.Fatal("workload produced no compaction output")
	}
	for _, multiple := range []float64{1.5, 2} {
		t.Run(fmt.Sprintf("paced_%.1fx", multiple), func(t *testing.T) {
			run(t, 2<<20, 2, int64(multiple*baseline.compaction))
		})
	}
	t.Run("small_memtable", func(t *testing.T) { run(t, 512<<10, 2, 0) })
	t.Run("small_memtable_raised_l0", func(t *testing.T) { run(t, 512<<10, 8, 0) })
}

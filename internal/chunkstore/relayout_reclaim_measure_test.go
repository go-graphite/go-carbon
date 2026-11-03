//go:build darwin || linux

package chunkstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

// The L6 target is 48 MiB while a group copies only 1 MiB. This deliberately
// reproduces repeatedly rewriting a bottom-level file containing many groups.
func TestRelayoutReclaimCadenceMeasurement(t *testing.T) {
	if os.Getenv("RELAYOUT_SPACE_MEASURE") == "" {
		t.Skip("opt-in on-disk cadence measurement")
	}
	metrics := 64
	if raw := os.Getenv("RELAYOUT_SPACE_METRICS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 64 || n > 256 || n%4 != 0 {
			t.Fatal("RELAYOUT_SPACE_METRICS must be a multiple of four in 64..256")
		}
		metrics = n
	}
	for _, tc := range []struct {
		name    string
		limit   uint64
		workers int
	}{{"per-group", 1, 1}, {"batched", 33 << 20, 1}, {"batched-K4", 33 << 20, 4}} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			usage, err := vfs.Default.GetDiskUsage(dir)
			if err != nil {
				t.Fatal(err)
			}
			if usage.AvailBytes < 25_000_000_000 {
				t.Fatal("stop: less than 25 GB free")
			}
			value := make([]byte, 8192)
			r := rand.New(rand.NewPCG(1, 2))
			for i := range value {
				value[i] = byte(r.Uint32())
			}
			codec := spaceRelayoutCodec{value: value}
			// Match the default L0:L6 target ratio (2:128 MiB). A flat 48 MiB
			// L0 target gives sparse metadata flushes 480 MiB of overlap and
			// does not model the store's default flush geometry.
			s, _ := openTestStore(t, dir, Options{fs: vfs.Default, ExactBlocks: true, TimeBuckets: true, Continuation: codec, RelayoutWorkers: tc.workers, RelayoutReclaimBytes: tc.limit, TargetFileSize: 48 << 20, L0TargetFileSize: 768 << 10, MemTableSize: 4 << 20, CacheSize: 4 << 20})
			defer closeTestStore(t, s)
			var metas []Metadata
			for i := 0; i < metrics; i++ {
				metas = append(metas, createTestMetric(t, s, fmt.Sprintf("cadence.%04d", i)))
			}
			if err := s.db.Compact([]byte{0}, []byte{255}, false); err != nil {
				t.Fatal(err)
			}
			for deadline := time.Now().Add(2 * time.Second); ; {
				m := s.db.Metrics()
				if m.Table.ObsoleteCount == 0 && m.Table.ZombieCount == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("initial deletion did not settle")
				}
				time.Sleep(time.Millisecond)
			}
			tables, err := s.db.SSTables()
			if err != nil {
				t.Fatal(err)
			}
			var maxL6 uint64
			for _, f := range tables[6] {
				maxL6 = max(maxL6, f.Size)
			}
			if maxL6 < 32<<20 {
				t.Fatalf("L6 fixture files too small: %d", maxL6)
			}
			before := sampleSpace(t, s, "before")
			startMetrics := s.db.Metrics()
			var startWrites, startReads uint64
			for _, l := range startMetrics.Levels {
				startWrites += l.BytesCompacted
				startReads += l.BytesRead
			}
			cpu, at := spaceCPU(), time.Now()
			peak, peakDebt, peakPending := before.Disk, before.Debt, uint64(0)
			stop, stopped := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(stopped)
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
						m := s.db.Metrics()
						peak = max(peak, m.DiskSpaceUsage())
						peakDebt = max(peakDebt, m.Compact.EstimatedDebt)
						peakPending = max(peakPending, s.RelayoutStats().UnreclaimedBytes)
					}
				}
			}()
			var once sync.Once
			stopSample := func() { once.Do(func() { close(stop); <-stopped }) }
			defer stopSample()
			ctx := context.Background()
			if tc.workers > 1 {
				if err := s.RelayoutContinuation(ctx, 0); err != nil {
					t.Fatal(err)
				}
			} else {
				for _, m := range metas {
					plan, err := codec.RelayoutPlan(ctx, s.newContinuationIO(ctx, s.db, nil, m))
					if err != nil {
						t.Fatal(err)
					}
					if err := s.relayoutGroup(ctx, []relayoutItem{{m: m, plan: plan}}); err != nil {
						t.Fatal(err)
					}
					// Match the real serial publication order, including cursor IO.
					if err := s.setRelayoutCursor(m.ID, false); err != nil {
						t.Fatal(err)
					}
				}
				if err := s.reclaimRelayout(ctx, true, 0); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.db.Flush(); err != nil {
				t.Fatal(err)
			}
			for deadline := time.Now().Add(time.Second); ; {
				m := s.db.Metrics()
				if m.Compact.NumInProgress == 0 && m.Table.ObsoleteCount == 0 && m.Table.ZombieCount == 0 {
					break
				}
				if time.Now().After(deadline) {
					break
				}
				time.Sleep(time.Millisecond)
			}
			stopSample()
			after := sampleSpace(t, s, "after")
			end := s.db.Metrics()
			var writes, reads uint64
			for _, l := range end.Levels {
				writes += l.BytesCompacted
				reads += l.BytesRead
			}
			writes -= startWrites
			reads -= startReads
			st := s.RelayoutStats()
			if st.UnreclaimedBytes != 0 {
				t.Fatalf("obligations left: %+v", st)
			}
			out := struct {
				Name                                                                                                   string
				Copied, CompactionWrites, CompactionReads, PeakGrowth, PeakDebt, EndDebt, PeakPending, Reclaims, MaxL6 uint64
				NetGrowth                                                                                              int64
				WA, WallSeconds, CPUSeconds                                                                            float64
			}{Name: tc.name, Copied: st.Bytes, CompactionWrites: writes, CompactionReads: reads, PeakGrowth: peak - before.Disk, NetGrowth: int64(after.Disk) - int64(before.Disk), PeakDebt: peakDebt, EndDebt: end.Compact.EstimatedDebt, PeakPending: peakPending, Reclaims: st.Reclaims, MaxL6: maxL6, WA: float64(writes) / float64(st.Bytes), WallSeconds: time.Since(at).Seconds(), CPUSeconds: (spaceCPU() - cpu).Seconds()}
			b, _ := json.Marshal(out)
			t.Log("CADENCE " + string(b))
			if tc.limit > 1 && writes > 3*st.Bytes {
				t.Errorf("batched compaction writes exceed 3x copied bytes: %d / %d", writes, st.Bytes)
			}
			if out.PeakGrowth > tc.limit+3*maxL6+4<<20 {
				t.Errorf("disk peak exceeds batch budget plus SST overlap: %+v", out)
			}
			for _, m := range metas {
				io := s.newContinuationIO(ctx, s.db, nil, m)
				if _, err := io.Get(spaceGroupKey(0)); !errors.Is(err, ErrNotFound) {
					t.Fatalf("old key remains: %v", err)
				}
				if v, err := io.Get(ContinuationBucketKey(0, spaceGroupKey(0))); err != nil || len(v) != len(value) {
					t.Fatalf("copy: %v", err)
				}
			}
		})
	}
}

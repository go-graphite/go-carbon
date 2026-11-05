//go:build darwin || linux

package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// Real interleaved foreground updates, with old data already at L6. Run the
// identical test on the tree before bounded maintenance spans for the
// whole-range control. Small SSTs scale the
// production byte grouping to several admitted spans without a
// multi-hundred-GB laptop fixture. The manifest regression covers 86 spans. Fsync is suppressed equally in both trees.
func TestManualCompactionIngestMeasurement(t *testing.T) {
	if os.Getenv("ZERO_MAINTENANCE_PERF") == "" {
		t.Skip("opt-in physical IO/flush measurement")
	}
	for _, mode := range []string{"reclaim", "compact-once"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			fs := newCompactionPacingFS(noSyncFS{FS: vfs.Default}, 0)
			opts := (&pebble.Options{FS: fs, MemTableSize: 4 << 20, MaxConcurrentCompactions: func() int { return 4 }, EventListener: &pebble.EventListener{TableCreated: fs.tableCreated}}).EnsureDefaults()
			opts.Levels = make([]pebble.LevelOptions, 7)
			for i := range opts.Levels {
				opts.Levels[i].TargetFileSize = 4 << 20
				opts.Levels[i].Compression = pebble.NoCompression
			}
			// Preserve the production 1:64 L0-to-deepest file target ratio.
			// A flat L0 target makes sparse live updates cover the entire
			// old layout, unlike the existing store's small L0 files.
			opts.Levels[0].TargetFileSize = 64 << 10
			opts.FlushSplitBytes = 128 << 10
			db, err := pebble.Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			s := &Store{db: db, fs: fs, dir: dir, writeOptions: pebble.NoSync, maintenanceCtx: context.Background(), pebbleOptions: opts}
			s.compactionWorkers.Store(4)
			if os.Getenv("ZERO_COMPACT_SERIAL") == "1" {
				// Isolate span admission from Pebble's unchanged four workers.
				s.compactionWorkers.Store(1)
			}
			_ = s.SetRelayoutLimits(1, ^uint64(0))
			const rows = 65536
			key := func(i int) []byte { return []byte(fmt.Sprintf("u%08d", i)) }
			value := make([]byte, 4096)
			rng := rand.New(rand.NewPCG(11, 22))
			for i := range value {
				value[i] = byte(rng.Uint32())
			}
			for i := range rows {
				if err := db.Set(key(i), value, pebble.NoSync); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Compact([]byte{0}, []byte{255}, true); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < rows; i += 2 {
				if err := db.Delete(key(i), pebble.NoSync); err != nil {
					t.Fatal(err)
				}
			}
			journal := binary.BigEndian.AppendUint64(append([]byte(nil), relayoutReclaimPrefix...), 1)
			if err := db.Set(journal, encodeRelayoutReclaim(rows*4096, map[byte]*relayoutSpan{'u': {start: key(0), end: key(rows - 1)}}), pebble.NoSync); err != nil {
				t.Fatal(err)
			}
			// Keep the deletes and an interleaved foreground write in the mutable
			// memtable at admission: Compact must perform the initial overlapping flush.
			if err := db.Set(key(1), value, pebble.NoSync); err != nil {
				t.Fatal(err)
			}
			var writes atomic.Int64
			stop, stopped := make(chan struct{}), make(chan struct{})
			writerError := make(chan error, 1)
			go func() {
				defer close(stopped)
				tick := time.NewTicker(10 * time.Millisecond)
				defer tick.Stop()
				for {
					select {
					case <-stop:
						return
					case <-tick.C:
						// Each tick spans the full old-layout range, rather than appending
						// to a disjoint new keyspace. Only retained odd keys are updated.
						for j := range 2 {
							i := int((writes.Load()*7919+int64(j)*4099)%(rows/2))*2 + 1
							if err := db.Set(key(i), value, pebble.NoSync); err != nil {
								writerError <- err
								return
							}
							writes.Add(1)
						}
					}
				}
			}()
			var once sync.Once
			join := func() { once.Do(func() { close(stop); <-stopped }) }
			defer join()
			fs.pacer.setRate(64 << 20)
			before := db.Metrics()
			var peakL0 atomic.Int64
			peakL0.Store(before.Levels[0].NumFiles)
			sampleStop, sampled := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(sampled)
				tick := time.NewTicker(time.Millisecond)
				defer tick.Stop()
				for {
					select {
					case <-sampleStop:
						return
					case <-tick.C:
						peakL0.Store(max(peakL0.Load(), db.Metrics().Levels[0].NumFiles))
					}
				}
			}()
			at := time.Now()
			if mode == "reclaim" {
				err = s.reclaimRelayout(context.Background(), true, 0)
			} else {
				err = s.startCompactOnce("measurement")
				if err == nil {
					<-s.compactOnce.done
					if msg := s.compactOnce.err.Load(); msg != nil {
						err = fmt.Errorf("compact-once: %s", *msg)
					}
				}
			}
			wall := time.Since(at).Seconds()
			join()
			close(sampleStop)
			<-sampled
			after := db.Metrics()
			fs.pacer.setRate(0)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-writerError:
				t.Fatal(err)
			default:
			}
			var compacted, flushed uint64
			for i, l := range after.Levels {
				compacted += l.BytesCompacted - before.Levels[i].BytesCompacted
				flushed += l.BytesFlushed - before.Levels[i].BytesFlushed
			}
			out := map[string]any{"mode": mode, "span_limit": min(2, int(s.compactionWorkers.Load())), "pebble_workers": 4, "wall_s": wall, "compact_write": compacted, "flushed_bytes": flushed, "flush_count": after.Flush.Count - before.Flush.Count, "L0_before": before.Levels[0].NumFiles, "L0_after": after.Levels[0].NumFiles, "L0_peak": peakL0.Load(), "ingest_updates": writes.Load()}
			b, _ := json.Marshal(out)
			t.Log("INGEST_COMPACT " + string(b))
			// The operation must preserve both deletes and every live update. All
			// writer values are the same, so the final oracle is independent of timing.
			for i := range rows {
				v, c, e := db.Get(key(i))
				if i%2 == 0 {
					if !errors.Is(e, pebble.ErrNotFound) {
						t.Fatalf("deleted %d: %v", i, e)
					}
				} else {
					if e != nil || !bytes.Equal(v, value) {
						t.Fatalf("retained %d: %v", i, e)
					}
					_ = c.Close()
				}
			}
		})
	}
}

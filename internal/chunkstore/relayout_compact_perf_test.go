//go:build darwin || linux

package chunkstore

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// An isolated manual-compaction control: identical old-key layouts and deletes,
// automatic compactions disabled, four normal Pebble compaction slots. Retained
// blocks model the live tail; history groups are deleted across many L6 files.
func TestRelayoutParallelCompactPerformance(t *testing.T) {
	if os.Getenv("RELAYOUT_RECLAIM_PERF") == "" {
		t.Skip("opt-in disk measurement")
	}
	for trial := 0; trial < 3; trial++ {
		for _, parallel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/%t", trial, parallel), func(t *testing.T) {
				fs := noSyncFS{FS: vfs.Default}
				dir := t.TempDir()
				usage, err := fs.GetDiskUsage(dir)
				if err != nil || usage.AvailBytes < 25_000_000_000 {
					t.Fatalf("disk floor: %v", err)
				}
				opts := (&pebble.Options{FS: fs, DisableAutomaticCompactions: true, MaxConcurrentCompactions: func() int { return 4 }, MemTableSize: 4 << 20}).EnsureDefaults()
				for i := range opts.Levels {
					opts.Levels[i].TargetFileSize = 4 << 20
					opts.Levels[i].Compression = pebble.NoCompression
				}
				db, err := pebble.Open(dir, opts)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				value := make([]byte, 8192)
				r := rand.New(rand.NewPCG(1, 2))
				for i := range value {
					value[i] = byte(r.Uint32())
				}
				key := func(id uint64, block int) []byte {
					return binary.BigEndian.AppendUint32(continuationPrefix(id, 1), uint32(block))
				}
				batch := db.NewBatch()
				defer batch.Close()
				for id := uint64(1); id <= 1024; id++ {
					n := 4
					if id <= 102 {
						n = 64
					}
					for block := 0; block < n; block++ {
						if err := batch.Set(key(id, block), value, nil); err != nil {
							t.Fatal(err)
						}
					}
					if batch.Len() > 2<<20 {
						if err := batch.Commit(pebble.NoSync); err != nil {
							t.Fatal(err)
						}
						batch.Reset()
					}
				}
				if err := batch.Commit(pebble.NoSync); err != nil {
					t.Fatal(err)
				}
				batch.Reset()
				if err := db.Compact([]byte{'u'}, []byte{'v'}, false); err != nil {
					t.Fatal(err)
				}
				tables, err := db.SSTables()
				if err != nil || len(tables[6]) < 4 {
					t.Fatalf("L6 fixture: %v", err)
				}
				for id := uint64(1); id <= 1024; id++ {
					n := 4
					if id <= 102 {
						n = 64
					}
					for block := 0; block < n-1; block++ {
						if err := batch.Delete(key(id, block), nil); err != nil {
							t.Fatal(err)
						}
					}
				}
				if err := batch.Commit(pebble.NoSync); err != nil {
					t.Fatal(err)
				}
				before := db.Metrics()
				cpu, at := spaceCPU(), time.Now()
				if err := db.Compact([]byte{'u'}, []byte{'v'}, parallel); err != nil {
					t.Fatal(err)
				}
				wall, cpuTime := time.Since(at), (spaceCPU() - cpu)
				after := db.Metrics()
				var reads, writes uint64
				for i, l := range after.Levels {
					reads += l.BytesRead - before.Levels[i].BytesRead
					writes += l.BytesCompacted - before.Levels[i].BytesCompacted
				}
				t.Logf("COMPACT parallel=%t L6=%d wall_s=%.6f cpu_s=%.6f read=%d write=%d", parallel, len(tables[6]), wall.Seconds(), cpuTime.Seconds(), reads, writes)
			})
		}
	}
}

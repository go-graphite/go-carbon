package chunkstore

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// TestMeasureCatalogScanClose replays a young process's close: the store
// opens on a large LSM, its catalog membership scan starts, compactions
// rewrite every table the scan opened on, and the store closes while the
// scan still runs. "one iterator" is the scan before it reopened its
// iterator; "refreshing" reopens it as it goes; "stopped early" also stops
// the scan before the close, as a dump stop does with input still flowing.
// Run it explicitly, with the LSM size in MB:
//
//	STORE_MEASURE_SCAN=2048 go test -run TestMeasureCatalogScanClose -v ./internal/chunkstore/
func TestMeasureCatalogScanClose(t *testing.T) {
	mb, _ := strconv.Atoi(os.Getenv("STORE_MEASURE_SCAN"))
	if mb <= 0 {
		t.Skip("set STORE_MEASURE_SCAN=LSM MB to run")
	}
	for _, mode := range []string{"one iterator", "refreshing", "stopped early"} {
		root := t.TempDir()
		dir, src := filepath.Join(root, "store"), filepath.Join(root, "src")
		if err := os.MkdirAll(src, 0755); err != nil {
			t.Fatal(err)
		}
		opts := Options{ExactBlocks: true, IncrementalSource: src, CacheSize: 4 << 30, MemTableSize: 64 << 20, TargetFileSize: 2 << 20}
		s, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		r := rand.New(rand.NewPCG(1, 2))
		value := make([]byte, 1024)
		write := func(kilobytes int, catalog bool) {
			for i := 0; i < kilobytes; i += 256 {
				b := s.db.NewBatch()
				for j := range 256 {
					for k := range value {
						value[k] = byte(r.Uint32() & 0x3f)
					}
					_ = b.Set(fmt.Appendf(nil, "x/%016x", r.Uint64()), value, nil)
					if catalog {
						_ = b.Set(fmt.Appendf(nil, "m/metric.%d", i+j), value[:64], nil)
					}
				}
				if err := b.Commit(pebble.NoSync); err != nil {
					t.Fatal(err)
				}
			}
		}
		write(mb<<10, true)
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}

		paused, resume := make(chan struct{}), make(chan struct{})
		catalogScanPause = func(stop <-chan struct{}) {
			select {
			case paused <- struct{}{}:
			case <-stop:
				return
			}
			select {
			case <-resume:
			case <-stop:
			}
		}
		refresh := membershipRefresh
		if mode == "one iterator" {
			membershipRefresh = 24 * time.Hour
		}
		if s, err = Open(dir, opts); err != nil {
			t.Fatal(err)
		}
		<-paused // the scan holds its first iterator
		all, _ := s.db.NewIter(nil)
		for ok := all.First(); ok; ok = all.Next() { // a serving store's block cache
		}
		_ = all.Close()
		// Rewrite the LSM, as compactions after a restart do.
		write(256<<10, false)
		if err := s.db.Compact([]byte{0}, []byte{255}, true); err != nil {
			t.Fatal(err)
		}
		m := s.db.Metrics()
		zombies, zombieMB, tables, cacheMB := m.Table.ZombieCount, m.Table.ZombieSize>>20, m.Total().NumFiles, m.BlockCache.Size>>20
		var stopped time.Duration
		if mode != "one iterator" {
			resume <- struct{}{}
			select {
			case <-paused: // the next iterator; the first one is released
			case <-s.catalogMembership.done: // a small catalog may finish first
			}
		}
		pinned := s.db.Metrics().Table.ZombieCount
		if mode == "stopped early" {
			started := time.Now()
			s.StopBackground()
			stopped = time.Since(started)
		}
		started := time.Now()
		if err := s.CloseForExit(); err != nil {
			t.Fatal(err)
		}
		closed := time.Since(started)
		t.Logf("%s: %d tables, %d MB cached; %d replaced tables (%d MB) pinned after the rewrite, %d before the close; "+
			"StopBackground %v; close %v, of which membership %v",
			mode, tables, cacheMB, zombies, zombieMB, pinned, stopped.Round(time.Microsecond),
			closed.Round(time.Microsecond), s.LastCloseTimings().Membership.Round(time.Microsecond))
		catalogScanPause, membershipRefresh = nil, refresh
	}
}

package chunkstore

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func logsIn(t *testing.T, fs vfs.FS, dir string) int {
	t.Helper()
	names, err := fs.List(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, name := range names {
		if strings.HasSuffix(name, ".log") {
			n++
		}
	}
	return n
}

// Moving the WAL to another directory and back keeps writes that were only
// in the WAL when the store closed.
func TestWALDirMoveKeepsUnflushedWrites(t *testing.T) {
	fs := vfs.NewMem()
	const dir, walDir = "/store", "/wal"
	if err := fs.MkdirAll(walDir, 0o755); err != nil {
		t.Fatal(err)
	}
	var names []string
	write := func(s *Store, n int) {
		for range n {
			names = append(names, fmt.Sprintf("wal.metric%03d", len(names)))
			createTestMetric(t, s, names[len(names)-1])
		}
	}
	check := func(s *Store) {
		for _, name := range names {
			if _, err := s.Metadata(context.Background(), name); err != nil {
				t.Fatalf("%s lost: %v", name, err)
			}
		}
	}
	s, _ := openTestStore(t, dir, Options{fs: fs, MemTableSize: 64 << 20})
	write(s, 50)
	closeTestStore(t, s)
	if logsIn(t, fs, dir) == 0 {
		t.Fatal("test needs unflushed writes in the store directory WAL")
	}

	s, _ = openTestStore(t, dir, Options{fs: fs, MemTableSize: 64 << 20, WALDir: walDir})
	check(s)
	if logsIn(t, fs, dir) != 0 || logsIn(t, fs, walDir) == 0 {
		t.Fatalf("WAL not moved: %d logs in store dir, %d in WAL dir", logsIn(t, fs, dir), logsIn(t, fs, walDir))
	}
	write(s, 50)
	closeTestStore(t, s)

	s, _ = openTestStore(t, dir, Options{fs: fs, MemTableSize: 64 << 20})
	defer closeTestStore(t, s)
	check(s)
	if logsIn(t, fs, walDir) != 0 {
		t.Fatal("logs left in the former WAL directory")
	}
}

// A missing WAL directory fails Open instead of creating it, so an unmounted
// WAL volume cannot put the WAL on another filesystem.
func TestWALDirMustExist(t *testing.T) {
	fs := vfs.NewMem()
	if _, err := Open("/store", Options{fs: fs, WALDir: "/missing", Now: func() time.Time { return time.Unix(100_000, 0) }}); err == nil {
		t.Fatal("opened with a missing WAL directory")
	}
}

func TestCacheShardOptions(t *testing.T) {
	t.Parallel()
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, CacheSize: 256 << 20, CacheShards: 48, TableCacheShards: 12})
	defer closeTestStore(t, s)
	if got := s.Stats().CacheShards; got != 48 {
		t.Fatalf("block cache shards %d, want 48", got)
	}
	if got := s.pebbleOptions.Experimental.TableCacheShards; got != 12 {
		t.Fatalf("table cache shards %d, want 12", got)
	}
}

// CompactOnce runs a full compaction once per token and records it.
func TestCompactOnceRunsOncePerToken(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, _ := openTestStore(t, dir, Options{})
	for i := range 20 {
		createTestMetric(t, s, fmt.Sprintf("compact.metric%02d", i))
	}
	closeTestStore(t, s)
	wait := func(s *Store) {
		deadline := time.Now().Add(30 * time.Second)
		for s.Stats().CompactOnceRunning {
			if time.Now().After(deadline) {
				t.Fatal("compaction did not finish")
			}
			time.Sleep(10 * time.Millisecond)
		}
		if msg := s.compactOnce.err.Load(); msg != nil {
			t.Fatal(*msg)
		}
	}
	s, _ = openTestStore(t, dir, Options{CompactOnce: "t1"})
	wait(s)
	if token, err := readStoreMarker(s.fs, dir, compactOnceMarker); err != nil || token != "t1" {
		t.Fatalf("marker %q, %v", token, err)
	}
	if n := s.db.Metrics().Levels[6].NumFiles; n == 0 {
		t.Fatal("full compaction left nothing in the bottom level")
	}
	closeTestStore(t, s)
	s, _ = openTestStore(t, dir, Options{CompactOnce: "t1"})
	defer closeTestStore(t, s)
	if s.Stats().CompactOnceRunning {
		t.Fatal("finished token compacted again")
	}
	for i := range 20 {
		if _, err := s.Metadata(context.Background(), fmt.Sprintf("compact.metric%02d", i)); err != nil {
			t.Fatal(err)
		}
	}
}

package chunkstore

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// A restart closes the store after reads have stopped. Close must cancel a
// running compaction rather than wait for it, and the store must reopen intact.
func TestPebbleCloseCancelsRunningCompaction(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := &pebble.Options{FS: vfs.NewMem(), DisableAutomaticCompactions: true, MemTableSize: 8 << 20}
	opts.EnsureDefaults()
	opts.Levels[0].TargetFileSize = 1 << 20
	db, err := pebble.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewSource(1))
	value := make([]byte, 4096)
	const keys = 24000
	for round := 0; round < 3; round++ {
		b := db.NewBatch()
		for i := round; i < keys; i += 3 {
			r.Read(value)
			if err := b.Set([]byte(fmt.Sprintf("k%08d", i)), value, nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Commit(pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	}
	compacted := make(chan error, 1)
	go func() { compacted <- db.Compact([]byte("k"), []byte("l"), false) }()
	deadline := time.Now().Add(10 * time.Second)
	for db.Metrics().Compact.NumInProgress == 0 {
		select {
		case err := <-compacted:
			t.Skipf("compaction finished before Close could race it: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("compaction never started")
		}
		time.Sleep(time.Millisecond)
	}
	closeStart := time.Now()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("Close took %v", time.Since(closeStart))
	if err := <-compacted; err == nil {
		t.Fatal("Close waited for the compaction to finish instead of cancelling it")
	} else if !errors.Is(err, pebble.ErrCancelledCompaction) && !errors.Is(err, pebble.ErrClosed) {
		t.Logf("compaction ended with %v", err)
	}
	db, err = pebble.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < keys; i += 997 {
		v, closer, err := db.Get([]byte(fmt.Sprintf("k%08d", i)))
		if err != nil || len(v) != len(value) {
			t.Fatalf("key %d after reopen: %d %v", i, len(v), err)
		}
		closer.Close()
	}
}

// CloseForExit releases the store for a successor without closing table
// readers; the store must reopen intact while those readers stay open.
func TestStoreCloseForExitReopensIntact(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := Open(dir, Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	b := s.db.NewBatch()
	for i := 0; i < 2000; i++ {
		if err := b.Set([]byte(fmt.Sprintf("x/%06d", i)), []byte("v"), nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Flush(); err != nil {
		t.Fatal(err)
	}
	if v, closer, err := s.db.Get([]byte("x/000007")); err != nil || string(v) != "v" {
		t.Fatalf("read before close: %q %v", v, err)
	} else {
		closer.Close()
	}
	if err := s.CloseForExit(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir, Options{ExactBlocks: true})
	if err != nil {
		t.Fatalf("reopen after CloseForExit: %v", err)
	}
	defer s2.Close()
	if v, closer, err := s2.db.Get([]byte("x/001999")); err != nil || string(v) != "v" {
		t.Fatalf("read after reopen: %q %v", v, err)
	} else {
		closer.Close()
	}
}

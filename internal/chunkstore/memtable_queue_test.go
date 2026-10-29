package chunkstore

import "testing"

// Every queued memtable is replayed from the WAL before a restarted store
// serves, so large memtables get a shallower queue.
func TestMemTableQueueIsBoundedByBytes(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		size uint64
		want int
	}{{256 << 20, 16}, {1 << 30, 8}, {2 << 30, 4}, {3 << 30, 4}} {
		s, err := Open(t.TempDir(), Options{ExactBlocks: true, MemTableSize: c.size})
		if err != nil {
			t.Fatal(err)
		}
		got := s.pebbleOptions.MemTableStopWritesThreshold
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if got != c.want {
			t.Fatalf("memtable size %d: queue %d, want %d", c.size, got, c.want)
		}
	}
}

func TestL0AndBaseLevelOptionsReachPebble(t *testing.T) {
	t.Parallel()
	s, err := Open(t.TempDir(), Options{ExactBlocks: true, L0CompactionThreshold: 8, L0StopWritesThreshold: 32, LBaseMaxBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	o := s.pebbleOptions
	if o.L0CompactionThreshold != 8 || o.L0StopWritesThreshold != 32 || o.LBaseMaxBytes != 1<<30 {
		t.Fatalf("options not applied: %d %d %d", o.L0CompactionThreshold, o.L0StopWritesThreshold, o.LBaseMaxBytes)
	}
	if st := s.Stats(); len(st.LevelBytes) != 7 || len(st.LevelCompactedBytes) != 7 {
		t.Fatal("per-level stats missing")
	}
}

func TestMemTableQueueBudgetIsConfigurable(t *testing.T) {
	t.Parallel()
	s, err := Open(t.TempDir(), Options{ExactBlocks: true, MemTableSize: 2 << 30, MaxUnflushedMemTableBytes: 16 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := s.pebbleOptions.MemTableStopWritesThreshold; got != 8 {
		t.Fatalf("queue %d, want 8", got)
	}
}

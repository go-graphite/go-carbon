package v2core

import (
	"slices"
	"testing"
	"time"
)

func TestCoreObservationUsesImmutableSnapshot(t *testing.T) {
	t.Parallel()
	cfg := coreTestConfig()
	e, err := Open(t.TempDir(), cfg, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	input := []Row{{ID: 7, Time: 1, Bits: 0x7ff8000000000042, Seq: 1}}
	if err = e.Append(0, input, 1, true); err != nil {
		t.Fatal(err)
	}
	snap := e.owners[Shard(7, cfg.Shards)].db.NewSnapshot()
	defer snap.Close()
	if err = e.Append(0, []Row{{ID: 7, Time: 2, Bits: 1 << 63, Seq: 2}}, 2, true); err != nil {
		t.Fatal(err)
	}
	rows, err := Read(snap, 7)
	if err != nil || !slices.Equal(input, rows) {
		t.Fatalf("snapshot changed: %v %v", rows, err)
	}
	stats := Sample(e, "external", time.Now(), 2)
	if _, ok := stats["compactions_in_progress"].(int64); !ok {
		t.Fatal("missing active-compaction count")
	}
	if stats["committed"] != uint64(2) || stats["stage"] != "external" {
		t.Fatal(stats)
	}
}

package v2core

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func TestArchivesAuthoritativeSnapshotRecoveryAndRelocation(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups = 1, 1, true
	_, policy := rollupTestPolicy(t, "authority.metric", "1s:120,1m:1d", sealrollup.Last, 0)
	root := filepath.Join(t.TempDir(), "source")
	e, err := openCore(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	appendPoint := func(order uint64, bits uint64) {
		t.Helper()
		if err := e.submitBatches(0, []coreBatch{{ID: 7, Order: order, Clock: 10000, Policy: policy, Points: []row{{ID: 7, Time: 9990, Bits: bits, Seq: order}}}}, order, true); err != nil {
			t.Fatal(err)
		}
	}
	read := func(snapshot *pebble.Snapshot, want uint64) {
		t.Helper()
		n := 0
		err := ScanRollups(snapshot, func(_ RollupPolicy, m RollupMetadata, records []sealrollup.Record) error {
			n++
			if m.Frontier.Order != want || len(records) == 0 || records[0].Sample.Bits != want {
				t.Fatalf("wrong authority: %+v %+v", m, records)
			}
			return nil
		})
		if err != nil || n != 1 {
			t.Fatalf("read %d: %v", n, err)
		}
	}
	appendPoint(1, 1)
	old := e.owners[0].db.NewSnapshot()
	appendPoint(2, 2)
	current := e.owners[0].db.NewSnapshot()
	read(old, 1)
	read(current, 2)
	// Archive replacement keys must never enter the LSM, including after Flush.
	if err := e.owners[0].db.Flush(); err != nil {
		t.Fatal(err)
	}
	it, err := e.owners[0].db.NewIter(&pebble.IterOptions{LowerBound: []byte{'G', 'C', 'P', 3}, UpperBound: []byte{'G', 'C', 'P', 4}})
	if err != nil {
		t.Fatal(err)
	}
	if it.First() {
		t.Fatal("archive row re-ingested into LSM")
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	// The indexed range reader follows exactly the same descriptor authority.
	directory, err := NewRollupSnapshot(context.Background(), current, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	records, err := directory.Records(context.Background(), "authority.metric")
	if err != nil || len(records) == 0 || records[0].Sample.Bits != 2 {
		t.Fatalf("indexed read: %v %v", records, err)
	}
	old.Close()
	current.Close()
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	moved := filepath.Join(t.TempDir(), "destination")
	if err = os.Rename(root, moved); err != nil {
		t.Fatal(err)
	}
	// A relocated read-only open must fail closed, never read another root.
	if wrong, err := OpenReadOnly(moved, cfg); err == nil {
		wrong.Close()
		t.Fatal("stale local locator accepted")
	}
	e, err = openCore(moved, cfg)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := e.owners[0].db.NewSnapshot()
	read(snapshot, 2)
	snapshot.Close()
	appendPoint(3, 3)
	snapshot = e.owners[0].db.NewSnapshot()
	read(snapshot, 3)
	snapshot.Close()
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	// Committed missing originals are an error, rather than a legacy LSM fallback.
	files, err := filepath.Glob(filepath.Join(moved, "owner-000", "parts", "*.sst"))
	if err != nil || len(files) == 0 {
		t.Fatal(files, err)
	}
	if err = os.Remove(files[0]); err != nil {
		t.Fatal(err)
	}
	if broken, err := openCore(moved, cfg); err == nil {
		broken.close()
		t.Fatal("missing authoritative original accepted")
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
}

func TestArchiveCorruptEarlierExternalLevelFailsClosed(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups = 1, 1, true
	_, policy := rollupTestPolicy(t, "corrupt.metric", "1s:120,1m:1d", sealrollup.Last, 0)
	root := t.TempDir()
	e, err := openCore(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for order := uint64(1); order <= 3; order++ {
		if err = e.submitBatches(0, []coreBatch{{ID: 7, Order: order, Clock: 10000, Policy: policy, Points: []row{{ID: 7, Time: 9990, Bits: order, Seq: order}}}}, order, true); err != nil {
			t.Fatal(err)
		}
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(root, "owner-000", "parts", "*.sst"))
	if err != nil || len(files) < 3 {
		t.Fatal(files, err)
	}
	f, err := os.OpenFile(files[0], os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt([]byte{0}, st.Size()-1); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	if broken, err := openCore(root, cfg); err == nil {
		broken.close()
		t.Fatal("corrupt earlier archive level silently skipped")
	}
}

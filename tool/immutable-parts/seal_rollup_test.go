package main

import (
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func atomicTestConfig() sealrollup.Config {
	return sealrollup.Config{Archives: []sealrollup.Archive{{Step: 1, Count: 120}, {Step: 60, Count: 1440}}, Method: sealrollup.Sum, XFilesFactor: 0}
}
func atomicTestInputs(order uint64, value float64) []atomicRollupInput {
	return []atomicRollupInput{{ID: 42, Config: atomicTestConfig(), Batches: []sealrollup.Batch{{Order: order, Now: 1800000060, Points: []sealrollup.Sample{{Time: 1800000001, Bits: math.Float64bits(value), Sequence: 1}, {Time: 1800000002, Bits: math.Float64bits(3), Sequence: 2}}}}}}
}
func atomicOpen(t *testing.T, root string) (*pebble.DB, *atomicRollupOwner) {
	t.Helper()
	db, err := pebble.Open(filepath.Join(root, "db"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	o, err := openAtomicRollupOwner(db, root)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	return db, o
}
func TestAtomicRollupCommitAndSnapshot(t *testing.T) {
	root := t.TempDir()
	db, o := atomicOpen(t, root)
	if _, err := o.Seal(atomicTestInputs(1, 2)); err != nil {
		t.Fatal(err)
	}
	snap := db.NewSnapshot()
	before := o.state[42].Frontier
	o.beforeIngest = func() error { return errors.New("injected staging failure") }
	if _, err := o.Seal(atomicTestInputs(2, 7)); err == nil || o.state[42].Frontier != before || o.Errors.Load() != 1 {
		t.Fatal("failed seal published", err)
	}
	o.beforeIngest = nil
	if _, err := o.Seal(atomicTestInputs(2, 7)); err != nil {
		t.Fatal(err)
	}
	if o.state[42].snapshotSlots()[[2]int64{1, 480}].Bits != math.Float64bits(10) {
		t.Fatal("coarse correction incorrect")
	}
	// Old readers keep a complete prior raw/archive/frontier generation.
	value, closer, err := snap.Get(rollupMetaKey('s', 42))
	if err != nil {
		t.Fatal(err)
	}
	saved := append([]byte(nil), value...)
	closer.Close()
	value, closer, err = db.Get(rollupMetaKey('s', 42))
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(saved, value) {
		t.Fatal("snapshot advanced")
	}
	closer.Close()
	snap.Close()
	want := o.state[42].Frontier
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, o = atomicOpen(t, root)
	defer db.Close()
	if o.state[42].Frontier != want || o.state[42].snapshotSlots()[[2]int64{1, 480}].Bits != math.Float64bits(10) {
		t.Fatal("recovery changed correction")
	}
	inputs := atomicTestInputs(3, 9)
	inputs[0].Config.Method = 99
	if _, err = o.Seal(inputs); err == nil || o.Errors.Load() != 1 {
		t.Fatal("invalid policy not fail closed")
	}
	if o.state[42].Frontier != want {
		t.Fatal("invalid policy advanced frontier")
	}
}

func TestAtomicRollupCrash(t *testing.T) {
	if root := os.Getenv("ATOMIC_ROLLUP_CRASH_ROOT"); root != "" {
		db, o := atomicOpen(t, root)
		if _, err := o.Seal(atomicTestInputs(1, 2)); err != nil {
			t.Fatal(err)
		}
		kill := func() error { _ = syscall.Kill(os.Getpid(), syscall.SIGKILL); return errors.New("kill returned") }
		switch os.Getenv("ATOMIC_ROLLUP_CRASH_STAGE") {
		case "before":
			o.beforeIngest = kill
		case "after-ingest":
			o.afterIngest = kill
		}
		if _, err := o.Seal(atomicTestInputs(2, 7)); err != nil {
			t.Fatal(err)
		}
		_ = kill()
		db.Close()
		return
	}
	for _, stage := range []string{"before", "after-ingest", "after-publish"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestAtomicRollupCrash$")
			cmd.Env = append(os.Environ(), "ATOMIC_ROLLUP_CRASH_ROOT="+root, "ATOMIC_ROLLUP_CRASH_STAGE="+stage)
			if output, err := cmd.CombinedOutput(); err == nil {
				t.Fatalf("child survived %s", output)
			}
			db, o := atomicOpen(t, root)
			wantOrder := uint64(2)
			wantValue := float64(10)
			if stage == "before" {
				wantOrder = 1
				wantValue = 5
			}
			if o.part != wantOrder || o.state[42].Frontier.Order != wantOrder || o.state[42].snapshotSlots()[[2]int64{1, 480}].Bits != math.Float64bits(wantValue) {
				t.Fatalf("torn crash recovery %+v", o.state[42])
			}
			// Whole input batches at/below frontier are deduplicated by replay before Seal.
			if wantOrder == 1 {
				if _, err := o.Seal(atomicTestInputs(2, 7)); err != nil {
					t.Fatal(err)
				}
			}
			count := 0
			it, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte{'S'}, UpperBound: []byte{'T'}})
			if err != nil {
				t.Fatal(err)
			}
			for it.First(); it.Valid(); it.Next() {
				if len(it.Key()) != 31 {
					t.Fatal("bad raw/archive key")
				}
				count++
			}
			err = it.Error()
			it.Close()
			if err != nil {
				t.Fatal(err)
			}
			if count != 8 {
				t.Fatalf("lost/duplicated raw+archive records: %d", count)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

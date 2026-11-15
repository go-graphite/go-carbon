package main

import (
	"encoding/binary"
	"math"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func TestSealArchiveParts(t *testing.T) {
	dir := t.TempDir()
	db, err := pebble.Open(filepath.Join(dir, "db"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	c := sealrollup.Config{Archives: []sealrollup.Archive{{Step: 1, Count: 86400}, {Step: 60, Count: 43200}}, Method: sealrollup.Sum, XFilesFactor: 0}
	state := map[int]map[int64]sealrollup.Sample{0: {}, 1: {}}
	view := func(a int, slot int64) (sealrollup.Sample, bool) { p, ok := state[a][slot]; return p, ok }
	makeInput := func(order uint64, value float64) []sealArchiveInput {
		return []sealArchiveInput{{ID: 42, Config: c, View: view, Batches: []sealrollup.Batch{{Order: order, Now: 1800010000, Points: []sealrollup.Sample{{Time: 1800000000, Bits: math.Float64bits(value), Sequence: order}}}}}}
	}
	if n, ds, err := sealArchiveParts(db, dir, 0, 1, makeInput(1, 5), false); err != nil || n != 0 || len(ds) != 0 {
		t.Fatal("disabled adapter wrote")
	}
	for part := uint64(1); part <= 2; part++ {
		n, ds, err := sealArchiveParts(db, dir, 0, part, makeInput(part, float64(part)), true)
		if err != nil || n == 0 || len(ds) != 2 {
			t.Fatalf("seal%d bytes%d deltas%d %v", part, n, len(ds), err)
		}
		for _, r := range ds {
			state[r.Delta.Archive][r.Delta.Slot] = r.Delta.Sample
		}
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = pebble.Open(filepath.Join(dir, "db"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte{'R'}, UpperBound: []byte{'S'}})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	count := 0
	for it.First(); it.Valid(); it.Next() {
		count++
		v := it.Value()
		order := binary.LittleEndian.Uint64(v[16:])
		bits := binary.LittleEndian.Uint64(v[8:])
		if order < 1 || order > 2 || bits != math.Float64bits(float64(order)) {
			t.Fatalf("bad persisted correction %x", v)
		}
	}
	if err = it.Error(); err != nil {
		t.Fatal(err)
	}
	if count != 4 {
		t.Fatalf("persisted corrections%d want4", count)
	}
}

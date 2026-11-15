package main

import (
	"bytes"
	"math"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/partspack"
)

func TestDictionaryFixturePersistsExactStoredSlots(t *testing.T) {
	ps := []chunkstore.Point{{Timestamp: 1000020, Value: math.Float64frombits(0x7ff8000000000012)}, {Timestamp: 1000080, Value: math.Copysign(0, -1)}, {Timestamp: 1000140, Value: 42.125}}
	dst := filepath.Join(t.TempDir(), "packed")
	if err := compactFixture(fixtureDB(t, ps), dst); err != nil {
		t.Fatal(err)
	}
	db, err := pebble.Open(filepath.Join(dst, "db"), &pebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	it, err := db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	if !it.First() {
		t.Fatal("missing packed part")
	}
	p, err := partspack.OpenPart(it.Value())
	if err != nil || len(p.Records) != 1 {
		t.Fatal("packed part", err)
	}
	got, err := partspack.DecodeBlock(p.Records[0].Data)
	if err != nil || len(got) != len(ps) {
		t.Fatal("stored points", err)
	}
	for i := range ps {
		if ps[i].Timestamp != got[i].Timestamp || math.Float64bits(ps[i].Value) != math.Float64bits(got[i].Value) {
			t.Fatal("changed stored bits")
		}
	}
}

func TestBundledPartitionDeletePreservesPinnedAndSurvivingParts(t *testing.T) {
	firstPoints := []chunkstore.Point{{Timestamp: 1000020, Value: math.Float64frombits(0x7ff8000000000012)}}
	src := fixtureDB(t, firstPoints)
	db, err := pebble.Open(src, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	it, err := db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !it.First() {
		t.Fatal("missing source")
	}
	key, err := partsformat.Decode(it.Key())
	if err != nil {
		t.Fatal(err)
	}
	_ = it.Close()
	key.Partition++
	key.BlockStart += 86400
	secondPoints := []chunkstore.Point{{Timestamp: 1086420, Value: math.Copysign(0, -1)}}
	kb, err := partsformat.Encode(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set(kb, archiveFixture(secondPoints), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "packed")
	if err := compactFixtureTables(src, dst, true); err != nil {
		t.Fatal(err)
	}
	db, err = pebble.Open(filepath.Join(dst, "db"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := db.NewSnapshot()
	pinned, err := snapshot.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !pinned.First() {
		t.Fatal("missing pinned part")
	}
	before := bytes.Clone(pinned.Value())
	first, err := partsformat.Decode(pinned.Key())
	if err != nil {
		t.Fatal(err)
	}
	lower, err := partsformat.PartitionPrefixForGroup(first.Group, first.VShard, first.Partition)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteRange(lower, partsformat.PrefixEnd(lower), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact([]byte{0}, []byte{255}, true); err != nil {
		t.Fatal(err)
	}
	if !pinned.First() || !bytes.Equal(before, pinned.Value()) {
		t.Fatal("range expiry changed pinned immutable bytes")
	}
	_ = pinned.Close()
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact([]byte{0}, []byte{255}, true); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = pebble.Open(filepath.Join(dst, "db"), &pebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	it, err = db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	if !it.First() {
		t.Fatal("surviving part was deleted")
	}
	p, err := partspack.OpenPart(it.Value())
	if err != nil || len(p.Records) != 1 {
		t.Fatal("surviving part", err)
	}
	points, err := partspack.DecodeBlock(p.Records[0].Data)
	if err != nil || len(points) != 1 || points[0].Timestamp != secondPoints[0].Timestamp || math.Float64bits(points[0].Value) != math.Float64bits(secondPoints[0].Value) {
		t.Fatal("surviving stored slot changed", err)
	}
	if it.Next() {
		t.Fatal("expired partition still visible")
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
}

func TestDictionaryFixtureRejectsAmbiguousStoredImages(t *testing.T) {
	for _, ps := range [][]chunkstore.Point{nil, {{Timestamp: 60, Value: 1}, {Timestamp: 60, Value: 2}}} {
		if err := compactFixture(fixtureDB(t, ps), filepath.Join(t.TempDir(), "out")); err == nil {
			t.Fatal("accepted empty/duplicate stored slots")
		}
	}
}

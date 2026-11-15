package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/partspack"
)

func archiveFixture(ps []chunkstore.Point) []byte {
	b := make([]byte, 36+16*len(ps))
	copy(b, "GCA2")
	binary.BigEndian.PutUint32(b[12:], 1)
	binary.BigEndian.PutUint32(b[20:], uint32(len(ps)))
	binary.BigEndian.PutUint32(b[28:], 60)
	binary.BigEndian.PutUint32(b[32:], 10080)
	for i, p := range ps {
		binary.BigEndian.PutUint64(b[36+16*i:], uint64(p.Timestamp))
		binary.BigEndian.PutUint64(b[44+16*i:], math.Float64bits(p.Value))
	}
	return b
}
func fixtureDB(t *testing.T, ps []chunkstore.Point) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source")
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	key, err := partsformat.Encode(partsformat.Key{Metric: partsformat.ID("test.metric"), VShard: partsformat.Route(partsformat.ID("test.metric")), Part: partsformat.PartID{1}, BlockStart: 1000000})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Set(key, archiveFixture(ps), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}
func TestOfflineRepackPersistsExactBits(t *testing.T) {
	ps := []chunkstore.Point{{Timestamp: 1000020, Value: math.Float64frombits(0x7ff8000000000012)}, {Timestamp: 1000080, Value: math.Copysign(0, -1)}, {Timestamp: 1000140, Value: 42.125}}
	src := fixtureDB(t, ps)
	for _, repartition := range []bool{false, true} {
		dst := filepath.Join(t.TempDir(), "packed")
		if err := repack(src, dst, false, repartition, 4); err != nil {
			t.Fatal(err)
		}
		policy, err := os.ReadFile(filepath.Join(dst, "policy"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := partspack.ParsePolicy(policy); err != nil {
			t.Fatal(err)
		}
		layout, err := os.ReadFile(filepath.Join(dst, "layout.json"))
		if err != nil {
			t.Fatal(err)
		}
		var manifest struct {
			ProductionReady bool              `json:"production_ready"`
			Intervals       []json.RawMessage `json:"intervals"`
		}
		if err := json.Unmarshal(layout, &manifest); err != nil || manifest.ProductionReady || len(manifest.Intervals) != 1 {
			t.Fatalf("manifest: %s %v", layout, err)
		}
		db, err := pebble.Open(filepath.Join(dst, "db"), &pebble.Options{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		it, err := db.NewIter(nil)
		if err != nil {
			t.Fatal(err)
		}
		var got []chunkstore.Point
		for ok := it.First(); ok; ok = it.Next() {
			points, err := partspack.DecodeBlock(it.Value())
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, points...)
		}
		if err := it.Error(); err != nil {
			t.Fatal(err)
		}
		it.Close()
		db.Close()
		if len(got) != len(ps) {
			t.Fatalf("points %d", len(got))
		}
		for i := range ps {
			if got[i].Timestamp != ps[i].Timestamp || math.Float64bits(got[i].Value) != math.Float64bits(ps[i].Value) {
				t.Fatalf("changed point %d", i)
			}
		}
	}
}
func TestRepartitionRejectsLostOrAmbiguousSlots(t *testing.T) {
	for _, ps := range [][]chunkstore.Point{nil, {{Timestamp: 60, Value: 1}, {Timestamp: 60, Value: 2}}} {
		if err := repack(fixtureDB(t, ps), filepath.Join(t.TempDir(), "out"), false, true, 4); err == nil {
			t.Fatal("accepted empty/duplicate slots")
		}
	}
}
func TestArchiveDecoderRejectsMalformed(t *testing.T) {
	valid := archiveFixture([]chunkstore.Point{{Timestamp: 60, Value: 1}})
	for n := 0; n < len(valid); n++ {
		if _, _, _, err := oldArchive(valid[:n]); err == nil {
			t.Fatalf("accepted truncation %d", n)
		}
	}
	for _, offset := range []int{12, 16, 20, 24} {
		bad := bytes.Clone(valid)
		binary.BigEndian.PutUint32(bad[offset:], math.MaxUint32)
		if _, _, _, err := oldArchive(bad); err == nil {
			t.Fatalf("accepted invalid field %d", offset)
		}
	}
}

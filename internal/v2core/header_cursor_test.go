package v2core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/sstable"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

type headerKV struct {
	key   []byte
	value []byte // a run for archive keys, an envelope at MetadataArchive
	k     v2part.Key
	runs  []sealrollup.Record
}

// headerFixture is one part's logical stream: names with one or two archives,
// runs longer than one dense run, names without an envelope and names with
// only an envelope (every run dropped).
func headerFixture(t testing.TB, names int) (v2part.Spec, []headerKV) {
	t.Helper()
	spec := v2part.Spec{Scope: v2part.Scope{Group: "example/cell-3", VShard: v2part.GroupVShard, Part: v2part.PartID{1, 2, 3, 4}}, Source: v2part.Provenance{Incarnation: v2part.PartID{9}, Stream: "input-0", MinOrder: 1, MaxOrder: 9}, MinTime: 1700000000, MaxTime: 1800000000}
	for a := 0; a < 2; a++ {
		def := []byte(fmt.Sprintf(`{"archive":%d}`, a))
		spec.Policies = append(spec.Policies, v2part.ArchivePolicy{Archive: uint32(a), Digest: sha256.Sum256(def), Definition: def})
	}
	var rows []headerKV
	add := func(k v2part.Key, value []byte, runs []sealrollup.Record) {
		key, err := v2part.Encode(k)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, headerKV{key: key, value: value, k: k, runs: runs})
	}
	for i := 0; i < names; i++ {
		name := v2part.ID(fmt.Sprintf("header.fixture.m%d", i))
		base := v2part.Key{Group: spec.Scope.Group, VShard: v2part.Route(name), Partition: spec.Scope.Partition, Part: spec.Scope.Part, Metric: name}
		archives := 1 + i%2
		if i%11 == 0 {
			archives = 0
		}
		for a := 0; a < archives; a++ {
			slots := 1 + i%5
			if i%13 == 0 && a == 0 {
				slots = v2part.MaxDenseSlots + 7
			}
			var records []sealrollup.Record
			for s := 0; s < slots; s++ {
				slot := int64(s*3 + i%3)
				bits := math.Float64bits(float64(i*1000 + s))
				if i%4 == 0 {
					bits = math.Float64bits(float64(i) + float64(s)/7)
				}
				records = append(records, sealrollup.Record{Delta: sealrollup.Delta{Archive: a, Slot: slot, Order: uint64(1 + s%3), Sample: sealrollup.Sample{Time: 1700000000 + slot*60, Bits: bits}}, Correction: s%9 == 1})
			}
			for len(records) > 0 {
				n := min(len(records), v2part.MaxDenseSlots)
				k := base
				k.Archive, k.BlockStart = uint32(a), records[0].Slot
				run, err := v2part.EncodeSlots(records[:n])
				if err != nil {
					t.Fatal(err)
				}
				add(k, run, records[:n])
				records = records[n:]
			}
		}
		if i%7 != 3 {
			add(v2part.EnvelopeKey(base), []byte(fmt.Sprintf("envelope-%d-%s", i, bytes.Repeat([]byte{'x'}, i%40))), nil)
		}
	}
	slices.SortFunc(rows, func(a, b headerKV) int { return bytes.Compare(a.key, b.key) })
	return spec, rows
}

type headerWriteMode int

const (
	headerEncoded headerWriteMode = iota // SetEncoded singletons (portable, conversion)
	headerPlanned                        // SetPlannedRecord + SetPlannedMetadata (seals)
	headerCopied                         // SetSlotsEncoded whole runs (merges)
)

func writeHeaderFixture(t testing.TB, path string, spec v2part.Spec, rows []headerKV, mode headerWriteMode, legacy bool) v2part.Descriptor {
	t.Helper()
	v2part.LegacyEnvelopeKV.Store(legacy)
	defer v2part.LegacyEnvelopeKV.Store(false)
	w, err := v2part.NewDenseWriter(path, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	for _, row := range rows {
		switch {
		case mode == headerPlanned && row.k.Archive == v2part.MetadataArchive:
			err = w.SetPlannedMetadata(row.k.VShard, 0, row.k.Metric, row.value)
		case mode == headerPlanned:
			for i := range row.runs {
				if err == nil {
					err = w.SetPlannedRecord(row.k.VShard, 0, row.k.Metric, &row.runs[i])
				}
			}
		case mode == headerCopied && row.k.Archive != v2part.MetadataArchive:
			err = w.SetSlotsEncoded(row.key, row.value)
		case row.k.Archive == v2part.MetadataArchive:
			err = w.SetEncoded(row.key, row.value)
		default:
			for _, r := range row.runs {
				k := row.k
				k.BlockStart = r.Slot
				key, _ := v2part.Encode(k)
				if err == nil {
					err = w.SetEncoded(key, sealrollup.EncodeRecord(r))
				}
			}
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	d, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func readLogical(t testing.TB, path string, d *v2part.Descriptor) (keys, values [][]byte) {
	t.Helper()
	err := v2part.Read(context.Background(), path, d, func(k v2part.Key, v []byte) error {
		key, err := v2part.Encode(k)
		keys, values = append(keys, key), append(values, bytes.Clone(v))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return keys, values
}

func openHeaderCursor(t testing.TB, path string, wrap bool) archiveCursor {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		t.Fatal(err)
	}
	r, err := sstable.NewReader(readable, sstable.ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	it, err := r.NewIter(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var c archiveCursor = &tableCursor{it: it}
	if wrap {
		c = newHeaderCursor(c, nil)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func cursorAt(c archiveCursor, valid bool) string {
	if !valid {
		return fmt.Sprintf("invalid %v", c.Error())
	}
	return fmt.Sprintf("%x=%x", c.Key(), c.Value())
}

// TestHeaderInRunDifferential proves a part with envelopes folded into each
// name's last run reads exactly like the part with separate envelope KVs:
// the same logical stream through v2part.Read, and the same answer from every
// cursor move readers use, for every write path, with one KV fewer per name
// that has both runs and an envelope.
func TestHeaderInRunDifferential(t *testing.T) {
	spec, rows := headerFixture(t, 300)
	folded := 0
	for i, row := range rows {
		if row.k.Archive == v2part.MetadataArchive && i > 0 && rows[i-1].k.Metric == row.k.Metric {
			folded++
		}
	}
	dir := t.TempDir()
	legacyPath := filepath.Join(dir, "legacy.sst")
	legacy := writeHeaderFixture(t, legacyPath, spec, rows, headerEncoded, true)
	wantKeys, wantValues := readLogical(t, legacyPath, &legacy)
	if len(wantKeys) != int(legacy.Coverage.Records) {
		t.Fatalf("legacy logical records %d, physical %d", len(wantKeys), legacy.Coverage.Records)
	}
	for _, mode := range []headerWriteMode{headerEncoded, headerPlanned, headerCopied} {
		path := filepath.Join(dir, fmt.Sprintf("folded%d.sst", mode))
		d := writeHeaderFixture(t, path, spec, rows, mode, false)
		if got, want := d.Coverage.Records, legacy.Coverage.Records-uint64(folded); got != want || folded == 0 {
			t.Fatalf("mode %d: physical records %d, want %d (%d folded)", mode, got, want, folded)
		}
		if d.Coverage.Names != legacy.Coverage.Names {
			t.Fatalf("mode %d: names %d, want %d", mode, d.Coverage.Names, legacy.Coverage.Names)
		}
		keys, values := readLogical(t, path, &d)
		if !slices.EqualFunc(keys, wantKeys, bytes.Equal) || !slices.EqualFunc(values, wantValues, bytes.Equal) {
			t.Fatalf("mode %d: logical stream differs from the separate-envelope part", mode)
		}
		want, got := openHeaderCursor(t, legacyPath, false), openHeaderCursor(t, path, true)
		var seq []string
		for ok := want.First(); ok; ok = want.Next() {
			seq = append(seq, cursorAt(want, true))
		}
		at := 0
		for ok := got.First(); ok; ok = got.Next() {
			if at >= len(seq) || cursorAt(got, true) != seq[at] {
				t.Fatalf("mode %d: scan differs at %d", mode, at)
			}
			at++
		}
		if at != len(seq) || got.Error() != nil {
			t.Fatalf("mode %d: scan stopped at %d of %d: %v", mode, at, len(seq), got.Error())
		}
		for _, k := range wantKeys {
			for _, probe := range [][]byte{k, append(bytes.Clone(k), 0), k[:len(k)-1], k[:len(k)-16]} {
				if w, g := cursorAt(want, want.SeekGE(probe)), cursorAt(got, got.SeekGE(probe)); w != g {
					t.Fatalf("mode %d: SeekGE(%x) = %s, want %s", mode, probe, g, w)
				}
				if got.Valid() && got.Next() != want.Next() || got.Valid() && cursorAt(got, true) != cursorAt(want, true) {
					t.Fatalf("mode %d: Next after SeekGE(%x) differs", mode, probe)
				}
				if w, g := cursorAt(want, want.SeekLT(probe)), cursorAt(got, got.SeekLT(probe)); w != g {
					t.Fatalf("mode %d: SeekLT(%x) = %s, want %s", mode, probe, g, w)
				}
			}
		}
		t.Logf("mode %d: physical records %d -> %d, bytes %d -> %d", mode, legacy.Coverage.Records, d.Coverage.Records, legacy.Bytes, d.Bytes)
	}
}

// TestHeaderInRunLegacyPartsMergeToFolded shows compat: a merge-style copy of
// an old part (separate envelopes) writes the folded form with the same
// logical stream, so merges retire the old form.
func TestHeaderInRunLegacyPartsMergeToFolded(t *testing.T) {
	spec, rows := headerFixture(t, 60)
	dir := t.TempDir()
	oldPath := filepath.Join(dir, "old.sst")
	old := writeHeaderFixture(t, oldPath, spec, rows, headerCopied, true)
	c := openHeaderCursor(t, oldPath, true)
	var copied []headerKV
	for ok := c.First(); ok; ok = c.Next() {
		k, err := v2part.Decode(c.Key())
		if err != nil {
			t.Fatal(err)
		}
		copied = append(copied, headerKV{key: bytes.Clone(c.Key()), value: bytes.Clone(c.Value()), k: k})
	}
	newPath := filepath.Join(dir, "new.sst")
	merged := writeHeaderFixture(t, newPath, spec, copied, headerCopied, false)
	if merged.Coverage.Records >= old.Coverage.Records {
		t.Fatalf("merge kept separate envelopes: %d >= %d records", merged.Coverage.Records, old.Coverage.Records)
	}
	wantKeys, wantValues := readLogical(t, oldPath, &old)
	keys, values := readLogical(t, newPath, &merged)
	if !slices.EqualFunc(keys, wantKeys, bytes.Equal) || !slices.EqualFunc(values, wantValues, bytes.Equal) {
		t.Fatal("merged logical stream differs")
	}
}

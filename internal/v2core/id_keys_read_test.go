package v2core

import (
	"bytes"
	"fmt"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2dict"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// idHeaderFixture is headerFixture with ID keys: each name gets a dictionary
// ID in reverse name order, so ID order differs from name order.
func idHeaderFixture(t testing.TB, names int) (v2part.Spec, []headerKV, map[v2part.ID]uint32) {
	t.Helper()
	spec, rows := headerFixture(t, names)
	var order []v2part.ID
	for _, row := range rows {
		if !slices.Contains(order, row.k.Metric) {
			order = append(order, row.k.Metric)
		}
	}
	ids := map[v2part.ID]uint32{}
	for i, name := range order {
		ids[name] = uint32(len(order) - 1 - i)
	}
	for i := range rows {
		k := rows[i].k
		k.Metric, k.ByID, k.Local = "", true, ids[rows[i].k.Metric]
		key, err := v2part.Encode(k)
		if err != nil {
			t.Fatal(err)
		}
		rows[i].k, rows[i].key = k, key
	}
	slices.SortFunc(rows, func(a, b headerKV) int { return bytes.Compare(a.key, b.key) })
	return spec, rows, ids
}

func writeIDHeaderFixture(t testing.TB, path string, spec v2part.Spec, rows []headerKV, copied, legacy bool) v2part.Descriptor {
	t.Helper()
	v2part.LegacyEnvelopeKV.Store(legacy)
	defer v2part.LegacyEnvelopeKV.Store(false)
	w, err := v2part.NewDenseWriter(path, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	if err = w.IDKeyed(1); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		switch {
		case row.k.Archive == v2part.MetadataArchive:
			err = w.SetEncoded(row.key, row.value)
		case copied:
			err = w.SetSlotsEncoded(row.key, row.value)
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

// TestHeaderInRunIDKeys is TestHeaderInRunDifferential for ID-keyed parts:
// with envelopes folded into each name's last run, the header cursor answers
// every scan and seek exactly like the part with separate envelope KVs.
// Without GCI2 support in headerCursor the folded envelopes are invisible.
func TestHeaderInRunIDKeys(t *testing.T) {
	v2part.IDKeys.Store(true)
	defer v2part.IDKeys.Store(false)
	spec, rows, _ := idHeaderFixture(t, 300)
	dir := t.TempDir()
	legacyPath := filepath.Join(dir, "legacy.sst")
	legacy := writeIDHeaderFixture(t, legacyPath, spec, rows, false, true)
	for _, copied := range []bool{false, true} {
		path := filepath.Join(dir, fmt.Sprintf("folded-%v.sst", copied))
		d := writeIDHeaderFixture(t, path, spec, rows, copied, false)
		if d.Coverage.Records >= legacy.Coverage.Records || d.DictGeneration != 1 {
			t.Fatalf("copied=%v: physical records %d, legacy %d, generation %d: nothing folded", copied, d.Coverage.Records, legacy.Coverage.Records, d.DictGeneration)
		}
		want, got := openHeaderCursor(t, legacyPath, false), openHeaderCursor(t, path, true)
		var seq []string
		var keys [][]byte
		for ok := want.First(); ok; ok = want.Next() {
			seq = append(seq, cursorAt(want, true))
			keys = append(keys, bytes.Clone(want.Key()))
		}
		at := 0
		for ok := got.First(); ok; ok = got.Next() {
			if at >= len(seq) || cursorAt(got, true) != seq[at] {
				t.Fatalf("copied=%v: scan differs at %d", copied, at)
			}
			at++
		}
		if at != len(seq) || got.Error() != nil {
			t.Fatalf("copied=%v: scan stopped at %d of %d: %v", copied, at, len(seq), got.Error())
		}
		for _, k := range keys {
			for _, probe := range [][]byte{k, append(bytes.Clone(k), 0), k[:len(k)-1], k[:len(k)-5]} {
				if w, g := cursorAt(want, want.SeekGE(probe)), cursorAt(got, got.SeekGE(probe)); w != g {
					t.Fatalf("copied=%v: SeekGE(%x) = %s, want %s", copied, probe, g, w)
				}
				if w, g := cursorAt(want, want.SeekLT(probe)), cursorAt(got, got.SeekLT(probe)); w != g {
					t.Fatalf("copied=%v: SeekLT(%x) = %s, want %s", copied, probe, g, w)
				}
			}
		}
	}
}

// TestIDKeyReadHelpers checks the tail helpers against the key codec for
// both generations, and that name <-> ID resolution goes through the
// dictionary once per metric.
func TestIDKeyReadHelpers(t *testing.T) {
	name := v2part.ID("helpers.metric.one")
	base := v2part.Key{Group: "g", VShard: v2part.Route(name), Partition: 1700000000, Part: v2part.PartID{7}, Archive: 2, BlockStart: 99}
	dict, err := v2dict.Open(t.TempDir(), v2dict.Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer dict.Close()
	if _, _, err = dict.Append("g", base.VShard, []byte("helpers.metric.zero"), 0); err != nil {
		t.Fatal(err)
	}
	e, _, err := dict.Append("g", base.VShard, []byte(name), 0)
	if err != nil || e.ID != 1 {
		t.Fatalf("append: %v %v", e, err)
	}
	for _, byID := range []bool{false, true} {
		k := base
		k.ByID = byID
		ids := metricIDs{dict: dict}
		k, ok, err := ids.key(k, name)
		if err != nil || !ok || (byID && (k.Local != 1 || k.Metric != "")) || (!byID && k.Metric != name) {
			t.Fatalf("byID=%v: metric key %+v %v %v", byID, k, ok, err)
		}
		ids.dict = nil // a second part of the same route reuses the lookup
		if again, ok, err := ids.key(k, name); err != nil || !ok || again != k {
			t.Fatalf("byID=%v: cached key %+v %v %v", byID, again, ok, err)
		}
		encoded, err := v2part.Encode(k)
		if err != nil {
			t.Fatal(err)
		}
		tail := partKeyTailOf(encoded)
		if tail != partKeyTail(byID) || isEnvelopeKey(encoded, tail) {
			t.Fatalf("byID=%v: tail %d", byID, tail)
		}
		env, err := v2part.Encode(v2part.EnvelopeKey(k))
		if err != nil {
			t.Fatal(err)
		}
		if got := appendEnvelopeKey(nil, encoded, tail); !bytes.Equal(got, env) || !isEnvelopeKey(env, tail) {
			t.Fatalf("byID=%v: envelope key %x, want %x", byID, got, env)
		}
		other := k
		other.Archive, other.BlockStart = 3, 0
		next := k
		next.BlockStart = 100
		a, _ := v2part.Encode(other)
		b, _ := v2part.Encode(next)
		n := archivePrefixLen(encoded, byID)
		if bytes.HasPrefix(a, encoded[:n]) || !bytes.HasPrefix(b, encoded[:n]) {
			t.Fatalf("byID=%v: archive prefix length %d", byID, n)
		}
		decoded, err := v2part.DecodeKey(encoded)
		if err != nil {
			t.Fatal(err)
		}
		names := keyNames{dict: dict}
		if err = names.resolve(&decoded); err != nil || decoded.Metric != name {
			t.Fatalf("byID=%v: resolved %q %v", byID, decoded.Metric, err)
		}
	}
	// An unknown name has no ID: no ID-keyed part holds it.
	if _, ok, err := metricKey(v2part.Key{Group: "g", VShard: base.VShard, ByID: true}, "helpers.metric.unknown", dict); ok || err != nil {
		t.Fatalf("unknown name: %v %v", ok, err)
	}
	if _, _, err := metricKey(v2part.Key{ByID: true}, name, nil); err == nil {
		t.Fatal("ID-keyed key without a dictionary")
	}
}

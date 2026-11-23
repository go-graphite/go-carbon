package v2core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

type moveFixture struct {
	path       string
	descriptor v2part.Descriptor
}

func movePart(t *testing.T, source byte, id, order uint64, samples ...sealrollup.Sample) moveFixture {
	t.Helper()
	policy := RollupPolicy{Name: "move.metric", Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 100}}}}
	encoded, err := EncodeRollupPolicy(policy)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]Row, len(samples))
	for i, p := range samples {
		rows[i] = Row{ID: id, Time: p.Time, Bits: p.Bits, Seq: uint64(i + 1)}
	}
	r := newCoreRollups()
	plan, err := r.plan(coreSealInput{Batches: []Batch{{ID: id, Order: order, Clock: 1010, Policy: encoded, Points: rows}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Files) != 1 {
		t.Fatalf("files %d", len(plan.Files))
	}
	path := filepath.Join(t.TempDir(), "part.sst")
	d, err := WriteDenseRollupPart(path, plan.Files[0], v2part.Provenance{Incarnation: v2part.PartID{source}, Stream: "producer-0"})
	if err != nil {
		t.Fatal(err)
	}
	return moveFixture{path, d}
}
func ingestLiveMovePart(t *testing.T, db *pebble.DB, f moveFixture) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "live.sst")
	if e := os.Link(f.path, path); e != nil {
		t.Fatal(e)
	}
	if e := db.Ingest([]string{path}); e != nil {
		t.Fatal(e)
	}
}
func moveRaw(t *testing.T, index *RollupSnapshot) map[int64]uint64 {
	t.Helper()
	out := map[int64]uint64{}
	e := index.ScanArchive(context.Background(), "move.metric", 0, 0, 100, func(r sealrollup.Record) error { out[r.Sample.Time] = r.Sample.Bits; return nil })
	if e != nil {
		t.Fatal(e)
	}
	return out
}
func moveIndex(t *testing.T, db *pebble.DB, prior *RollupSnapshot) *RollupSnapshot {
	t.Helper()
	snap := db.NewSnapshot()
	t.Cleanup(func() { _ = snap.Close() })
	index, e := NewRollupSnapshot(context.Background(), snap, prior, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	return index
}
func moveSample(time int64, value float64) sealrollup.Sample {
	return sealrollup.Sample{Time: time, Bits: math.Float64bits(value)}
}

func TestMoveCachedCopiedOnlyBecomesLive(t *testing.T) {
	dir := t.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	donor := movePart(t, 1, 99, 100, moveSample(1000, 1))
	if _, err = InstallCopiedPart(context.Background(), db, dir, donor.path, donor.descriptor); err != nil {
		t.Fatal(err)
	}
	before := moveIndex(t, db, nil)
	if moveRaw(t, before)[1000] != math.Float64bits(1) {
		t.Fatal("missing donor-only cache fixture")
	}
	ingestLiveMovePart(t, db, movePart(t, 2, 7, 1, moveSample(1000, 2)))
	if moveRaw(t, moveIndex(t, db, before))[1000] != math.Float64bits(2) {
		t.Fatal("copied-only cached generation masked live intake")
	}
	if moveRaw(t, before)[1000] != math.Float64bits(1) {
		t.Fatal("old copied-only snapshot changed")
	}
}

func TestMoveCatalogCopiedPrecedenceAndRepair(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, e := pebble.Open(dir, &pebble.Options{})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = db.Close() })
	// Independent IDs and radically different counters must not decide precedence.
	stale := movePart(t, 1, 99, 100, moveSample(1000, 1))
	live := movePart(t, 2, 7, 1, moveSample(1000, 2))
	ingestLiveMovePart(t, db, live)
	before := moveIndex(t, db, nil)
	if moveRaw(t, before)[1000] != math.Float64bits(2) {
		t.Fatal("bad live fixture")
	}
	install := func(f moveFixture) {
		t.Helper()
		b, e := os.ReadFile(f.path)
		if e != nil {
			t.Fatal(e)
		}
		old := sha256.Sum256(b)
		added, e := InstallCopiedPart(ctx, db, dir, f.path, f.descriptor)
		if e != nil || !added {
			t.Fatalf("install %v %v", added, e)
		}
		b, e = os.ReadFile(f.path)
		if e != nil || sha256.Sum256(b) != old {
			t.Fatal("copied bytes changed")
		}
		added, e = InstallCopiedPart(ctx, db, dir, f.path, f.descriptor)
		if e != nil || added {
			t.Fatalf("retry %v %v", added, e)
		}
	}
	install(stale)
	after := moveIndex(t, db, before)
	if moveRaw(t, after)[1000] != math.Float64bits(2) {
		t.Fatal("stale copy rolled B back")
	}
	missing := movePart(t, 1, 99, 101, moveSample(1001, 3), sealrollup.Sample{Time: 1002, Bits: 0x7ff8000000000042}, sealrollup.Sample{Time: 1003, Bits: 1 << 63})
	install(missing)
	repaired := moveIndex(t, db, after)
	got := moveRaw(t, repaired)
	if got[1001] != math.Float64bits(3) || got[1002] != 0x7ff8000000000042 || got[1003] != 1<<63 {
		t.Fatalf("fresh-slot repair/bits %x", got)
	}
	// A dropped overwrite of an occupied slot deliberately remains as B saw it.
	correction := movePart(t, 1, 99, 102, moveSample(1000, 3))
	install(correction)
	if moveRaw(t, moveIndex(t, db, repaired))[1000] != math.Float64bits(2) {
		t.Fatal("drop semantics changed")
	}
	ingestLiveMovePart(t, db, movePart(t, 2, 7, 2, moveSample(1000, 4)))
	late := movePart(t, 1, 99, 103, moveSample(1000, 3))
	install(late)
	newest := moveIndex(t, db, repaired)
	got = moveRaw(t, newest)
	if got[1000] != math.Float64bits(4) {
		t.Fatal("later copy rolled B-only intake back")
	}
	if moveRaw(t, before)[1000] != math.Float64bits(2) {
		t.Fatal("pinned snapshot changed")
	}
	if e := db.Compact([]byte{'G'}, []byte{'H'}, true); e != nil {
		t.Fatal(e)
	}
	compacted := moveIndex(t, db, nil)
	if moveRaw(t, compacted)[1000] != math.Float64bits(4) {
		t.Fatal("compaction lost precedence")
	}
	snap := db.NewSnapshot()
	defer snap.Close()
	count := 0
	if e := ScanRollups(snap, func(_ RollupPolicy, m RollupMetadata, rs []sealrollup.Record) error {
		count++
		if m.ID != 7 || m.Frontier.Order != 2 {
			t.Fatalf("metadata compared donor order: %+v", m)
		}
		for _, r := range rs {
			if r.Archive == 0 && got[r.Sample.Time] != r.Sample.Bits {
				t.Fatal("scan/seek disagree")
			}
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if count != 1 {
		t.Fatalf("metrics %d", count)
	}
	if _, _, e := recoverCoreRollups(db); e == nil || !strings.Contains(e.Error(), "adoption") {
		t.Fatalf("unadopted writable recovery allowed: %v", e)
	}
}

func TestMoveCatalogRejectsCorruptionAndReclassification(t *testing.T) {
	dir := t.TempDir()
	db, e := pebble.Open(dir, &pebble.Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	ctx := context.Background()
	f := movePart(t, 1, 1, 1, moveSample(1000, 1))
	ingestLiveMovePart(t, db, f)
	if _, e = InstallCopiedPart(ctx, db, dir, f.path, f.descriptor); e == nil {
		t.Fatal("reclassified live bytes")
	}
	bad := movePart(t, 1, 1, 2, moveSample(1001, 2))
	body, _ := os.ReadFile(bad.path)
	body[len(body)/2] ^= 1
	if e = os.WriteFile(bad.path, body, 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = InstallCopiedPart(ctx, db, dir, bad.path, bad.descriptor); e == nil {
		t.Fatal("accepted corrupt bytes")
	}
	found, e := hasCopiedParts(db)
	if e != nil || found {
		t.Fatalf("failed copy published catalog: %v %v", found, e)
	}
}

func TestMoveCatalogRejectsUnrelatedDonorOrders(t *testing.T) {
	dir := t.TempDir()
	db, e := pebble.Open(dir, &pebble.Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	for _, source := range []byte{1, 3} {
		f := movePart(t, source, 1, 100, moveSample(1000, float64(source)))
		if _, e = InstallCopiedPart(context.Background(), db, dir, f.path, f.descriptor); e != nil {
			t.Fatal(e)
		}
	}
	snap := db.NewSnapshot()
	defer snap.Close()
	_, e = NewRollupSnapshot(context.Background(), snap, nil, nil, nil)
	if e == nil || !strings.Contains(e.Error(), "incomparable") {
		t.Fatalf("compared unrelated source orders: %v", e)
	}
}

func TestMoveCatalogPersistedBindingIsExact(t *testing.T) {
	dir := t.TempDir()
	db, e := pebble.Open(dir, &pebble.Options{})
	if e != nil {
		t.Fatal(e)
	}
	f := movePart(t, 1, 1, 1, moveSample(1000, 1))
	if _, e = InstallCopiedPart(context.Background(), db, dir, f.path, f.descriptor); e != nil {
		t.Fatal(e)
	}
	if e = db.Close(); e != nil {
		t.Fatal(e)
	}
	db, e = pebble.Open(dir, &pebble.Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	snap := db.NewSnapshot()
	defer snap.Close()
	index, e := NewRollupSnapshot(context.Background(), snap, nil, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	if moveRaw(t, index)[1000] != math.Float64bits(1) {
		t.Fatal("restart lost copy")
	}
	altered := f.descriptor
	altered.SHA256[0] ^= 1
	if _, e = InstallCopiedPart(context.Background(), db, dir, f.path, altered); e == nil {
		t.Fatal("same UUID accepted different checksum")
	}
	it, e := db.NewIter(&pebble.IterOptions{LowerBound: movePartPrefix, UpperBound: v2part.PrefixEnd(movePartPrefix)})
	if e != nil {
		t.Fatal(e)
	}
	defer it.Close()
	if !it.First() {
		t.Fatal("missing binding")
	}
	var binding partImport
	if e = json.Unmarshal(it.Value(), &binding); e != nil {
		t.Fatal(e)
	}
	b, _ := binding.Descriptor.MarshalBinary()
	want, _ := f.descriptor.MarshalBinary()
	if !bytes.Equal(b, want) {
		t.Fatal("descriptor changed")
	}
}

func TestMoveCrossBoundaryRollupRebuild(t *testing.T) {
	for _, method := range []sealrollup.Method{sealrollup.Sum, sealrollup.Average, sealrollup.Last, sealrollup.First, sealrollup.Min, sealrollup.Max} {
		t.Run(string(rune('0'+method)), func(t *testing.T) {
			policy := RollupPolicy{Name: "move.metric", Group: "test", Config: sealrollup.Config{Method: method, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 100}, {Step: 10, Count: 100}}}}
			// A has the prefix, B the suffix, so neither partial coarse value is correct.
			all := []sealrollup.Record{}
			for i := int64(0); i < 10; i++ {
				p := moveSample(1000+i, float64(i+1))
				order := uint64(100)
				if i >= 5 {
					order = 1
				}
				p.Sequence = order
				all = append(all, sealrollup.Record{Delta: sealrollup.Delta{Archive: 0, Slot: p.Time % 100, Order: order, Sample: p}})
			}
			// This intentionally wrong existing partial must be replaced, not accumulated.
			all = append(all, sealrollup.Record{Delta: sealrollup.Delta{Archive: 1, Slot: 0, Order: 1, Sample: moveSample(1000, 40)}, Kind: sealrollup.KindMat})
			var versions []sealrollup.Record
			for _, r := range all {
				var binding *partImport
				if r.Order == 100 {
					binding = &partImport{BaseOrder: 100, Descriptor: v2part.Descriptor{Source: v2part.Provenance{MinOrder: 100}}}
				}
				mapped, err := moveVersionRecord(r, binding)
				if err != nil {
					t.Fatal(err)
				}
				versions = append(versions, mapped)
			}
			var got []sealrollup.Record
			meta := metricMetadata{Version: 3}
			meta.Frontier.Order, meta.Frontier.MaxTime = 1, 1009
			err := windowRecords(policy.Config, meta, 1, 0, 100, nil, func(l int, first, last int64, from uint64) ([]sealrollup.Version, error) {
				var out []sealrollup.Version
				for _, r := range versions {
					if r.Archive == l && r.Slot >= first && r.Slot < last {
						out = append(out, r.Version())
					}
				}
				return out, nil
			}, func(r sealrollup.Record) error { got = append(got, r); return nil }, true)
			if err != nil {
				t.Fatal(err)
			}
			expectedBits := map[sealrollup.Method]uint64{sealrollup.Sum: math.Float64bits(55), sealrollup.Average: math.Float64bits(5.5), sealrollup.Last: math.Float64bits(10), sealrollup.First: math.Float64bits(1), sealrollup.Min: math.Float64bits(1), sealrollup.Max: math.Float64bits(10)}[method]

			count := 0
			for _, r := range got {
				if r.Archive == 0 {
					orig := all[int(r.Sample.Time-1000)]
					if r != orig {
						t.Fatal("rebuild changed winning raw bits/order")
					}
				} else {
					count++
					if r.Sample.Time != 1000 || r.Slot != 0 || r.Sample.Bits != expectedBits || r.Order != moveLiveOrder+1 {
						t.Fatalf("coarse mismatch got %+v want bits %x", r, expectedBits)
					}
				}
			}
			if count != 1 {
				t.Fatalf("coarse count %d", count)
			}
		})
	}
}

func hasCopiedParts(db *pebble.DB) (bool, error) {
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: movePartPrefix, UpperBound: v2part.PrefixEnd(movePartPrefix)})
	if err != nil {
		return false, err
	}
	found := it.First()
	return found, errors.Join(it.Error(), it.Close())
}

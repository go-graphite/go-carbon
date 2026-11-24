package v2core

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func TestRollupSnapshotSeeksAndReusesImmutableParts(t *testing.T) {
	for _, dense := range []bool{false, true} {
		t.Run(fmt.Sprint(dense), func(t *testing.T) {
			db, err := pebble.Open(t.TempDir(), &pebble.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx := context.Background()
			policy := RollupPolicy{Name: "seek.metric", Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 100}, {Step: 10, Count: 100}}}}
			encoded, err := EncodeRollupPolicy(policy)
			if err != nil {
				t.Fatal(err)
			}
			digest, err := sealrollup.PolicyDigest(policy.Config)
			if err != nil {
				t.Fatal(err)
			}
			put := func(part byte, archive uint32, slot int64, value []byte) {
				var id v2part.PartID
				id[0] = part
				k, err := v2part.Encode(v2part.Key{Group: policy.Group, VShard: v2part.Route(policy.Name), Part: id, Metric: policy.Name, Archive: archive, BlockStart: slot})
				if err != nil {
					t.Fatal(err)
				}
				if err = db.Set(k, value, pebble.NoSync); err != nil {
					t.Fatal(err)
				}
			}
			add := func(part byte, order uint64) {
				meta := RollupMetadata{Version: 1, ID: 1, Policy: encoded, Frontier: sealrollup.Frontier{Order: order, Policy: digest}}
				b, err := json.Marshal(meta)
				if err != nil {
					t.Fatal(err)
				}
				put(part, v2part.MetadataArchive, 0, b)
				for archive := range policy.Config.Archives {
					rs := []sealrollup.Record{}
					for slot := int64(10); slot < 20; slot++ {
						r, err := sealrollup.DecodeRecord(archive, slot, sealrollup.EncodeRecord(sealrollup.Record{Delta: sealrollup.Delta{Order: order, Archive: archive, Slot: slot, Sample: sealrollup.Sample{Time: (slot + 100) * policy.Config.Archives[archive].Step, Bits: math.Float64bits(float64(order)), Sequence: order}}}))
						if err != nil {
							t.Fatal(err)
						}
						rs = append(rs, r)
					}
					if dense {
						b, err := v2part.EncodeSlots(rs)
						if err != nil {
							t.Fatal(err)
						}
						put(part, uint32(archive), 10, b)
					} else {
						for _, r := range rs {
							put(part, uint32(archive), r.Slot, sealrollup.EncodeRecord(r))
						}
					}
				}
			}
			add(1, 1)
			old := db.NewSnapshot()
			defer old.Close()
			prior, err := NewRollupSnapshot(ctx, old, nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			warm, err := prior.Records(ctx, string(policy.Name))
			if err != nil || len(warm) != 20 {
				t.Fatalf("warm prior %d %v", len(warm), err)
			}
			add(2, 2)
			current := db.NewSnapshot()
			defer current.Close()
			index, err := NewRollupSnapshot(ctx, current, prior, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(prior.metricPartIDs(prior.metrics[string(policy.Name)])) != 1 || len(index.metricPartIDs(index.metrics[string(policy.Name)])) != 2 {
				t.Fatal("extending a directory changed an older pin's part authority")
			}
			for _, view := range []struct {
				index *RollupSnapshot
				order uint64
			}{{prior, 1}, {index, 2}} {
				p, m, ok := view.index.LookupMetadata(string(policy.Name))
				if !ok || p.Name != policy.Name || m.ID != 1 || m.Frontier.Order != view.order || string(m.Policy) != string(encoded) {
					t.Fatal("metadata lookup differs from its pinned publication")
				}
				if _, _, ok := view.index.LookupMetadata("absent"); ok {
					t.Fatal("absent metadata exists")
				}
			}
			for key, part := range prior.parts {
				if index.parts[key] != part {
					t.Fatal("immutable part metadata was not reused")
				}
			}

			same, err := NewRollupSnapshot(ctx, current, index, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if same.metrics[string(policy.Name)] != index.metrics[string(policy.Name)] {
				t.Fatal("unchanged inventory rebuilt metric directory")
			}
			if len(same.metricPartIDs(same.metrics[string(policy.Name)])) != len(same.metricParts(string(policy.Name), same.metrics[string(policy.Name)])) {
				t.Fatal("snapshot part membership is incomplete")
			}
			all, err := index.Records(ctx, string(policy.Name))
			if err != nil {
				t.Fatal(err)
			}
			// Distinct subranges must reuse the full immutable archive result,
			// including its newly appended part, without filling the range cache
			// with duplicate subsets or opening another archive iterator.
			cachedEntries := index.cache.count
			cachedOnly := *index
			cachedOnly.snapshot = nil // a complete cache hit needs no storage access
			for archive := range policy.Config.Archives {
				for first := int64(0); first < 20; first++ {
					last := first + 3
					var subset []sealrollup.Record
					err = cachedOnly.ScanArchive(ctx, string(policy.Name), archive, first, last, func(r sealrollup.Record) error {
						subset = append(subset, r)
						return nil
					})
					if err != nil {
						t.Fatal(err)
					}
					var expected []sealrollup.Record
					for _, r := range all {
						if r.Archive == archive && r.Slot >= first && r.Slot < last {
							expected = append(expected, r)
						}
					}
					slices.SortFunc(subset, func(a, b sealrollup.Record) int { return int(a.Slot - b.Slot) })
					slices.SortFunc(expected, func(a, b sealrollup.Record) int { return int(a.Slot - b.Slot) })
					if !slices.Equal(subset, expected) || index.cache.count != cachedEntries {
						t.Fatal("cached subrange changed winners or retained duplicate records")
					}
				}
			}
			less := func(a, b sealrollup.Record) int {
				if a.Archive != b.Archive {
					return a.Archive - b.Archive
				}
				return int(a.Slot - b.Slot)
			}
			var want []sealrollup.Record
			err = ScanRollups(current, func(_ RollupPolicy, _ RollupMetadata, r []sealrollup.Record) error { want = r; return nil })
			if err != nil {
				t.Fatal(err)
			}
			slices.SortFunc(all, less)
			if !slices.Equal(all, want) {
				t.Fatal("full state differs from ScanRollups")
			}
			// Query begins inside a dense run. Physical winners carry newer epochs;
			// filtering event timestamps here would resurrect overwritten ring slots.
			var got []sealrollup.Record
			err = index.ScanArchive(ctx, string(policy.Name), 0, 13, 16, func(r sealrollup.Record) error { got = append(got, r); return nil })
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 3 {
				t.Fatalf("range decoded %d records, want 3 winners", len(got))
			}
			for _, r := range got {
				if r.Archive != 0 || r.Slot < 13 || r.Slot >= 16 || r.Sample.Time <= 100 {
					t.Fatal("wrong physical range")
				}
			}
			// Reusing a newer directory's cache while loading an older pinned
			// snapshot must discard the removed part's winning replacements.
			rewind, err := NewRollupSnapshot(ctx, old, index, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			rewound, err := rewind.Records(ctx, string(policy.Name))
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range rewound {
				if r.Order != 1 {
					t.Fatal("cache resurrected a part absent from this snapshot")
				}
			}
			before, err := prior.Records(ctx, string(policy.Name))
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range before {
				if r.Order != 1 {
					t.Fatal("old snapshot observed new part")
				}
			}
			// A request pinned to the old generation must coexist with the new
			// generation in the shared cache rather than replacing its authority.
			index.cache.mu.Lock()
			if index.cache.count < 2 {
				t.Fatal("cache did not retain concurrent generations")
			}
			index.cache.mu.Unlock()
			for first := int64(0); first < 80; first++ {
				if err := index.ScanArchive(ctx, string(policy.Name), 0, first, first+1, func(sealrollup.Record) error { return nil }); err != nil {
					t.Fatal(err)
				}
			}
			index.cache.mu.Lock()
			if index.cache.count > rollupReadCacheEntries || index.cache.bytes > index.cache.limit {
				t.Fatal("unbounded range cache")
			}
			for _, entries := range index.cache.entries {
				if len(entries) > 8 {
					t.Fatal("unbounded generation history")
				}
			}
			index.cache.mu.Unlock()
			// Unrequested archive values must not be decoded by the range path or
			// metadata construction (the recovery/full scan correctly rejects them).
			put(2, 1, 10, []byte("bad unrequested value"))
			corrupt := db.NewSnapshot()
			defer corrupt.Close()
			sparse, err := NewRollupSnapshot(ctx, corrupt, nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			err = sparse.ScanArchive(ctx, string(policy.Name), 0, 13, 16, func(sealrollup.Record) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			err = sparse.ScanArchive(ctx, string(policy.Name), 1, 10, 11, func(sealrollup.Record) error { return nil })
			if err == nil {
				t.Fatal("requested corrupt archive was accepted")
			}
		})
	}
}

func (s *RollupSnapshot) metricPartIDs(m *snapshotMetric) map[string]struct{} {
	ids := map[string]struct{}{}
	for _, p := range s.metricParts(m.latest.Name(), m) {
		ids[p.part.id] = struct{}{}
	}
	return ids
}

// TestNamesMmapMixedLengthBounds exercises names whose encoded-key order differs
// from lexical range order in one virtual shard and one physical part.
func TestNamesMmapMixedLengthBounds(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	names := []string{"z", ""}
	for i := 0; ; i++ {
		candidate := fmt.Sprintf("a.long.metric.%d", i)
		if v2part.Route(v2part.ID(candidate)) == v2part.Route(v2part.ID(names[0])) {
			names[1] = candidate
			break
		}
	}
	var part v2part.PartID
	part[0] = 1
	for i, name := range names {
		policy := RollupPolicy{Name: v2part.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 100}}}}
		raw, err := EncodeRollupPolicy(policy)
		if err != nil {
			t.Fatal(err)
		}
		digest, err := sealrollup.PolicyDigest(policy.Config)
		if err != nil {
			t.Fatal(err)
		}
		meta, err := json.Marshal(RollupMetadata{Version: 1, ID: uint64(i + 1), Policy: raw, Frontier: sealrollup.Frontier{Order: 1, Policy: digest}})
		if err != nil {
			t.Fatal(err)
		}
		key := v2part.Key{Group: policy.Group, VShard: v2part.Route(policy.Name), Part: part, Metric: policy.Name, Archive: v2part.MetadataArchive}
		encoded, err := v2part.Encode(key)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Set(encoded, meta, pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		key.Archive = 0
		key.BlockStart = 10
		encoded, err = v2part.Encode(key)
		if err != nil {
			t.Fatal(err)
		}
		record := sealrollup.Record{Delta: sealrollup.Delta{Order: 1, Archive: 0, Slot: 10, Sample: sealrollup.Sample{Time: 110, Bits: math.Float64bits(float64(i + 42)), Sequence: 1}}}
		if err := db.Set(encoded, sealrollup.EncodeRecord(record), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := db.NewSnapshot()
	defer snapshot.Close()
	view, err := NewRollupSnapshot(context.Background(), snapshot, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		records, err := view.Records(context.Background(), name)
		if err != nil || len(records) != 1 {
			t.Fatalf("name=%q records=%d want=1 err=%v; encoded order cannot supply lexical bounds", name, len(records), err)
		}
	}
}

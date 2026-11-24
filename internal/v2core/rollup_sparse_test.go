package v2core

import (
	"math"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// Archive capacity must not allocate a memo entry (or a window) for every
// empty slot. This is the full-state path used by scans and pending reads.
func TestWindowRecordsSparseAllocation(t *testing.T) {
	c := sealrollup.Config{Method: sealrollup.Sum, Archives: []sealrollup.Archive{{Step: 1, Count: 100}, {Step: 10, Count: 100000}}}
	meta := metricMetadata{Version: 3}
	meta.Frontier.Order, meta.Frontier.MaxTime = 1, 1800000000
	v := sealrollup.Version{Archive: 1, Time: 1799999990, Bits: math.Float64bits(42), Order: 16}
	load := func(l int, first, last int64, _ uint64) ([]sealrollup.Version, error) {
		if l == v.Archive && (v.Time/10)%c.Archives[1].Count >= first && (v.Time/10)%c.Archives[1].Count < last {
			return []sealrollup.Version{v}, nil
		}
		return nil, nil
	}
	allocs := testing.AllocsPerRun(1, func() {
		var got []sealrollup.Record
		if err := windowRecords(c, meta, 1, 0, c.Archives[1].Count, nil, load, func(r sealrollup.Record) error { got = append(got, r); return nil }); err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].Sample.Time != v.Time || got[0].Sample.Bits != v.Bits {
			t.Fatalf("sparse scan: %+v", got)
		}
	})
	if allocs > 128 {
		t.Fatalf("sparse scan allocated %.0f objects for one occupied slot", allocs)
	}
	t.Logf("one occupied slot in 100000: %.0f allocations", allocs)
}

// Sparse resolution must retain the dense reader's physical-slot winners,
// including old epochs, ghosts, absence markers and partially requested rings.
func TestWindowRecordsSparseSlotDifferential(t *testing.T) {
	c := sealrollup.Config{Method: sealrollup.Sum, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 20}, {Step: 5, Count: 12}, {Step: 10, Count: 9}}}
	var versions []sealrollup.Version
	for i, ts := range []int64{1, 2, 3, 26, 27, 28, 61, 62, 63, 86, 87, 88} {
		versions = append(versions, sealrollup.Version{Time: ts, Bits: math.Float64bits(float64(i + 1)), Order: sealrollup.Tick(uint64(i+1), 0)})
	}
	versions = append(versions,
		sealrollup.Version{Archive: 1, Time: 5, Bits: math.Float64bits(17), Order: 1, Mat: true},
		sealrollup.Version{Archive: 1, Time: 65, Order: 16, Upto: 16, None: true},
		sealrollup.Version{Time: 89, Order: sealrollup.Tick(13, 0), Ghost: true},
	)
	for _, frontier := range []int64{3, 28, 88, 188} {
		meta := metricMetadata{Version: 3}
		meta.Frontier.Order, meta.Frontier.MaxTime = 13, frontier
		for a := 1; a < len(c.Archives); a++ {
			ar := c.Archives[a]
			load := func(l int, first, last int64, _ uint64) ([]sealrollup.Version, error) {
				var out []sealrollup.Version
				for _, v := range versions {
					slot := (v.Time / c.Archives[l].Step) % c.Archives[l].Count
					if v.Archive == l && slot >= first && slot < last {
						out = append(out, v)
					}
				}
				return out, nil
			}
			for first := int64(0); first < ar.Count; first++ {
				for last := first; last <= ar.Count; last++ {
					var got, want []sealrollup.Sample
					if err := windowRecords(c, meta, a, first, last, nil, load, func(r sealrollup.Record) error { got = append(got, r.Sample); return nil }); err != nil {
						t.Fatal(err)
					}
					// Build the original dense traversal's union of loaded versions,
					// then evaluate every requested slot, including empty ones.
					open := frontier - frontier%ar.Step
					phys := (open / ar.Step) % ar.Count
					var recs []sealrollup.Version
					seen := map[sealrollup.Version]bool{}
					var slots []int64
					for p := first; p < last; p++ {
						w := open - ((phys-p+ar.Count)%ar.Count)*ar.Step
						if w < 0 {
							continue
						}
						slots = append(slots, p)
						if err := sealrollup.WindowRanges(c, a, w, func(l int, lo, hi int64) error {
							rs, err := load(l, lo, hi, 0)
							for _, r := range rs {
								if !seen[r] {
									seen[r] = true
									recs = append(recs, r)
								}
							}
							return err
						}); err != nil {
							t.Fatal(err)
						}
					}
					oracle := sealrollup.NewVersions(c, recs)
					for _, p := range slots {
						if v, ok := oracle.SlotValue(a, p); ok {
							v.Sequence >>= 4
							want = append(want, v)
						}
					}
					if !slices.Equal(got, want) {
						t.Fatalf("frontier=%d archive=%d [%d,%d): got %+v want %+v", frontier, a, first, last, got, want)
					}
				}
			}
		}
	}
}

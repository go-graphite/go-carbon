package v2core

import (
	"cmp"
	"errors"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"sort"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// TestMergeVersionsDifferential compares complete ordered record output and
// errors with the pre-optimization implementation, including scratch reuse.
func TestMergeVersionsDifferential(t *testing.T) {
	rng := rand.New(rand.NewPCG(421, 919))
	var fast mergeVersions
	var reference referenceMergeVersions
	for trial := 0; trial < 1200; trial++ {
		c := sealrollup.Config{Archives: []sealrollup.Archive{{Step: 1, Count: 12}, {Step: 3, Count: 12}, {Step: 12, Count: 12}}, Method: sealrollup.Method(1 + rng.IntN(6)), XFilesFactor: float32(rng.IntN(4)) / 4}
		var recs []sealrollup.Record
		var from []int
		for input := 0; input < 1+rng.IntN(5); input++ {
			var run []sealrollup.Record
			for n := rng.IntN(24); n > 0; n-- {
				a := rng.IntN(len(c.Archives))
				ar := c.Archives[a]
				tm := int64(rng.IntN(90)) * ar.Step
				r := sealrollup.Record{Delta: sealrollup.Delta{Archive: a, Slot: tm / ar.Step % ar.Count, Order: uint64(1 + rng.IntN(20)), Sample: sealrollup.Sample{Time: tm, Bits: math.Float64bits(float64(rng.IntN(100)))}}}
				// Some cases isolate the no-drop path, others exercise every record kind.
				if trial%4 != 0 {
					r.Kind = sealrollup.Kind(rng.IntN(4))
					if r.Kind == sealrollup.KindMat && rng.IntN(2) == 0 {
						r.Upto = r.Order
					}
				}
				if trial%17 == 0 {
					r.Slot = (r.Slot + 1) % ar.Count
				}
				run = append(run, r)
			}
			slices.SortStableFunc(run, func(a, b sealrollup.Record) int {
				if mergeSlotLess(a, b) {
					return -1
				}
				if mergeSlotLess(b, a) {
					return 1
				}
				return 0
			})
			recs = append(recs, run...)
			for range run {
				from = append(from, input)
			}
		}
		meta := RollupMetadata{}
		if trial%7 == 0 {
			meta.ResetOrder = uint64(rng.IntN(15))
		}
		meta.Expired = trial%31 == 0
		retire := int64(0)
		if trial%3 == 0 {
			retire = int64(rng.IntN(200))
		}
		for _, prefix := range []bool{false, true} {
			fast.records, fast.from = recs, from
			reference.records, reference.from = recs, from
			got, err := fast.resolve(c, &meta, retire, prefix)
			want, werr := reference.resolve(c, &meta, retire, prefix)
			if (err == nil) != (werr == nil) || err != nil && err.Error() != werr.Error() || err == nil && !reflect.DeepEqual(got, want) {
				t.Fatalf("trial=%d prefix=%v retire=%d metadata=%+v\nrecords=%+v\nfrom=%v\ngot=%+v err=%v\nwant=%+v err=%v", trial, prefix, retire, meta, recs, from, got, err, want, werr)
			}
		}
	}
}

// TestMergeVersionsEncodedDifferential runs merge over encoded input runs
// (dense runs with and without kinds, singletons, several runs per input),
// including the header-only skip of names without versions, against the
// original implementation over the decoded records.
func TestMergeVersionsEncodedDifferential(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 1696))
	c := sealrollup.Config{Archives: []sealrollup.Archive{{Step: 1, Count: 12}, {Step: 3, Count: 12}, {Step: 12, Count: 12}}, Method: sealrollup.Average}
	fast := mergeVersions{out: make([]sealrollup.Record, 0, 64)}
	reference := referenceMergeVersions{out: make([]sealrollup.Record, 0, 64)}
	skipped, versioned := 0, 0
	for trial := 0; trial < 3000; trial++ {
		var members []*mergeCursor
		var recs []sealrollup.Record
		var from []int
		for input := 0; input < 1+rng.IntN(4); input++ {
			var run []sealrollup.Record
			for n := rng.IntN(16); n > 0; n-- {
				a := rng.IntN(len(c.Archives))
				slot := int64(rng.IntN(12))
				if trial%3 == 0 { // inputs of disjoint slots
					slot = int64(input*3 + rng.IntN(3))
				}
				r := sealrollup.Record{Delta: sealrollup.Delta{Archive: a, Slot: slot, Order: uint64(1 + rng.IntN(20)), Sample: sealrollup.Sample{Time: (12*int64(1+rng.IntN(6)) + slot) * c.Archives[a].Step, Bits: math.Float64bits(float64(rng.IntN(9)))}}}
				if trial%2 == 1 {
					r.Kind = sealrollup.Kind(rng.IntN(4))
					if r.Kind == sealrollup.KindMat && rng.IntN(2) == 0 {
						r.Upto = r.Order
					}
				}
				run = append(run, r)
			}
			slices.SortStableFunc(run, func(a, b sealrollup.Record) int {
				return cmp.Or(cmp.Compare(a.Archive, b.Archive), cmp.Compare(a.Slot, b.Slot))
			})
			if trial%2 == 0 { // plain runs: one Raw version per slot
				run = slices.CompactFunc(run, func(a, b sealrollup.Record) bool { return a.Archive == b.Archive && a.Slot == b.Slot })
			}
			cur := &mergeCursor{}
			for i := 0; i < len(run); {
				j := i + 1
				for j < len(run) && run[j].Archive == run[i].Archive && (run[j].Slot == run[j-1].Slot || rng.IntN(4) != 0) {
					j++
				}
				var b []byte
				var err error
				if j == i+1 && run[i].Kind == sealrollup.KindRaw && rng.IntN(2) == 0 {
					b = sealrollup.EncodeRecord(run[i])
				} else if b, err = v2part.EncodeSlots(run[i:j]); err != nil {
					t.Fatal(err)
				}
				cur.blocks = append(cur.blocks, mergeBlock{archive: uint32(run[i].Archive), slot: run[i].Slot, start: len(cur.arena), end: len(cur.arena) + len(b)})
				cur.arena = append(cur.arena, b...)
				decoded, err := v2part.DecodeRecordsInto(nil, uint32(run[i].Archive), run[i].Slot, b)
				if err != nil {
					t.Fatal(err)
				}
				// The merge reuses decoded Sequence for descriptor provenance;
				// these legacy cursors all have sequence zero.
				for i := range decoded {
					decoded[i].Sample.Sequence = cur.seq
				}
				recs = append(recs, decoded...)
				for range decoded {
					from = append(from, len(members))
				}
				i = j
			}
			members = append(members, cur)
		}
		meta := RollupMetadata{}
		if trial%7 == 0 {
			meta.ResetOrder = uint64(rng.IntN(15))
		}
		retire := int64(0)
		if trial%5 == 0 {
			retire = int64(rng.IntN(200))
		}
		// The part bound mergeParts sets ttl from: the inputs' minimum data expiry.
		expiring := false
		for _, r := range recs {
			expiring = expiring || rollupRecordExpiry(c, r) < retire
		}
		for _, prefix := range []bool{false, true} {
			fast.ttl = prefix && expiring
			got, err := fast.merge(members, c, &meta, retire, prefix)
			reference.records, reference.from = recs, from
			want, werr := reference.resolve(c, &meta, retire, prefix)
			if (err == nil) != (werr == nil) || err != nil && err.Error() != werr.Error() || (got == nil) != (want == nil) || len(got)+len(want) > 0 && !reflect.DeepEqual(got, want) {
				t.Fatalf("trial=%d prefix=%v retire=%d metadata=%+v\nrecords=%+v\nfrom=%v\ngot=%+v err=%v\nwant=%+v err=%v", trial, prefix, retire, meta, recs, from, got, err, want, werr)
			}
			if want == nil {
				skipped++
			} else {
				versioned++
			}
		}
	}
	t.Logf("%d names without versions, %d with", skipped, versioned)
	if skipped < 500 || versioned < 500 {
		t.Fatalf("coverage: %d names without versions, %d with", skipped, versioned)
	}
}

type referenceMergeVersions struct {
	records, block, out []sealrollup.Record
	from                []int // records[i]'s input
	versions            []sealrollup.Version
	view                []int // records the write-down sees (live incarnation)
	keep, gone, last    []bool
	seen                map[[2]int64]int // (archive, slot) -> winner index
}

// merge returns the name's output records sorted by (archive, slot), or nil
// when no input carries versions and a prefix merge drops no expired record
// (the per-slot merge then runs unchanged).
// resolve preserves the original map-based merge and unconditional prefix write-down.
func (m *referenceMergeVersions) resolve(config sealrollup.Config, metadata *RollupMetadata, retire int64, prefix bool) ([]sealrollup.Record, error) {
	m.seen = map[[2]int64]int{}
	versioned := false
	for i, r := range m.records {
		k := [2]int64{int64(r.Archive), r.Slot}
		if _, ok := m.seen[k]; ok || r.Kind != sealrollup.KindRaw {
			versioned = true
		}
		m.seen[k] = i
	}
	recs := m.records
	dead := func(r sealrollup.Record) bool { return metadata.Expired || r.Order <= metadata.ResetOrder }
	expired := func(r sealrollup.Record) bool { return rollupRecordExpiry(config, r) < retire }
	// A prefix merge that drops an expired live record writes down (TTL).
	expiring := false
	for _, r := range recs {
		expiring = expiring || !dead(r) && expired(r)
	}
	if !versioned && !(prefix && expiring) {
		return nil, nil
	}
	// The per-slot winner, as the per-slot merge picks it: of each input's
	// per-slot view (its last Raw or seal Mat record of a slot), the live
	// record with the greatest time, then order.
	clear(m.seen)
	for i, r := range recs {
		if r.Kind != sealrollup.KindRaw && (r.Kind != sealrollup.KindMat || r.Upto != 0) || dead(r) || expired(r) {
			continue
		}
		k := [2]int64{int64(r.Archive), r.Slot}
		w, ok := m.seen[k]
		if !ok || m.from[w] == m.from[i] {
			m.seen[k] = i
			continue
		}
		b := recs[w]
		newer, err := newerOrigin(partOrigin{Class: LivePart}, r, partOrigin{Class: LivePart}, b)
		if err != nil {
			return nil, err
		}
		if !newer && r.Sample.Time == b.Sample.Time && r.Order == b.Order && r.Delta != b.Delta {
			return nil, errors.New("conflicting merge replacement order")
		}
		if newer {
			m.seen[k] = i
		}
	}
	m.keep = append(m.keep[:0], make([]bool, len(recs))...)
	var mats []sealrollup.Version
	if prefix {
		// The write-down sees the live incarnation; a reset deletes the rest.
		m.versions, m.view, m.gone = m.versions[:0], m.view[:0], m.gone[:0]
		for i, r := range recs {
			if !dead(r) {
				m.versions = append(m.versions, r.Version())
				m.view = append(m.view, i)
				m.gone = append(m.gone, expired(r))
			}
		}
		var keep []bool
		keep, mats = sealrollup.WriteDown(config, m.versions, func(i int) bool { return m.gone[i] })
		for j, i := range m.view {
			m.keep[i] = keep[j]
		}
	} else {
		for i, r := range recs {
			m.keep[i] = !dead(r) && !expired(r)
		}
	}
	m.out, m.last = m.out[:0], m.last[:0]
	for i, r := range recs {
		w, ok := m.seen[[2]int64{int64(r.Archive), r.Slot}]
		if win := ok && w == i; m.keep[i] || win {
			m.out = append(m.out, r)
			m.last = append(m.last, win)
		}
	}
	for _, v := range mats {
		if r := v.Record(config); !dead(r) && !expired(r) {
			m.out = append(m.out, r)
			m.last = append(m.last, false)
		}
	}
	// Sort by (archive, slot), the per-slot winner last in its slot.
	sort.Stable(versionOrder{m.out, m.last})
	return m.out, nil
}

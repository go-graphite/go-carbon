package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/partsrun"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func TestCompressedHistoryPhysicalReplacementAndBound(t *testing.T) {
	cfg := sealrollup.Config{Archives: []sealrollup.Archive{{Step: 1, Count: 64}, {Step: 8, Count: 128}}}
	s := &atomicRollupState{Config: cfg}
	reference := sealrollup.NewSlots(cfg.Archives)
	var w rollupHistoryWorkspace
	for n := int64(1); n <= 256; n++ {
		if err := w.load(s, cfg); err != nil {
			t.Fatal(err)
		}
		// One old-epoch correction after wrap must replace its physical slot even
		// when a subsequent range excludes that older epoch.
		epoch := n
		if n == 130 {
			epoch = 2
		}
		bits := uint64(n) * 0x9e3779b97f4a7c15
		if n == 129 {
			bits = math.Float64bits(math.Copysign(0, -1))
		}
		if n == 130 {
			bits = 0x7ff80000000000ab
		}
		records := []sealrollup.Record{{Delta: sealrollup.Delta{Archive: 0, Slot: epoch % 64, Order: uint64(n), Sample: sealrollup.Sample{Time: epoch, Bits: bits, Sequence: uint64(n)}}}}
		reference.Install(records[0].Delta)
		h, err := w.seal(records)
		if err != nil {
			t.Fatal(err)
		}
		s.history = h
		if err = w.load(s, cfg); err != nil {
			t.Fatal(err)
		}
		header, err := partsrun.Inspect(h[0], 64)
		if err != nil || header.Count > 64 {
			t.Fatal(header, err)
		}
		for slot := int64(0); slot < 64; slot++ {
			got, gok := w.view(0, slot)
			want, wok := reference.View(0, slot)
			if got != want || gok != wok {
				t.Fatalf("seal%d slot%d mismatch", n, slot)
			}
		}
		for start := max(int64(0), n-64); start < n; start += 7 {
			got := w.ranges(0, start, start+8, nil)
			want := reference.Range(0, start, start+8, nil)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("seal%d range%d mismatch %v/%v", n, start, got, want)
			}
		}
	}
	if s.Slots != nil || s.index != nil {
		t.Fatal("expanded history retained")
	}
}

func TestHistoryTilesPlannerDifferential(t *testing.T) {
	for method := sealrollup.Average; method <= sealrollup.First; method++ {
		for _, xff := range []float32{0, .5, 1} {
			t.Run(fmt.Sprintf("method%d/xff%g", method, xff), func(t *testing.T) { historyTilesPlannerDifferential(t, method, xff) })
		}
	}
}
func historyTilesPlannerDifferential(t *testing.T, method sealrollup.Method, xff float32) {
	cfg := sealrollup.Config{Method: method, XFilesFactor: xff, Archives: []sealrollup.Archive{{Step: 1, Count: 4096}, {Step: 8, Count: 1024}, {Step: 64, Count: 256}}}
	policy, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := sealrollup.Prepare(cfg)
	if err != nil {
		t.Fatal(err)
	}
	reference := sealrollup.NewSlots(cfg.Archives)
	var prior sealrollup.Frontier
	var scratch sealrollup.Workspace
	o := &atomicRollupOwner{}
	var h compactRollupHead
	expected := make(map[[2]int64]sealrollup.Sample)
	for n := 1; n <= 40; n++ {
		now := int64(1794700800 + n*513)
		ps := make([]row, 513)
		fillSealRows(ps, 0, now, n, nil)
		if n%7 == 0 {
			for i := range ps {
				ps[i].Time -= 4096
				ps[i].Bits = 0x7ff80000000000ab
			}
		}
		if n%3 == 0 {
			ps[17].Bits = 1 << 63
		}
		batch := coreBatch{ID: 0, Order: uint64(n), Clock: now, Policy: policy, Points: ps}
		h.reset()
		if err = h.append(batch); err != nil {
			t.Fatal(err)
		}
		in := coreSealInput{Next: coreFrontier{NextPart: uint64(n), Config: coreConfig{PartitionSeconds: 3600}, ExpiredBefore: math.MinInt64}, Compact: &compactRollupSelection{head: &h, batches: h.batches}}
		points := make([]sealrollup.Sample, len(ps))
		for i, p := range ps {
			points[i] = sealrollup.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Seq}
		}
		want, err := prepared.PlanInto(nil, prior, reference.View, reference.Range, []sealrollup.Batch{{Order: uint64(n), Now: now, Points: points}}, &scratch)
		if err != nil {
			t.Fatal(err)
		}
		got, err := planCompactCoreRollups(o, in)
		if err != nil {
			t.Fatal(err)
		}
		for _, kv := range got.Files[0] {
			if kv.Key[18] != 'b' {
				continue
			}
			a := int(binary.BigEndian.Uint32(kv.Key[19:]))
			var records []sealrollup.Record
			for _, r := range want.Records {
				if r.Archive == a {
					records = append(records, r)
				}
			}
			encoded, err := partsrun.Append(nil, 0, len(records), func(i int) partsrun.Sample {
				r := records[i]
				return partsrun.Sample{Time: r.Sample.Time, Bits: r.Sample.Bits, Sequence: r.Order}
			})
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(kv.Value, encoded) {
				t.Fatalf("seal%d archive%d persisted bytes differ", n, a)
			}
		}
		got.Commit()
		for _, r := range want.Records {
			reference.Install(r.Delta)
			expected[[2]int64{int64(r.Archive), r.Slot}] = r.Sample
		}
		prior = want.Next
		state := o.state.get(0)
		if state.Frontier != prior {
			t.Fatal("frontier mismatch")
		}
		if !reflect.DeepEqual(state.snapshotSlots(), expected) {
			t.Fatalf("seal%d physical state mismatch", n)
		}
	}
}

func TestHistoryTilesLazyExactAndDetached(t *testing.T) {
	cfg := sealrollup.Config{Archives: []sealrollup.Archive{{Step: 1, Count: 4096}, {Step: 8, Count: 1024}}}
	s := &atomicRollupState{Config: cfg}
	reference := sealrollup.NewSlots(cfg.Archives)
	var w rollupHistoryWorkspace
	var records []sealrollup.Record
	for a, ar := range cfg.Archives {
		for slot := int64(0); slot < ar.Count; slot++ {
			r := sealrollup.Record{Delta: sealrollup.Delta{Archive: a, Slot: slot, Order: 1, Sample: sealrollup.Sample{Time: slot * ar.Step, Bits: uint64(slot) * 0x9e3779b97f4a7c15, Sequence: uint64(slot)}}}
			records = append(records, r)
			reference.Install(r.Delta)
		}
	}
	if err := w.load(s, cfg); err != nil {
		t.Fatal(err)
	}
	h, err := w.seal(records)
	if err != nil {
		t.Fatal(err)
	}
	s.history = h
	if !historyTiled(h[0]) {
		t.Fatal("history not tiled")
	}
	prior := bytes.Clone(h[0])
	unchanged := bytes.Clone(historyTileValue(h[0], 3))
	if err = w.load(s, cfg); err != nil {
		t.Fatal(err)
	}
	if w.decodedTiles != 0 {
		t.Fatal("load decoded history")
	}
	if _, ok := w.view(0, 17); !ok || w.decodedTiles != 1 {
		t.Fatal("view did not decode just one tile", w.decodedTiles)
	}
	// Replace a newer physical slot with an older epoch before filtering it.
	r := sealrollup.Record{Delta: sealrollup.Delta{Archive: 0, Slot: 17, Order: 2, Sample: sealrollup.Sample{Time: 4113, Bits: 0x8000000000000000, Sequence: 2}}}
	reference.Install(r.Delta)
	next, err := w.seal([]sealrollup.Record{r})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(prior, h[0]) {
		t.Fatal("pending seal mutated committed history")
	}
	if !bytes.Equal(unchanged, historyTileValue(next[0], 3)) {
		t.Fatal("untouched tile reencoded")
	}
	if w.decodedTiles != 1 {
		t.Fatal("seal decoded untouched tiles", w.decodedTiles)
	}
	s.history = next
	for iteration := 0; iteration < 40; iteration++ {
		if err = w.load(s, cfg); err != nil {
			t.Fatal(err)
		}
		for a, ar := range cfg.Archives {
			for slot := int64(0); slot < ar.Count; slot++ {
				got, gok := w.view(a, slot)
				want, wok := reference.View(a, slot)
				if got != want || gok != wok {
					t.Fatalf("iteration%d archive%d slot%d mismatch", iteration, a, slot)
				}
			}
		}
		for _, start := range []int64{0, 250, 4080, 4096, 4100} {
			got := w.ranges(0, start, start+64, nil)
			want := reference.Range(0, start, start+64, nil)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("range%d mismatch", start)
			}
		}
		a := iteration % 2
		ar := cfg.Archives[a]
		slot := int64((iteration*257 + 17) % int(ar.Count))
		epoch := slot + ar.Count*int64(iteration%3)
		r = sealrollup.Record{Delta: sealrollup.Delta{Archive: a, Slot: slot, Order: uint64(iteration + 3), Sample: sealrollup.Sample{Time: epoch * ar.Step, Bits: 0x7ff80000000000ab + uint64(iteration), Sequence: uint64(iteration)}}}
		reference.Install(r.Delta)
		next, err = w.seal([]sealrollup.Record{r})
		if err != nil {
			t.Fatal(err)
		}
		s.history = next
	}
	if got := s.snapshotSlots(); len(got) != 5120 {
		t.Fatal("snapshot omitted tiles", len(got))
	}
}

func TestCompressedHistorySparseLargeRetention(t *testing.T) {
	cfg := sealrollup.Config{Archives: []sealrollup.Archive{{Step: 1, Count: 1 << 30}}}
	s := &atomicRollupState{Config: cfg}
	var w rollupHistoryWorkspace
	if err := w.load(s, cfg); err != nil {
		t.Fatal(err)
	}
	r := sealrollup.Record{Delta: sealrollup.Delta{Archive: 0, Slot: 17, Order: 1, Sample: sealrollup.Sample{Time: 17, Bits: 1, Sequence: 1}}}
	h, err := w.seal([]sealrollup.Record{r})
	if err != nil {
		t.Fatal(err)
	}
	if len(h) != 1 || len(h[0]) > 128 || cap(w.merged) > 8 {
		t.Fatal("retention-sized allocation", len(h[0]), cap(w.merged))
	}
}

func TestHistoryTilesDecodeOnceAcrossTiersAndSeal(t *testing.T) {
	cfg := sealrollup.Config{Archives: []sealrollup.Archive{{Step: 1, Count: 4096}, {Step: 8, Count: 1024}}}
	s := &atomicRollupState{Config: cfg}
	var w rollupHistoryWorkspace
	var records []sealrollup.Record
	for a, ar := range cfg.Archives {
		for slot := int64(0); slot < ar.Count; slot++ {
			records = append(records, sealrollup.Record{Delta: sealrollup.Delta{Archive: a, Slot: slot, Order: 1, Sample: sealrollup.Sample{Time: slot * ar.Step, Bits: uint64(slot), Sequence: 1}}})
		}
	}
	if err := w.load(s, cfg); err != nil {
		t.Fatal(err)
	}
	h, err := w.seal(records)
	if err != nil {
		t.Fatal(err)
	}
	s.history = h
	if err = w.load(s, cfg); err != nil {
		t.Fatal(err)
	}
	// Alternate distant tiles and archives, revisit through ranges, then encode
	// physical replacements. All three consumers share the same decoded tiles.
	for repeat := 0; repeat < 4; repeat++ {
		for _, a := range []int{0, 1} {
			for _, slot := range []int64{17, 600, 17, 800, 600} {
				got, ok := w.view(a, slot)
				if !ok || got.Time != slot*cfg.Archives[a].Step {
					t.Fatal(a, slot, got, ok)
				}
			}
			step := cfg.Archives[a].Step
			_ = w.ranges(a, 0, 900*step, nil)
		}
	}
	if w.decodedTiles != 8 {
		t.Fatal("tile decoded repeatedly", w.decodedTiles)
	}
	replacements := []sealrollup.Record{records[17], records[600], records[4096+17], records[4096+600]}
	next, err := w.seal(replacements)
	if err != nil {
		t.Fatal(err)
	}
	if w.decodedTiles != 8 {
		t.Fatal("publication decoded tile again", w.decodedTiles)
	}
	for a := range h {
		if !bytes.Equal(h[a], next[a]) {
			t.Fatal("no-op seal changed bytes", a)
		}
	}
	// A different metric must reset offsets without retaining old tile contents.
	if err = w.load(nil, cfg); err != nil {
		t.Fatal(err)
	}
	if _, ok := w.view(0, 17); ok {
		t.Fatal("previous metric escaped scratch")
	}
}

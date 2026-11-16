package partsread

import (
	"context"
	"math"
	"testing"
)

func TestPhysicalOverlapConservative(t *testing.T) {
	a := Archive{60, 10}
	f := Freeze([]Record{{ID: 7, Step: 60, Sample: Sample{Time: 660}}})
	index, err := NewSlotIndex([]SlotBlock{{ID: 7, Archive: a, Slots: SlotRange{1, 2}, Key: []byte("full")}}, func(context.Context, []byte, []SlotRange, func(Sample) error) error {
		t.Fatal("overlap check performed IO")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []interface {
		MayOverlap(context.Context, uint64, Archive, int64, int64) (bool, error)
	}{f, index} {
		for _, tc := range []struct {
			from, until int64
			want        bool
		}{{60, 180, true}, {120, 240, false}, {600, 720, true}, {0, 600, true}, {540, 720, true}} {
			got, err := s.MayOverlap(context.Background(), 7, a, tc.from, tc.until)
			if err != nil || got != tc.want {
				t.Fatal(tc, got, err)
			}
		}
	}
}

type opaqueOverlay struct{ Source }

func TestOverlayGenerationUnknownAndCancellation(t *testing.T) {
	ctx := context.Background()
	a := Archive{60, 10}
	var p Publication
	p.Publish(View{Catalog: testCatalog([]Archive{a}), Parts: []Source{Freeze(nil)}})
	h, err := p.Pin(NewBudget(1<<20), 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	token, err := h.Resolve("sample")
	if err != nil {
		t.Fatal(err)
	}
	base := &Series{From: 60, Until: 180, Step: 60, Values: make([]float64, 2)}
	if yes, err := h.HasOverlayRangeResolved(ctx, token, 60, 60, 180); err != nil || yes {
		t.Fatal(yes, err)
	}
	if yes, err := h.HasOverlayResolved(ctx, token, base); err != nil || yes {
		t.Fatal(yes, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := h.HasOverlayResolved(canceled, token, base); err != context.Canceled {
		t.Fatal(err)
	}
	p.Publish(View{Catalog: testCatalog([]Archive{a}), Parts: []Source{opaqueOverlay{Freeze(nil)}}})
	next, err := p.Pin(NewBudget(1<<20), 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	defer p.Close()
	if _, err := next.HasOverlayRangeResolved(ctx, token, 60, 60, 180); err != ErrMetadataGeneration {
		t.Fatal(err)
	}
	other, err := next.Resolve("sample")
	if err != nil {
		t.Fatal(err)
	}
	if yes, err := next.HasOverlayResolved(ctx, other, base); err != nil || !yes {
		t.Fatal("unknown source omitted", yes, err)
	}
	h.Close()
	if _, err := h.HasOverlayResolved(ctx, token, base); err != ErrClosed {
		t.Fatal(err)
	}
}

func TestLegacyPrefixWorkspaceReset(t *testing.T) {
	ctx := context.Background()
	var p Publication
	p.Publish(View{Catalog: testCatalog([]Archive{{1, 1000}}), Parts: []Source{Freeze(nil)}})
	defer p.Close()
	for run := 0; run < 20; run++ {
		h, e := p.Pin(NewBudget(1<<20), 1<<18)
		if e != nil {
			t.Fatal(e)
		}
		out, e := h.Render(ctx, "sample", 100, 109, 110)
		if e != nil {
			t.Fatal(e)
		}
		for _, v := range out.Values {
			if !math.IsNaN(v) {
				t.Fatal("legacy prefix leaked across handle reuse")
			}
		}
		if e := h.JoinLegacyPrefix(ctx, out, []float64{1, 2, 3}); e != nil {
			t.Fatal(e)
		}
		if out.Values[0] != 1 || out.Occupied[0] {
			t.Fatal("prefix fill changed occupancy")
		}
		h.Close()
	}
}

func TestJoinLegacyPrefixPreservesPartBitsAndSparseCleanup(t *testing.T) {
	ctx := context.Background()
	var p Publication
	bits := []uint64{0x8000000000000000, 0x7ff8000000000042}
	p.Publish(View{Catalog: testCatalog([]Archive{{1, 1000}}), Parts: []Source{Freeze([]Record{
		{ID: 7, Step: 1, Sample: Sample{Time: 102, Bits: bits[0]}},
		{ID: 7, Step: 1, Sample: Sample{Time: 108, Bits: bits[1]}},
	})}})
	defer p.Close()
	h, err := p.Pin(NewBudget(1<<20), 1<<18)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	out, err := h.Render(ctx, "sample", 100, 109, 110)
	if err != nil {
		t.Fatal(err)
	}
	legacy := []float64{1, 2, 3, 4, 5}
	if err := h.JoinLegacyPrefix(ctx, out, legacy); err != nil {
		t.Fatal(err)
	}
	if math.Float64bits(out.Values[1]) != bits[0] || math.Float64bits(out.Values[7]) != bits[1] || out.Values[4] != 5 || !math.IsNaN(out.Values[5]) {
		t.Fatal("sparse join changed values", out.Values)
	}
	if len(h.workspaces.touched) != 2 {
		t.Fatal("join made cleanup dense")
	}
}

func TestCoverageHintUsesPinnedManifestWithoutIO(t *testing.T) {
	ctx := context.Background()
	a := Archive{1, 1000}
	samples := make([]Sample, 25)
	for i := range samples {
		samples[i] = Sample{Time: int64(100 + i), Bits: math.Float64bits(float64(i))}
	}
	path, err := PublishBlockPart(t.TempDir(), "part", 1, []BlockData{{SlotBlock: SlotBlock{ID: 7, Archive: a, Slots: SlotRange{0, 1000}, Key: []byte("block")}, Samples: samples}})
	if err != nil {
		t.Fatal(err)
	}
	source, err := OpenBlockPart(path)
	if err != nil {
		t.Fatal(err)
	}
	var p Publication
	p.Publish(View{Catalog: testCatalog([]Archive{a}), Parts: []Source{source}, Close: source.Close})
	defer p.Close()
	h, err := p.Pin(NewBudget(1<<20), 1<<18)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	token, err := h.Resolve("sample")
	if err != nil {
		t.Fatal(err)
	}
	// Closing only the OS descriptor proves that hint routing cannot read it.
	// This source's Close remains idempotent and the actual render is not used.
	for _, file := range source.files {
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if _, _, err := h.OverlayCoverageResolved(ctx, token, 1, 100, 200); err != nil {
			t.Fatal(err)
		}
	}); allocations != 0 {
		t.Fatalf("coverage hint allocated %g times/query", allocations)
	}
	yes, slots, err := h.OverlayCoverageResolved(ctx, token, 1, 100, 200)
	if err != nil || !yes || slots != 25 {
		t.Fatal(yes, slots, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := h.OverlayCoverageResolved(canceled, token, 1, 100, 200); err != context.Canceled {
		t.Fatal(err)
	}
	if _, _, err := h.OverlayCoverageResolved(ctx, ResolvedMetric{}, 1, 100, 200); err != ErrMetadataGeneration {
		t.Fatal(err)
	}
}

func TestReusableLegacyJoinBitExactAndWorkspaceCleanup(t *testing.T) {
	ctx := context.Background()
	var p Publication
	p.Publish(View{Catalog: testCatalog([]Archive{{1, 1000}}), Parts: []Source{Freeze([]Record{
		{ID: 7, Step: 1, Sample: Sample{Time: 102, Bits: 0x8000000000000000}},
		{ID: 7, Step: 1, Sample: Sample{Time: 108, Bits: 0x7ff8000000000042}},
	})}})
	defer p.Close()
	scratch := make([]float64, 9)
	for run := 0; run < 10; run++ {
		h, err := p.Pin(NewBudget(1<<20), 1<<18)
		if err != nil {
			t.Fatal(err)
		}
		out, err := h.Render(ctx, "sample", 100, 109, 110)
		if err != nil {
			t.Fatal(err)
		}
		copy(scratch, []float64{1, 2, 3, 4, 5})
		if err := h.JoinLegacyPrefixInto(ctx, out, scratch[:5], scratch); err != nil {
			t.Fatal(err)
		}
		if math.Float64bits(out.Values[1]) != 0x8000000000000000 || math.Float64bits(out.Values[7]) != 0x7ff8000000000042 || out.Values[4] != 5 || !math.IsNaN(out.Values[5]) {
			t.Fatal(out.Values)
		}
		if len(h.workspaces.touched) != 2 {
			t.Fatal("cleanup grew dense")
		}
		h.Close()
	}
}

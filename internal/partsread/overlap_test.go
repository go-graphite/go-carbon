package partsread

import (
	"context"
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
	if _, err := next.HasOverlayResolved(ctx, token, base); err != ErrMetadataGeneration {
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

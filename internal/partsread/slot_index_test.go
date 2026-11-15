package partsread

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestSlotIndexWrappedVersionsAndIsolation(t *testing.T) {
	a := Archive{Step: 7, Points: 10}
	data := map[string][]Sample{
		"pg0/v0/name/archive7/low":  {{Time: 0, Bits: 0x8000000000000000, Sequence: 1}, {Time: 70, Bits: 0x7ff8000000000042, Sequence: 2}},
		"pg0/v0/name/archive7/high": {{Time: 63, Bits: math.Float64bits(9), Sequence: 1}},
	}
	input := []SlotBlock{
		{ID: 1, Archive: a, Slots: SlotRange{0, 1}, Key: []byte("pg0/v0/name/archive7/low")},
		{ID: 1, Archive: a, Slots: SlotRange{9, 10}, Key: []byte("pg0/v0/name/archive7/high")},
		{ID: 2, Archive: a, Slots: SlotRange{0, 10}, Key: []byte("other series")},
		{ID: 1, Archive: Archive{Step: 70, Points: 10}, Slots: SlotRange{0, 10}, Key: []byte("coarse archive")},
	}
	var keys []string
	index, err := NewSlotIndex(input, func(ctx context.Context, key []byte, ranges []SlotRange, y func(Sample) error) error {
		keys = append(keys, string(key))
		if len(ranges) != 1 {
			t.Fatalf("unexpected spans %v", ranges)
		}
		for _, p := range data[string(key)] {
			if err := y(p); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	input[0].Key[0] = 'X'
	input[1].Slots = SlotRange{1, 2}
	var got []Sample
	err = index.Scan(context.Background(), 1, a, 63, 77, func(p Sample) error { got = append(got, p); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || len(keys) != 2 || got[0].Time != 0 || got[1].Time != 70 || got[1].Bits != 0x7ff8000000000042 || got[2].Time != 63 {
		t.Fatalf("lost wrapped versions or identity isolation: keys%v samples%v", keys, got)
	}
	keys = nil
	if err := index.Scan(context.Background(), 1, a, 7, 56, func(Sample) error { t.Fatal("empty span yielded data"); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("empty ranges performed disk reads: %v", keys)
	}
	if found, err := index.HasArchive(context.Background(), 1, 7); err != nil || !found {
		t.Fatalf("populated archive: %v %v", found, err)
	}
	if found, _ := index.HasArchive(context.Background(), 3, 7); found {
		t.Fatal("unknown series populated")
	}
}

func TestSlotIndexConservativeSpanAndErrors(t *testing.T) {
	a := Archive{Step: 1, Points: 10}
	key := []byte("complete immutable identity")
	var spans []SlotRange
	failure := errors.New("read failed")
	index, err := NewSlotIndex([]SlotBlock{{ID: 1, Archive: a, Slots: SlotRange{0, 10}, Key: key}}, func(ctx context.Context, _ []byte, r []SlotRange, y func(Sample) error) error {
		spans = append(spans, r...)
		for _, ts := range []int64{0, 1, 9, 10, 11, 19} {
			if err := y(Sample{Time: ts}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var times []int64
	if err := index.Scan(context.Background(), 1, a, 9, 11, func(p Sample) error { times = append(times, p.Time); return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(spans, []SlotRange{{9, 10}, {0, 1}}) || !reflect.DeepEqual(times, []int64{0, 9, 10, 19}) {
		t.Fatalf("spans%v times%v", spans, times)
	}
	if err := index.Scan(context.Background(), 1, a, 9, 11, func(Sample) error { return failure }); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := index.Scan(ctx, 1, a, 9, 11, func(Sample) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := index.HasArchive(ctx, 1, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, b := range []SlotBlock{{}, {ID: 1, Archive: a, Slots: SlotRange{-1, 2}, Key: key}, {ID: 1, Archive: a, Slots: SlotRange{2, 11}, Key: key}} {
		if _, err := NewSlotIndex([]SlotBlock{b}, index.read); err == nil {
			t.Fatalf("invalid entry accepted %v", b)
		}
	}
}

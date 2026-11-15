package main

import (
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

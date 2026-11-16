package main

import (
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2daemon"
	"github.com/go-graphite/go-carbon/points"
	pb "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"math"
	"testing"
	"time"
)

func TestComparisonPresenceBitsAndCorrections(t *testing.T) {
	nan := uint64(0x7ff8000000000042)
	rows := []v2core.Row{{Time: 60, Bits: 1, Seq: 1}, {Time: 60, Bits: 1 << 63, Seq: 2}, {Time: 120, Bits: nan, Seq: 3}}
	classic := &pb.FetchResponse{StartTime: 60, StepTime: 60, Values: []float64{math.Copysign(0, -1), math.Float64frombits(nan), 0}, IsAbsent: []bool{false, false, true}}
	n, bad, _, err := compareRaw("a", rows, classic, 60, 240)
	if err != nil || n != 2 || bad != 0 {
		t.Fatalf("%d %d %v", n, bad, err)
	}
	classic.IsAbsent[1] = true
	_, bad, ex, err := compareRaw("a", rows, classic, 60, 240)
	if err != nil || bad != 1 || len(ex) != 1 || ex[0].Time != 120 {
		t.Fatalf("%d %+v %v", bad, ex, err)
	}
	classic.IsAbsent[1] = false
	classic.Values[0] = 0
	_, bad, _, _ = compareRaw("a", rows, classic, 60, 240)
	if bad != 1 {
		t.Fatal("lost signed zero mismatch")
	}
	rows = append(rows, v2core.Row{Time: 61, Seq: 4})
	if _, _, _, err = compareRaw("a", rows, classic, 60, 240); err == nil {
		t.Fatal("accepted unaligned input")
	}
}

func TestSerialCatalogIdentityAndSnapshot(t *testing.T) {
	store, err := v2daemon.Open(t.TempDir(), v2daemon.Config{Layer: 2, Producers: 1, Shards: 2, GroupInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	p := store.Producers[0]
	if err = p.Append([]string{"a", "b", "a"}, []points.Point{{Timestamp: 60, Value: 1}, {Timestamp: 60, Value: 2}, {Timestamp: 120, Value: 3}}, []uint64{1, 2, 3}, 120); err != nil {
		t.Fatal(err)
	}
	if err = p.Seal(); err != nil {
		t.Fatal(err)
	}
	if err = p.Drain(); err != nil {
		t.Fatal(err)
	}
	// A synchronous core barrier fences the pipelined producer before reading.
	if err = store.Core.Append(0, nil, 3, true); err != nil {
		t.Fatal(err)
	}
	got := map[uint64][]v2core.Row{}
	for _, db := range store.Core.DBs() {
		s := db.NewSnapshot()
		err = v2core.ScanRaw(s, func(r v2core.Row) error { got[r.ID] = append(got[r.ID], r); return nil })
		_ = s.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(got) != 2 || len(got[1]) != 2 || len(got[2]) != 1 || got[2][0].Bits != math.Float64bits(2) {
		t.Fatalf("first-seen identity mismatch: %+v", got)
	}
}

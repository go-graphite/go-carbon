package main

import (
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2daemon"
	"github.com/go-graphite/go-carbon/points"
	pb "github.com/go-graphite/protocol/carbonapi_v2_pb"

	"google.golang.org/protobuf/proto"
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

func TestClassicFetchExcludedFromBoundary(t *testing.T) {
	rows := []v2core.Row{{Time: 60, Bits: math.Float64bits(1)}, {Time: 61, Bits: math.Float64bits(2)}, {Time: 62, Bits: math.Float64bits(3)}, {Time: 63, Bits: math.Float64bits(4)}}
	classic := &pb.FetchResponse{StartTime: 61, StepTime: 1, Values: []float64{2, 3, 4}, IsAbsent: []bool{false, false, false}}
	n, bad, _, err := compareRaw("a", rows, classic, 60, 63)
	if err != nil || n != 2 || bad != 0 {
		t.Fatalf("excluded endpoints counted: n=%d bad=%d err=%v", n, bad, err)
	}
	n, bad, examples, err := compareRaw("a", rows[2:], classic, 60, 63)
	if err != nil || n != 2 || bad != 1 || len(examples) != 1 || examples[0].Time != 61 {
		t.Fatalf("lost an interior missing slot: n=%d bad=%d examples=%v err=%v", n, bad, examples, err)
	}
}

func TestFrozenComparisonFreshSnapshotConvergence(t *testing.T) {
	store, err := v2daemon.Open(t.TempDir(), v2daemon.Config{Layer: 1, Producers: 1, Shards: 1, GroupInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("from") != strconv.Itoa(60) || r.URL.Query().Get("until") != strconv.Itoa(120) || r.URL.Query().Get("target") != "a" {
			t.Error("retry changed frozen query", r.URL.RawQuery)
		}
		b, _ := proto.Marshal(&pb.MultiFetchResponse{Metrics: []*pb.FetchResponse{{Name: "a", StartTime: 60, StepTime: 60, Values: []float64{2}, IsAbsent: []bool{false}}}})
		_, _ = w.Write(b)
	}))
	defer server.Close()
	sums := &totals{}
	job := compareJob{Names: []candidate{{ID: 1, Name: "a"}}, From: 60, Until: 120, FirstSnapshot: time.Now()}
	for seq := uint64(1); seq <= 2; seq++ {
		p := store.Producers[0]
		if err = p.Append([]string{"a"}, nil, []points.Point{{Timestamp: 60, Value: float64(seq)}}, []uint64{seq}, 120); err != nil {
			t.Fatal(err)
		}
		if err = p.Seal(); err != nil {
			t.Fatal(err)
		}
		if err = p.Drain(); err != nil {
			t.Fatal(err)
		}
		if err = store.Core.Append(0, nil, seq, true); err != nil {
			t.Fatal(err)
		}
		job.Snapshots = nil
		for _, db := range store.Core.DBs() {
			job.Snapshots = append(job.Snapshots, db.NewSnapshot())
		}
		job.Attempt = int(seq - 1)
		if retry := compare(job, server.URL, sums); retry != (seq == 1) {
			t.Fatalf("attempt %d retry=%v", seq-1, retry)
		}
	}
	if sums.Names != 2 || sums.Points != 2 || sums.Mismatches != 1 || sums.Failures != 0 {
		t.Fatalf("incorrect totals: %+v", sums)
	}
}

func TestSerialCatalogIdentityAndSnapshot(t *testing.T) {
	store, err := v2daemon.Open(t.TempDir(), v2daemon.Config{Layer: 1, Producers: 1, Shards: 2, GroupInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	p := store.Producers[0]
	if err = p.Append([]string{"a", "b", "a"}, nil, []points.Point{{Timestamp: 60, Value: 1}, {Timestamp: 60, Value: 2}, {Timestamp: 120, Value: 3}}, []uint64{1, 2, 3}, 120); err != nil {
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

package v2backend

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func TestLiveReadsNeverSealAndSurvivePublication(t *testing.T) {
	clock := int64(1794700800)
	s, e := openAt(testOptions(t.TempDir(), simplePolicy()), func() time.Time { return time.Unix(clock, 0) })
	if e != nil {
		t.Fatal(e)
	}
	defer requireClose(t, s)
	appendPoint := func(v float64) {
		t.Helper()
		if e := s.Append(&points.Points{Metric: "head.metric", Data: []points.Point{{Timestamp: clock - 10, Value: v}}}); e != nil {
			t.Fatal(e)
		}
	}
	appendPoint(math.Copysign(0, -1))
	before := s.Stats().Parts
	first, e := s.Fetch(context.Background(), "head.metric", int(clock-11), int(clock))
	if e != nil {
		t.Fatal(e)
	}
	defer first.Release()
	if math.Float64bits(first.Values[0]) != 0x8000000000000000 {
		t.Fatal("active head not visible")
	}
	info, e := s.Info(context.Background(), "head.metric")
	if e != nil {
		t.Fatal(e)
	}
	info.Archives[0].Step = 999
	info, e = s.Info(context.Background(), "head.metric")
	if e != nil || info.Archives[0].Step != 1 {
		t.Fatal("Info leaked mutable policy")
	}
	matches, e := s.Find(context.Background(), "head.*")
	if e != nil || len(matches) != 1 || matches[0].Name != "head.metric" {
		t.Fatalf("find: %+v %v", matches, e)
	}
	if s.Stats().Parts != before {
		t.Fatal("read sealed")
	}
	entered, release := make(chan struct{}), make(chan struct{})
	s.beforeSeal = func(int) { close(entered); <-release }
	flushed := make(chan error, 1)
	go func() { flushed <- s.Flush() }()
	<-entered
	appendPoint(7)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	frozen, e := s.Fetch(ctx, "head.metric", int(clock-11), int(clock))
	if e != nil {
		close(release)
		t.Fatal(e)
	}
	if frozen.Values[0] != 7 {
		t.Fatal("frozen+active prefix lost")
	}
	close(release)
	if e = <-flushed; e != nil {
		t.Fatal(e)
	}
	s.beforeSeal = nil
	if math.Float64bits(first.Values[0]) != 0x8000000000000000 || frozen.Values[0] != 7 {
		t.Fatal("pinned result changed")
	}
	frozen.Release()
	after, e := s.Fetch(context.Background(), "head.metric", int(clock-11), int(clock))
	if e != nil {
		t.Fatal(e)
	}
	defer after.Release()
	if after.Values[0] != 7 {
		t.Fatal("publication/head overlap lost latest point")
	}
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	sealed, e := s.Fetch(context.Background(), "head.metric", int(clock-11), int(clock))
	if e != nil {
		t.Fatal(e)
	}
	defer sealed.Release()
	for i, v := range after.Values {
		if math.Float64bits(v) != math.Float64bits(sealed.Values[i]) {
			t.Fatalf("live/sealed slot%d differs", i)
		}
	}
}

package v2daemon

import (
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/points"
)

// A newly admitted name reaches both the mirror and core submission boundary
// without a timer tick. Established names keep grouped writes and application order.
func TestFirstAdmissionDrainsWithoutTicker(t *testing.T) {
	s, resolve := openProbeStore(t)
	p := s.Producers[0]
	var values []float64
	calls := 0
	p.mirror = func(_ int, batches []v2core.Batch) error {
		calls++
		for _, b := range batches {
			for _, pt := range b.Points {
				values = append(values, math.Float64frombits(pt.Bits))
			}
		}
		return nil
	}
	o := &applicationOwner{p: p, resolve: resolve}
	appendPoint := func(name string, value float64) {
		t.Helper()
		if err := o.Append([]string{name}, nil, []points.Point{{Timestamp: 1000, Value: value}}, nil, 1010); err != nil {
			t.Fatal(err)
		}
	}
	appendPoint("admission.first", 1)
	if calls != 1 || !reflect.DeepEqual(values, []float64{1}) {
		t.Fatalf("first admission waited for producer drain tick: calls=%d values=%v", calls, values)
	}
	appendPoint("admission.first", 2)
	if calls != 1 {
		t.Fatalf("established name bypassed grouped writes: calls=%d", calls)
	}
	appendPoint("admission.second", 3)
	if calls != 3 || !reflect.DeepEqual(values, []float64{1, 2, 3}) {
		t.Fatalf("application order changed: calls=%d values=%v", calls, values)
	}
}

func TestFirstAdmissionReturnsDrainError(t *testing.T) {
	s, resolve := openProbeStore(t)
	p := s.Producers[0]
	want := errors.New("mirror unavailable")
	p.mirror = func(int, []v2core.Batch) error { return want }
	o := &applicationOwner{p: p, resolve: resolve}
	err := o.Append([]string{"admission.error"}, nil, []points.Point{{Timestamp: 1000, Value: 1}}, nil, 1010)
	if !errors.Is(err, want) {
		t.Fatalf("drain error=%v, want %v", err, want)
	}
	// The expected producer failure is checked above, rather than reported again at cleanup.
	p.failed = nil
	p.mirror = nil
}

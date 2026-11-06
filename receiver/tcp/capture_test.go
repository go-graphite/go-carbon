package tcp

import (
	"bytes"
	"errors"
	"math"
	"math/rand/v2"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/go-graphite/go-carbon/points"
)

type testCapture struct {
	data  []byte
	count int
	err   error
}

type captureFunc func([]byte, int) error

func (f captureFunc) Write(data []byte, n int) error { return f(data, n) }

// Reuse the existing byte-boundary/protocol oracle with the capture encoder in
// the successor. The sink decodes its persisted representation independently.
func inheritTestInput(capture bool, opts *Options, out func(*points.Points), h *Handoff) (*TCP, error) {
	if !capture {
		return NewInherited("tcp", opts, out, h)
	}
	return NewInheritedCapture("tcp", opts, h, captureFunc(func(data []byte, _ int) error { return points.ReadBinary(bytes.NewReader(data), out) }))
}

func (s *testCapture) Write(data []byte, n int) error {
	if s.err != nil {
		return s.err
	}
	s.data = append(s.data, data...)
	s.count += n
	return nil
}

func TestCaptureMatchesLegacyRecords(t *testing.T) {
	sink := new(testCapture)
	b := captureBuffer{out: sink}
	rng := rand.New(rand.NewPCG(21, 42))
	var want []byte
	for i := range 20000 {
		name := []byte("capture.metric." + string(bytes.Repeat([]byte{'x'}, i%4097)))
		value, ts := math.Float64frombits(rng.Uint64()), int64(rng.Uint64())
		want = points.OnePoint(string(name), value, ts).AppendBinary(want)
		b.point(name, value, ts)
		// Both caller buffers can be overwritten immediately.
		clear(name)
	}
	if err := b.flush(); err != nil {
		t.Fatal(err)
	}
	if sink.count != 20000 || !bytes.Equal(want, sink.data) {
		t.Fatal("capture differs from legacy binary oracle")
	}
}

func TestCaptureStopsAfterWriteError(t *testing.T) {
	sink := &testCapture{err: errors.New("disk full")}
	b := captureBuffer{out: sink}
	b.point([]byte("first"), 1, 1)
	if err := b.flush(); !errors.Is(err, sink.err) {
		t.Fatal(err)
	}
	b.point([]byte("later"), 2, 2)
	if err := b.flush(); !errors.Is(err, sink.err) || sink.count != 0 {
		t.Fatal(err, sink.count)
	}
}

func TestCapturedPointStatsCarryExactlyOnce(t *testing.T) {
	r := &TCP{}
	reg := prometheus.NewRegistry()
	r.InitPrometheus(reg)
	const captured = uint64(1<<32) + 7
	r.SeedCapturedPoints(captured)
	atomic.AddUint32(&r.metricsReceived, 3)
	var received float64
	r.Stat(func(name string, v float64) {
		if name == "metricsReceived" {
			received = v
		}
	})
	if received != float64(captured+3) {
		t.Fatal(received)
	}
	r.Stat(func(name string, v float64) {
		if name == "metricsReceived" && v != 0 {
			t.Fatal("recounted", v)
		}
	})
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() == "metrics_received_tcp_total" {
			if f.Metric[0].GetCounter().GetValue() != float64(captured+3) {
				t.Fatal("prometheus count", f)
			}
			return
		}
	}
	t.Fatal("missing receiver counter")
}

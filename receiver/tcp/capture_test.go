package tcp

import (
	"bytes"
	"errors"
	"math"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"time"

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

type shardTestCapture struct{ sinks [8]testCapture }

func (s *shardTestCapture) Write([]byte, int) error { panic("global capture writer used") }
func (s *shardTestCapture) CaptureShards() []Capture {
	out := make([]Capture, len(s.sinks))
	for i := range s.sinks {
		out[i] = &s.sinks[i]
	}
	return out
}

func TestCaptureShardsPreservePerMetricOrder(t *testing.T) {
	s := new(shardTestCapture)
	a, b := newCaptureBuffer(s), newCaptureBuffer(s)
	// Same timestamp corrections must retain the recorded order across handlers.
	for i := range 1000 {
		for _, name := range []string{"shared.metric", "other.metric", "third.metric"} {
			c := &a
			if i%2 != 0 {
				c = &b
			}
			c.point([]byte(name), float64(i), 1700000000)
			if err := c.flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	seen := make(map[string]int)
	for i := range s.sinks {
		if err := points.ReadBinary(bytes.NewReader(s.sinks[i].data), func(p *points.Points) {
			for _, point := range p.Data {
				if point.Value != float64(seen[p.Metric]) {
					t.Fatalf("%s correction %v; want %d", p.Metric, point.Value, seen[p.Metric])
				}
				seen[p.Metric]++
			}
		}); err != nil {
			t.Fatal(err)
		}
	}
	for name, count := range seen {
		if count != 1000 {
			t.Fatal(name, count)
		}
	}
	if len(seen) != 3 {
		t.Fatal(seen)
	}
}

func TestSwitchCaptureFencesSubmissionsAndDrainsLocalBuffers(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	r := &TCP{}
	b := newCaptureBuffer(captureFunc(func([]byte, int) error {
		close(entered)
		<-release
		return nil
	}))
	b.sink = &r.captureSink
	b.point([]byte("same"), 1, 1)
	flushed := make(chan error, 1)
	go func() { flushed <- b.flush() }()
	<-entered
	sealed := make(chan struct{})
	switched := make(chan error, 1)
	var values []float64
	go func() {
		switched <- r.SwitchCapture(func(p *points.Points) {
			for _, d := range p.Data {
				values = append(values, d.Value)
			}
		}, func() error { close(sealed); return nil })
	}()
	select {
	case <-sealed:
		t.Fatal("sealed ahead of an in-flight capture submission")
	default:
	}
	close(release)
	if err := <-flushed; err != nil {
		t.Fatal(err)
	}
	if err := <-switched; err != nil {
		t.Fatal(err)
	}
	// A handler that buffered this record before publication flushes it to
	// ordinary Add before its next direct point, with no further spool write.
	b.point([]byte("same"), 2, 1)
	if err := b.flush(); err != nil {
		t.Fatal(err)
	}
	(*r.captureSink.direct.Load())(points.OnePoint("same", 3, 1))
	if len(values) != 2 || values[0] != 2 || values[1] != 3 {
		t.Fatal("buffered/direct order", values)
	}
}

func TestSwitchCaptureTimeoutDoesNotSealAfterReturning(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	r := &TCP{}
	b := newCaptureBuffer(captureFunc(func([]byte, int) error { close(entered); <-release; return nil }))
	b.sink = &r.captureSink
	b.point([]byte("quota.blocked"), 1, 1)
	flushed := make(chan error, 1)
	go func() { flushed <- b.flush() }()
	<-entered
	var seals atomic.Int64
	seal := func() error { seals.Add(1); return nil }
	if err := r.SwitchCapture(func(*points.Points) {}, seal); err == nil {
		t.Fatal("switch waited indefinitely behind capture")
	}
	if seals.Load() != 0 {
		t.Fatal("timed-out switch sealed input")
	}
	close(release)
	if err := <-flushed; err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for r.captureSink.gate.Load() != nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := r.SwitchCapture(func(*points.Points) {}, seal); err != nil {
		t.Fatal(err)
	}
	if seals.Load() != 1 {
		t.Fatal("cancelled switch ran its seal callback later", seals.Load())
	}
}

func TestSwitchCaptureTimeoutReleasesNewSubmissions(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	r := &TCP{}
	blocked := newCaptureBuffer(captureFunc(func([]byte, int) error { close(entered); <-release; return nil }))
	blocked.sink = &r.captureSink
	blocked.point([]byte("blocked"), 1, 1)
	flushed := make(chan error, 1)
	go func() { flushed <- blocked.flush() }()
	<-entered
	defer func() { close(release); <-flushed }()
	if err := r.SwitchCapture(func(*points.Points) {}, func() error { return nil }); err == nil {
		t.Fatal("accepted an in-flight capture submission")
	}
	next := newCaptureBuffer(captureFunc(func([]byte, int) error { return nil }))
	next.sink = &r.captureSink
	next.point([]byte("unblocked"), 2, 2)
	done := make(chan error, 1)
	go func() { done <- next.flush() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed-out switch still blocks new capture submissions")
	}
}

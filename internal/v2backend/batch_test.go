package v2backend

import (
	"context"
	"math"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func TestReceiverBatchPreservesBoundariesBitsAndRecovery(t *testing.T) {
	clock := int64(1794700800)
	scalar, e := openAt(testOptions(t.TempDir(), simplePolicy()), func() time.Time { return time.Unix(clock, 0) })
	if e != nil {
		t.Fatal(e)
	}
	defer requireClose(t, scalar)
	opts := testOptions(t.TempDir(), simplePolicy())
	batch, e := openAt(opts, func() time.Time { return time.Unix(clock, 0) })
	if e != nil {
		t.Fatal(e)
	}
	names := make([][]byte, 256)
	data := make([]points.Point, len(names))
	bits := []uint64{0x8000000000000000, 0x7ff8000000000042, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x3ff0000000000000}
	for i := range data {
		names[i] = []byte("batch.metric")
		data[i] = points.Point{Timestamp: clock - 60 + int64(i%30), Value: math.Float64frombits(bits[i%len(bits)])}
		if e := scalar.Append(&points.Points{Metric: string(names[i]), Data: data[i : i+1]}); e != nil {
			t.Fatal(e)
		}
	}
	if e = batch.AppendBatch(names, data); e != nil {
		t.Fatal(e)
	}
	cached, e := batch.CacheQuery(context.Background(), "batch.metric")
	if e != nil {
		t.Fatal(e)
	}
	if len(cached) != len(data) {
		t.Fatalf("unsealed cache=%d", len(cached))
	}
	for i, p := range cached {
		if p.Timestamp != data[i].Timestamp || math.Float64bits(p.Value) != math.Float64bits(data[i].Value) {
			t.Fatalf("cached receipt %d differs", i)
		}
	}
	clear(names)
	clear(data) // receiver immediately reuses every borrowed buffer
	want, e := scalar.Fetch(context.Background(), "batch.metric", int(clock-70), int(clock))
	if e != nil {
		t.Fatal(e)
	}
	defer want.Release()
	compare := func(s *Store) {
		t.Helper()
		got, e := s.Fetch(context.Background(), "batch.metric", int(clock-70), int(clock))
		if e != nil {
			t.Fatal(e)
		}
		defer got.Release()
		if !reflect.DeepEqual(got.Metric, want.Metric) || got.From != want.From || got.Until != want.Until || got.Step != want.Step || len(got.Values) != len(want.Values) {
			t.Fatal("grid differs")
		}
		for i, v := range got.Values {
			if math.Float64bits(v) != math.Float64bits(want.Values[i]) {
				t.Fatalf("slot%d bits differ", i)
			}
		}
	}
	compare(batch)
	if e = batch.Close(); e != nil {
		t.Fatal(e)
	}
	batch, e = openAt(opts, func() time.Time { return time.Unix(clock, 0) })
	if e != nil {
		t.Fatal(e)
	}
	defer requireClose(t, batch)
	if batch.Stats().IngestedPoints != 256 {
		t.Fatal(batch.Stats())
	}
	compare(batch)
}

func TestSealDoesNotBlockAdmissionAndCacheSeesFrozenHeads(t *testing.T) {
	s, e := openAt(testOptions(t.TempDir(), simplePolicy()), time.Now)
	if e != nil {
		t.Fatal(e)
	}
	defer requireClose(t, s)
	entered, release := make(chan struct{}), make(chan struct{})
	s.beforeSeal = func(int) { close(entered); <-release }
	p := points.Point{Timestamp: time.Now().Unix() - 10, Value: math.Copysign(0, -1)}
	if e = s.Append(&points.Points{Metric: "live.metric", Data: []points.Point{p}}); e != nil {
		t.Fatal(e)
	}
	sealed := make(chan error, 1)
	go func() { sealed <- s.Flush() }()
	<-entered
	admitted := make(chan error, 1)
	go func() { admitted <- s.Append(&points.Points{Metric: "live.metric", Data: []points.Point{p}}) }()
	select {
	case e := <-admitted:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("admission waited for seal")
	}
	cached, e := s.CacheQuery(context.Background(), "live.metric")
	if e != nil {
		t.Fatal(e)
	}
	if len(cached) != 2 {
		t.Fatalf("frozen+active cache=%d", len(cached))
	}
	close(release)
	if e := <-sealed; e != nil {
		t.Fatal(e)
	}
	s.beforeSeal = nil
	cached, e = s.CacheQuery(context.Background(), "live.metric")
	if e != nil {
		t.Fatal(e)
	}
	if len(cached) != 1 || math.Float64bits(cached[0].Value) != math.Float64bits(p.Value) {
		t.Fatal("active receipt lost")
	}
}

func TestMultiReceiptEnvelopeRejectsTruncation(t *testing.T) {
	s, e := openAt(testOptions(t.TempDir(), simplePolicy()), time.Now)
	if e != nil {
		t.Fatal(e)
	}
	defer requireClose(t, s)
	f, e := s.makeFrame("codec.metric", []points.Point{{Value: math.Float64frombits(0x7ff8000000000042), Timestamp: 123}}, 456)
	if e != nil {
		t.Fatal(e)
	}
	fs := []frame{f, f}
	wire, e := encodeFrames(fs)
	if e != nil {
		t.Fatal(e)
	}
	for i := range wire {
		if _, e := decodeFrames(wire[:i]); e == nil {
			t.Fatalf("accepted truncation%d", i)
		}
	}
	got, e := decodeFrames(wire)
	if e != nil {
		t.Fatal(e)
	}
	if len(got) != 2 || got[0].clock != 456 || !slices.Equal(got[0].payload, fs[0].payload) || math.Float64bits(got[1].points[0].Value) != 0x7ff8000000000042 {
		t.Fatal("receipt changed")
	}
}

func TestLegacyReceiptsAndDictionaryEpochRestart(t *testing.T) {
	clock := int64(1794700800)
	opts := testOptions(t.TempDir(), simplePolicy())
	open := func() *Store {
		t.Helper()
		s, e := openAt(opts, func() time.Time { return time.Unix(clock, 0) })
		if e != nil {
			t.Fatal(e)
		}
		return s
	}
	s := open()
	name := "epoch.\xff.metric"
	f, e := s.makeFrame(name, []points.Point{{Timestamp: clock - 10, Value: math.Copysign(0, -1)}}, clock)
	if e != nil {
		t.Fatal(e)
	}
	old, e := encodeFrame(f)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.log.Append(0, clock, old); e != nil {
		t.Fatal(e)
	}
	multi, e := encodeFrames([]frame{f})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.log.Append(0, clock, multi); e != nil {
		t.Fatal(e)
	}
	requireClose(t, s)
	for epoch := range 3 {
		s = open()
		if got := s.Stats().IngestedPoints; got != uint64(2+epoch) {
			t.Fatalf("epoch%d count=%d", epoch, got)
		}
		if e = s.Append(&points.Points{Metric: name, Data: []points.Point{{Timestamp: clock - 9 + int64(epoch), Value: math.Float64frombits(uint64(epoch) + 0x7ff8000000000042)}}}); e != nil {
			t.Fatal(e)
		}
		requireClose(t, s)
	}
	s = open()
	defer requireClose(t, s)
	got, e := s.Fetch(context.Background(), name, int(clock-11), int(clock))
	if e != nil {
		t.Fatal(e)
	}
	defer got.Release()
	if s.Stats().IngestedPoints != 5 || math.Float64bits(got.Values[0]) != 0x8000000000000000 {
		t.Fatal("old receipt bits/count changed")
	}
	for i := range 3 {
		if math.Float64bits(got.Values[i+1]) != uint64(i)+0x7ff8000000000042 {
			t.Fatalf("epoch%d payload changed", i)
		}
	}
}

func BenchmarkResolvedMetricCache(b *testing.B) {
	s := &Store{opts: testOptions("", simplePolicy())}
	name := []byte("namespace.cluster.host.component.requests.value")
	if _, e := s.resolveBytes(name); e != nil {
		b.Fatal(e)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, e := s.resolveBytes(name); e != nil {
			b.Fatal(e)
		}
	}
}

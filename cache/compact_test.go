package cache

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

// Float bit patterns that must survive exactly, including NaN payloads.
var specialBits = []uint64{
	0,                     // +0
	1 << 63,               // -0
	0x7ff0000000000000,    // +Inf
	0xfff0000000000000,    // -Inf
	0x7ff8000000000000,    // quiet NaN
	0x7ff8000000000001,    // quiet NaN with payload
	0x7ff0000000000001,    // signalling NaN
	0xfff8deadbeef0042,    // negative NaN with payload
	0x7fefffffffffffff,    // MaxFloat64
	1,                     // smallest denormal
	0x000fffffffffffff,    // largest denormal
	0x3ff0000000000000,    // 1
	math.Float64bits(0.1), // inexact decimal
	^uint64(0),            // all ones, a NaN
}

var specialTimestamps = []int64{
	0, 1, -1, 60, 1_760_000_000, math.MaxInt64, math.MinInt64, math.MaxInt64 - 1, math.MinInt64 + 1, 1 << 32, -(1 << 40),
}

func encodePoints(data []points.Point) compactPoints {
	var c compactPoints
	c.appendPoints(data)
	return c
}

// requireSameBits compares points by timestamp and exact value bits.
func requireSameBits(t testing.TB, got, want []points.Point) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("decoded %d points, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Timestamp != want[i].Timestamp || math.Float64bits(got[i].Value) != math.Float64bits(want[i].Value) {
			t.Fatalf("point %d: got (%d, %#x), want (%d, %#x)", i,
				got[i].Timestamp, math.Float64bits(got[i].Value), want[i].Timestamp, math.Float64bits(want[i].Value))
		}
	}
}

// requireRoundTrip also decodes every prefix from the buffer as it was when
// that prefix was complete, which is what an unlocked reader sees.
func requireRoundTrip(t testing.TB, data []points.Point) {
	t.Helper()
	var c compactPoints
	snapshots := make([]compactPoints, 0, len(data))
	for _, p := range data {
		c.append(p.Value, p.Timestamp)
		snapshots = append(snapshots, c)
	}
	decoded := appendDecoded(nil, c.buf, c.n)
	requireSameBits(t, decoded, data)
	for i, s := range snapshots {
		if s.n != i+1 {
			t.Fatalf("snapshot %d holds %d points", i, s.n)
		}
		requireSameBits(t, appendDecoded(decoded[:0], s.buf, s.n), data[:i+1])
	}
	if len(data) > 0 && firstTimestamp(c.buf) != data[0].Timestamp {
		t.Fatalf("first timestamp %d, want %d", firstTimestamp(c.buf), data[0].Timestamp)
	}
}

func TestCompactRoundTripSpecialValues(t *testing.T) {
	var data []points.Point
	for i, bits := range specialBits {
		for j, ts := range specialTimestamps {
			// Every value after every other, at every timestamp jump.
			data = append(data, points.Point{Value: math.Float64frombits(bits), Timestamp: ts})
			data = append(data, points.Point{Value: math.Float64frombits(specialBits[(i+j)%len(specialBits)]), Timestamp: ts})
		}
	}
	requireRoundTrip(t, data)
	requireRoundTrip(t, nil)
	requireRoundTrip(t, []points.Point{{}})
}

func TestCompactRoundTripOrderAnomalies(t *testing.T) {
	for name, timestamps := range map[string][]int64{
		"regular":      {60, 120, 180, 240, 300},
		"duplicates":   {60, 60, 60, 120, 120, 180},
		"out of order": {300, 240, 360, 120, 420, 420, 0},
		"negative":     {-60, -120, -180, -1, -1 << 62},
		"jumps":        {0, math.MaxInt64, math.MinInt64, 0, math.MaxInt64, -1, math.MinInt64, math.MaxInt64},
		"jitter":       {600, 659, 721, 780, 841, 900},
	} {
		t.Run(name, func(t *testing.T) {
			data := make([]points.Point, len(timestamps))
			for i, ts := range timestamps {
				data[i] = points.Point{Value: float64(i % 2), Timestamp: ts}
			}
			requireRoundTrip(t, data)
		})
	}
}

func TestCompactRoundTripRandom(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(1, 2))
	for round := 0; round < 300; round++ {
		n := rng.IntN(1000)
		data := make([]points.Point, n)
		g := newSeries(rng.Uint64())
		for i := range data {
			value, ts := g.next()
			switch rng.IntN(8) {
			case 0:
				value = math.Float64frombits(rng.Uint64())
			case 1:
				value = math.Float64frombits(specialBits[rng.IntN(len(specialBits))])
			case 2:
				ts = int64(rng.Uint64())
			case 3:
				ts = specialTimestamps[rng.IntN(len(specialTimestamps))]
			}
			data[i] = points.Point{Value: value, Timestamp: ts}
		}
		t.Run(fmt.Sprintf("round=%d", round), func(t *testing.T) {
			t.Parallel()
			requireRoundTrip(t, data)
		})
	}
}

// The encoding is sized for regular series: a repeated value at a regular
// step costs one byte.
func TestCompactSize(t *testing.T) {
	data := make([]points.Point, 100)
	for i := range data {
		data[i] = points.Point{Value: 42, Timestamp: 1_760_000_000 + 60*int64(i)}
	}
	c := encodePoints(data)
	// The first point stores the full timestamp and value, the second its step.
	if want := 1 + 5 + 2 + 2 + 98; len(c.buf) != want {
		t.Fatalf("encoded %d bytes, want %d", len(c.buf), want)
	}
}

// The fast and slow encoder paths write the same bytes at every capacity.
func TestCompactAppendAtCapacity(t *testing.T) {
	data := []points.Point{
		{Value: 1, Timestamp: 1_760_000_000},
		{Value: math.Float64frombits(0xfff8deadbeef0042), Timestamp: math.MinInt64},
		{Value: 1, Timestamp: 1_760_000_060},
		{Value: 1, Timestamp: 1_760_000_120},
		{Value: 0.1, Timestamp: 1_760_000_180},
	}
	want := compactPoints{buf: make([]byte, 0, 1024)}
	want.appendPoints(data)
	for capacity := 0; capacity <= len(want.buf)+maxPointBytes; capacity++ {
		c := compactPoints{buf: make([]byte, 0, capacity)}
		c.appendPoints(data)
		if !slices.Equal(c.buf, want.buf) {
			t.Fatalf("capacity %d: %x, want %x", capacity, c.buf, want.buf)
		}
	}
	requireRoundTrip(t, data)
}

func FuzzCompactRoundTrip(f *testing.F) {
	seed := func(pairs ...uint64) []byte {
		var b []byte
		for _, v := range pairs {
			b = binary.LittleEndian.AppendUint64(b, v)
		}
		return b
	}
	f.Add([]byte{})
	f.Add(seed(1_760_000_000, 0x3ff0000000000000, 1_760_000_060, 0x3ff0000000000000))
	f.Add(seed(uint64(math.MaxInt64), 1<<63, 1<<63, 0x7ff8000000000001, 0, 0xfff0000000000000))
	for _, bits := range specialBits {
		f.Add(seed(60, bits, 60, bits^1, 0, bits))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		// Each 16 bytes are a timestamp and a value's bits.
		data := make([]points.Point, len(raw)/16)
		for i := range data {
			ts := int64(binary.LittleEndian.Uint64(raw[16*i:]))
			bits := binary.LittleEndian.Uint64(raw[16*i+8:])
			data[i] = points.Point{Value: math.Float64frombits(bits), Timestamp: ts}
		}
		requireRoundTrip(t, data)
	})
}

// cacheModel is the reference for what every reader must observe: in-flight
// batches in pop order, then queued points, all in arrival order.
type cacheModel struct {
	queued   map[string][]points.Point
	inFlight map[string][]*points.Points
}

func (m *cacheModel) get(name string) []points.Point {
	var out []points.Point
	for _, p := range m.inFlight[name] {
		out = append(out, p.Data...)
	}
	return append(out, m.queued[name]...)
}

func TestCacheMatchesModel(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	c := New()
	c.SetMaxSize(0)
	m := &cacheModel{queued: map[string][]points.Point{}, inFlight: map[string][]*points.Points{}}
	names := make([]string, 40)
	gens := make([]*series, len(names))
	for i := range names {
		names[i] = fmt.Sprintf("model.metric.%02d", i)
		gens[i] = newSeries(uint64(i))
	}
	point := func(i int) points.Point {
		value, ts := gens[i].next()
		switch rng.IntN(10) {
		case 0:
			value = math.Float64frombits(specialBits[rng.IntN(len(specialBits))])
		case 1:
			ts = specialTimestamps[rng.IntN(len(specialTimestamps))]
		}
		return points.Point{Value: value, Timestamp: ts}
	}
	size := func() int64 {
		var n int64
		for _, data := range m.queued {
			n += int64(len(data))
		}
		return n
	}
	for step := 0; step < 50000; step++ {
		i := rng.IntN(len(names))
		name := names[i]
		switch op := rng.IntN(100); {
		case op < 60:
			p := point(i)
			c.AddPoint([]byte(name), p.Value, p.Timestamp)
			m.queued[name] = append(m.queued[name], p)
		case op < 70:
			batch := make([]points.Point, rng.IntN(5))
			for j := range batch {
				batch[j] = point(i)
			}
			c.Add(&points.Points{Metric: name, Data: slices.Clone(batch)})
			// An empty batch still creates an empty item.
			m.queued[name] = append(m.queued[name], batch...)
		case op < 75:
			p, ok := c.Pop(name)
			want, queued := m.queued[name]
			if ok != queued {
				t.Fatalf("step %d: Pop(%s) = %v, model has %v", step, name, ok, queued)
			}
			if ok {
				requireSameBits(t, p.Data, want)
				delete(m.queued, name)
			}
		case op < 85:
			p, ok := c.PopNotConfirmed(name)
			want, queued := m.queued[name]
			if ok != queued {
				t.Fatalf("step %d: PopNotConfirmed(%s) = %v, model has %v", step, name, ok, queued)
			}
			if ok {
				requireSameBits(t, p.Data, want)
				delete(m.queued, name)
				m.inFlight[name] = append(m.inFlight[name], p)
			}
		case op < 95:
			batches := m.inFlight[name]
			if len(batches) == 0 {
				continue
			}
			j := rng.IntN(len(batches))
			p := batches[j]
			m.inFlight[name] = slices.Delete(batches, j, j+1)
			if rng.IntN(2) == 0 {
				c.Confirm(p)
			} else {
				c.Requeue(p)
				m.queued[name] = append(slices.Clone(p.Data), m.queued[name]...)
			}
		default:
			requireSameBits(t, c.Get(name), m.get(name))
		}
		if step%1000 == 0 {
			if got, want := c.Size(), size(); got != want {
				t.Fatalf("step %d: size %d, want %d", step, got, want)
			}
		}
	}
	for _, name := range names {
		requireSameBits(t, c.Get(name), m.get(name))
	}
	dumped := map[string][]points.Point{}
	if err := c.DumpPoints(func(p *points.Points) error {
		dumped[p.Metric] = append(dumped[p.Metric], p.Data...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		requireSameBits(t, dumped[name], m.get(name))
	}
}

// Get decodes queued points after releasing the shard lock, while writers
// keep appending to the same buffer and the persister pops it.
func TestGetDecodesWhileAppending(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	value := func(ts int64) float64 { return float64(ts) * 1.1 }
	const writers, perWriter = 4, 20000
	var wg, readers sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < writers; w++ {
		name := fmt.Sprintf("concurrent.metric.%d", w)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ts := int64(0); ts < perWriter; ts++ {
				c.AddPoint([]byte(name), value(ts), ts)
				if ts%1000 == 999 {
					if p, ok := c.PopNotConfirmed(name); ok {
						c.Confirm(p)
					}
				}
			}
		}()
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				got := c.Get(name)
				for i, p := range got {
					if p.Timestamp != got[0].Timestamp+int64(i) || math.Float64bits(p.Value) != math.Float64bits(value(p.Timestamp)) {
						t.Errorf("%s: point %d is %v after %v", name, i, p, got[0])
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	readers.Wait()
}

func BenchmarkCompactAppend(b *testing.B) {
	g := newSeries(1)
	data := make([]points.Point, 480)
	for i := range data {
		data[i].Value, data[i].Timestamp = g.next()
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	var c compactPoints
	for i := 0; i < b.N; i++ {
		c = compactPoints{buf: c.buf[:0]}
		c.appendPoints(data)
	}
}

func BenchmarkCompactDecode(b *testing.B) {
	g := newSeries(1)
	var c compactPoints
	for i := 0; i < 480; i++ {
		c.append(g.next())
	}
	b.ReportAllocs()
	b.SetBytes(int64(c.n))
	out := make([]points.Point, 0, c.n)
	for i := 0; i < b.N; i++ {
		out = appendDecoded(out[:0], c.buf, c.n)
	}
}

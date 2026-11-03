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

// requireRoundTrip decodes snapshots as an unlocked reader sees them: every
// short prefix and a sample of longer prefixes, keeping fuzz work linear.
func requireRoundTrip(t testing.TB, data []points.Point) {
	t.Helper()
	var c compactPoints
	snapshots := make([]compactPoints, 0, len(data))
	for _, p := range data {
		c.append(p.Value, p.Timestamp)
		snapshots = append(snapshots, c)
	}
	decoded := c.appendDecoded(nil)
	requireSameBits(t, decoded, data)
	for i, s := range snapshots {
		if len(data) > 128 && i > 64 && i != len(data)-1 && i%max(1, len(data)/32) != 0 {
			continue
		}
		if s.n != i+1 {
			t.Fatalf("snapshot %d holds %d points", i, s.n)
		}
		requireSameBits(t, s.appendDecoded(decoded[:0]), data[:i+1])
	}
	if len(data) > 0 && c.firstTimestamp() != data[0].Timestamp {
		t.Fatalf("first timestamp %d, want %d", c.firstTimestamp(), data[0].Timestamp)
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

// Repeated values at a regular step cost two bits after the first step.
func TestCompactSize(t *testing.T) {
	data := make([]points.Point, 100)
	for i := range data {
		data[i] = points.Point{Value: 42, Timestamp: 1_760_000_000 + 60*int64(i)}
	}
	c := encodePoints(data)
	if got := c.encodedBytes(); got > 40 {
		t.Fatalf("encoded %d bytes, want <=40", got)
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
	for capacity := 0; capacity <= len(want.buf)+32; capacity++ {
		c := compactPoints{buf: make([]byte, 0, capacity)}
		c.appendPoints(data)
		if !slices.Equal(c.buf, want.buf) || c.tail != want.tail || c.used != want.used {
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
	for _, kind := range []string{"counter", "stable", "rounded", "float"} {
		for _, data := range compactWorkload(kind, 100)[:2] {
			var raw []byte
			for _, p := range data {
				raw = binary.LittleEndian.AppendUint64(raw, uint64(p.Timestamp))
				raw = binary.LittleEndian.AppendUint64(raw, math.Float64bits(p.Value))
			}
			f.Add(raw)
		}
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		// Bound the snapshot allocations per fuzz input, while covering
		// long series and every word offset. Each pair is timestamp/value bits.
		if len(raw) > 16*1024 {
			raw = raw[:16*1024]
		}
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
		out = c.appendDecoded(out[:0])
	}
}

// Every bit width at every offset, including the 64-bit spill path. Keeping
// snapshots alive also checks that filling the tail never rewrites old bytes.
func TestCompactBitWords(t *testing.T) {
	rng := rand.New(rand.NewPCG(17, 23))
	for offset := uint8(0); offset < 64; offset++ {
		var c compactPoints
		if offset != 0 {
			c.put(0, offset)
		}
		var values []uint64
		var widths []uint8
		for width := uint8(1); width <= 64; width++ {
			v := rng.Uint64() & (^uint64(0) >> (64 - width))
			c.put(v, width)
			values = append(values, v)
			widths = append(widths, width)
			saved := slices.Clone(c.buf)
			snapshot := c
			c.put(rng.Uint64(), 64)
			if !slices.Equal(saved, c.buf[:len(saved)]) {
				t.Fatal("published word changed")
			}
			c = snapshot
		}
		r := bitReader{buf: c.buf, tail: c.tail}
		if offset != 0 {
			r.get(offset)
		}
		for i, width := range widths {
			got := uint64(0)
			if width == 1 {
				got = r.bit()
			} else {
				got = r.get(width)
			}
			if got != values[i] {
				t.Fatalf("offset=%d width=%d: %#x want %#x", offset, width, got, values[i])
			}
		}
	}
}

func TestCompactNumericRuns(t *testing.T) {
	var data []points.Point
	ts := int64(1_760_000_000)
	add := func(v float64) { ts += 60; data = append(data, points.Point{Value: v, Timestamp: ts}) }
	// Enter decimals from zero, change scales, then leave and reenter numeric
	// runs. Large integer deltas exercise wraparound and every short-code edge.
	for _, step := range []float64{1, 0.01, 0.001} {
		add(0)
		for i := -40; i < 100; i++ {
			add(float64(i) * step)
		}
	}
	for _, v := range []float64{-0x1p63, 0x1p63 - 1024, 0x1p53, 0x1p53 + 2, 0x1p63, math.Inf(1), math.Copysign(0, -1), 0, 1, 1, 2, 4, 7, 11, 16, 22, 29, math.Float64frombits(0x7ff8000000000042), 0.1, 0.11, 0, 0.01} {
		add(v)
	}
	for _, d := range []int64{-65536, -32769, -32768, -129, -128, -9, -8, -1, 0, 1, 7, 8, 127, 128, 32767, 32768, math.MinInt64, math.MaxInt64} {
		add(float64(d))
		add(float64(-d))
		add(0)
	}
	requireRoundTrip(t, data)
}

func TestCompactTimestampCodeBoundaries(t *testing.T) {
	var data []points.Point
	var ts, delta int64
	for _, change := range []int64{-2049, -2048, -257, -256, -65, -64, -1, 0, 1, 63, 64, 255, 256, 2047, 2048, math.MaxInt64, math.MinInt64} {
		delta += change
		ts += delta
		data = append(data, points.Point{Value: 1, Timestamp: ts})
	}
	requireRoundTrip(t, data)
}

func TestCompactSnapshotsWithPartialWords(t *testing.T) {
	var c compactPoints
	c.buf = make([]byte, 0, 4096)
	var data []points.Point
	var snapshots []compactPoints
	var published [][]byte
	for i := 0; i < 500; i++ {
		value := float64(i)
		if i%3 == 0 {
			value = math.Float64frombits(uint64(i) * 0x9e3779b97f4a7c15)
		}
		p := points.Point{Value: value, Timestamp: int64(i)}
		c.append(p.Value, p.Timestamp)
		data = append(data, p)
		snapshots = append(snapshots, c)
		published = append(published, slices.Clone(c.buf))
	}
	for i := range snapshots {
		if !slices.Equal(snapshots[i].buf, published[i]) {
			t.Fatalf("snapshot %d bytes changed", i)
		}
		requireSameBits(t, snapshots[i].appendDecoded(nil), data[:i+1])
	}
}

func TestCompactMixedCapacity(t *testing.T) {
	for _, n := range []int{100, 4096} {
		data := compactWorkload("mix", n)
		capacity := 0
		for _, p := range data {
			c := encodePoints(p)
			capacity += cap(c.buf) + 8 // charge the entire unfinished word, even when empty
		}
		got := float64(capacity) / float64(n*len(data))
		t.Logf("n=%d retained stream capacity including tail %.4f B/point", n, got)
		if got > 1.90 {
			t.Fatalf("retained stream capacity including tail %.4f B/point exceeds 1.90", got)
		}
	}
}

func TestCompactIntegerDeltaOfDelta(t *testing.T) {
	data := make([]points.Point, 1000)
	value := int64(1_000_000)
	for i := range data {
		value += 1000 + int64(i%3)
		data[i] = points.Point{Value: float64(value), Timestamp: int64(i)}
	}
	c := encodePoints(data)
	requireSameBits(t, c.appendDecoded(nil), data)
	// The large delta changes by only one or two; delta-of-delta is smaller
	// than either a new integer delta or the float XOR window.
	if c.encodedBytes() > 2*len(data) {
		t.Fatalf("delta-of-delta run uses %.3f B/point", float64(c.encodedBytes())/float64(len(data)))
	}
}

func TestCompactNumericRunAfterRawEscape(t *testing.T) {
	data := make([]points.Point, 1000)
	for i := range data {
		v := float64(i%100+1) / 100
		if i%100 == 0 {
			v = math.Float64frombits(0x7ff8000000000042)
		}
		data[i] = points.Point{Value: v, Timestamp: int64(i)}
	}
	c := encodePoints(data)
	requireSameBits(t, c.appendDecoded(nil), data)
	if c.encodedBytes() > 2*len(data) {
		t.Fatalf("raw escape prevents numeric recovery: %.3f B/point", float64(c.encodedBytes())/float64(len(data)))
	}
}

// Exercise short numeric controls at many word offsets, including transitions
// to raw floats and back. The oracle is the exact source point/float bit stream;
// snapshots also verify the immutable-prefix contract on these fast paths.
func TestCompactNumericFastPathsAcrossWords(t *testing.T) {
	for prefix := 0; prefix < 64; prefix++ {
		for _, step := range []float64{1, 0.01, 0.001} {
			data := make([]points.Point, 0, 256)
			ts := int64(1_760_000_000)
			add := func(v float64) { ts += 60; data = append(data, points.Point{Value: v, Timestamp: ts}) }
			for i := 0; i < prefix; i++ {
				add(math.Float64frombits(uint64(i) * 0x9e3779b97f4a7c15))
			}
			value := 0
			for i := 0; i < 160; i++ {
				if i%31 == 0 {
					add(math.Float64frombits(specialBits[i%len(specialBits)]))
				}
				if i%29 == 0 {
					ts -= 1
				}
				value += i % 100
				add(float64(value) * step)
			}
			requireRoundTrip(t, data)
		}
	}
}

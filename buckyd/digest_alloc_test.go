package buckyd

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// digestSeriesReference is the original per-slot implementation. The served
// digest is a wire contract between replicas, so the fast path must match it
// byte for byte.
func digestSeriesReference(series *chunkstore.Series, from, until int64) DigestArchive {
	d := DigestArchive{Step: series.Step}
	for _, retention := range series.Metadata.Retentions {
		if retention.Step == series.Step {
			d.Points = retention.Count
			break
		}
	}
	h := sha256.New()
	var buf [16]byte
	step := int64(series.Step)
	for i, v := range series.Values {
		ts := int64(series.FromTime) + int64(i)*step
		if ts < from || ts >= until || math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		binary.BigEndian.PutUint64(buf[:8], uint64(ts))
		binary.BigEndian.PutUint64(buf[8:], math.Float64bits(v))
		_, _ = h.Write(buf[:])
		d.Count++
	}
	sum := h.Sum(nil)
	d.Hash = hex.EncodeToString(sum[:8])
	return d
}

func randomDigestSeries(rng *rand.Rand) *chunkstore.Series {
	steps := []int{1, 7, 10, 60, 600, 3600}
	step := steps[rng.Intn(len(steps))]
	n := rng.Intn(5000)
	if rng.Intn(8) == 0 {
		n = rng.Intn(4) // empty and tiny windows
	}
	from := (1_600_000_000 + rng.Intn(1_000_000)) / step * step
	values := make([]float64, n)
	density := rng.Float64()
	special := []float64{math.NaN(), math.Inf(1), math.Inf(-1), 0, math.Copysign(0, -1), math.MaxFloat64, math.SmallestNonzeroFloat64, math.Float64frombits(0x7ff0000000000001), math.Float64frombits(0xfff8000000000000)}
	for i := range values {
		switch {
		case rng.Float64() >= density:
			values[i] = math.NaN() // gap
		case rng.Intn(20) == 0:
			values[i] = special[rng.Intn(len(special))]
		default:
			values[i] = rng.NormFloat64() * 1e6
		}
	}
	return &chunkstore.Series{
		Metadata: chunkstore.Metadata{MetricConfig: chunkstore.MetricConfig{Retentions: []chunkstore.Retention{{Step: step, Count: 100}, {Step: step * 6, Count: 1000}}}},
		FromTime: from, UntilTime: from + n*step, Step: step, Values: values,
	}
}

// randomDigestWindow picks windows around, inside, across and outside the
// series, aligned and unaligned to its step.
func randomDigestWindow(rng *rand.Rand, s *chunkstore.Series) (int64, int64) {
	first, last := int64(s.FromTime), int64(s.UntilTime)
	span := last - first + int64(s.Step)*4
	pick := func() int64 {
		switch rng.Intn(5) {
		case 0:
			return first + int64(rng.Intn(3)-1)
		case 1:
			return last + int64(rng.Intn(3)-1)
		case 2:
			return first + int64(rng.Intn(int(span)+1))*int64(s.Step)/int64(max(1, s.Step))
		default:
			return first - int64(s.Step)*2 + rng.Int63n(span+1)
		}
	}
	from, until := pick(), pick()
	if from > until {
		from, until = until, from
	}
	if rng.Intn(10) == 0 {
		from, until = first-1000*int64(s.Step), last+1000*int64(s.Step) // whole series
	}
	return from, until
}

func TestDigestSeriesMatchesReference(t *testing.T) {
	rng := rand.New(rand.NewSource(20261121))
	for i := 0; i < 20000; i++ {
		s := randomDigestSeries(rng)
		from, until := randomDigestWindow(rng, s)
		got, want := digestSeries(s, from, until), digestSeriesReference(s, from, until)
		if got != want {
			t.Fatalf("case %d step %d from %d until %d series [%d,%d) n=%d: got %+v want %+v", i, s.Step, from, until, s.FromTime, s.UntilTime, len(s.Values), got, want)
		}
	}
}

func TestDigestSlotsEdges(t *testing.T) {
	for _, c := range []struct {
		first, step, from, until int64
		n, lo, hi                int
	}{
		{100, 10, 100, 130, 5, 0, 3},  // aligned
		{100, 10, 101, 130, 5, 1, 3},  // from just after a slot
		{100, 10, 99, 131, 5, 0, 4},   // from before, until just after a slot
		{100, 10, 0, 1000, 5, 0, 5},   // covers all
		{100, 10, 200, 300, 5, 5, 5},  // after the series
		{100, 10, -50, 90, 5, 0, 0},   // before the series
		{100, 10, 130, 120, 5, 3, 3},  // inverted window
		{-100, 7, -95, -80, 10, 1, 3}, // negative timestamps
	} {
		lo, hi := digestSlots(c.first, c.step, c.from, c.until, c.n)
		if lo != c.lo || hi != c.hi {
			t.Errorf("%+v: got [%d,%d)", c, lo, hi)
		}
	}
}

func BenchmarkDigestSeries(b *testing.B) {
	for _, density := range []float64{0.01, 0.2, 1} {
		// A 30-day verify window over a 60 s archive: 43,200 slots.
		rng := rand.New(rand.NewSource(1))
		values := make([]float64, 43200)
		for i := range values {
			values[i] = math.NaN()
			if rng.Float64() < density {
				values[i] = float64(rng.Intn(1000))
			}
		}
		s := &chunkstore.Series{Metadata: chunkstore.Metadata{MetricConfig: chunkstore.MetricConfig{Retentions: []chunkstore.Retention{{Step: 60, Count: 43200}}}}, FromTime: 1_700_000_040, Step: 60, Values: values}
		from, until := int64(1_700_000_000), int64(1_700_000_040+43200*60)
		b.Run(fmt.Sprintf("density=%g/fast", density), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				digestSeries(s, from, until)
			}
		})
		b.Run(fmt.Sprintf("density=%g/reference", density), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				digestSeriesReference(s, from, until)
			}
		})
	}
}

// BenchmarkServiceDigestLegacy serves legacy SHA digests for names x a
// 30-day window from a real store, the survivor's verify workload.
func BenchmarkServiceDigestLegacy(b *testing.B) {
	now := time.Unix(1_700_000_000, 0)
	db, err := chunkstore.Open(b.TempDir(), chunkstore.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, Now: func() time.Time { return now }, CacheSize: 64 << 20, MemTableSize: 16 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.StopBackground()
	s, err := New(Config{TmpDir: b.TempDir(), MaxBodyBytes: 1 << 20}, db)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	const names = 64
	retentions := []chunkstore.Retention{{Step: 60, Count: 43200}, {Step: 600, Count: 52560}}
	rng := rand.New(rand.NewSource(2))
	request := DigestRequest{From: now.Unix() - 30*86400 + 3600, Until: now.Unix() - 3600}
	for i := 0; i < names; i++ {
		name := fmt.Sprintf("bench.digest.n%03d", i)
		if _, err := db.Create(ctx, chunkstore.MetricConfig{Name: name, Retentions: retentions, AggregationMethod: chunkstore.Average}); err != nil {
			b.Fatal(err)
		}
		// Sparse history (about 5% of slots) plus a dense last day.
		var points []chunkstore.Point
		for ts := now.Unix() - 30*86400 + 60; ts < now.Unix(); ts += 60 {
			if ts > now.Unix()-86400 || rng.Intn(20) == 0 {
				points = append(points, chunkstore.Point{Timestamp: ts, Value: float64(rng.Intn(1000))})
			}
		}
		if err := db.UpdateMany(ctx, name, points); err != nil {
			b.Fatal(err)
		}
		request.Names = append(request.Names, name)
	}
	out, err := s.digest(ctx, request)
	if err != nil || len(out) != names || !out[0].Found || len(out[0].Archives) != 1 || out[0].Archives[0].Count == 0 {
		b.Fatalf("fixture digest %v %+v", err, out[:1])
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.digest(ctx, request); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*names), "ns/name")
}

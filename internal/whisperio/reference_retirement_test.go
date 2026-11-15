package whisperio

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func retirementFixture(t testing.TB, method store.AggregationMethod) (store.Metadata, mapContinuationIO, int64) {
	t.Helper()
	const now = int64(1794000000)
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "retirement", Retentions: []store.Retention{{Step: 60, Count: 2048}, {Step: 1800, Count: 512}}, AggregationMethod: method, XFilesFactor: .5}}
	archives := make([][]splicePart, len(m.Retentions))
	bases := make([]int64, len(archives))
	bits := []uint64{0, 1 << 63, 1, 0x7ff8000000000012, 0x7ff0000000000000, 0xfff0000000000000, 0x3ff0000000000001}
	for ai, r := range m.Retentions {
		points := make([]store.Point, r.Count)
		bases[ai] = now/int64(r.Step)*int64(r.Step) - int64(r.Count*r.Step)
		for i := range points {
			points[i] = store.Point{Timestamp: bases[ai] + int64(i*r.Step), Value: math.Float64frombits(bits[i%len(bits)])}
		}
		data, err := store.EncodeSealedPoints(points, r.Step)
		if err != nil {
			t.Fatal(err)
		}
		payload := &splicePayload{data: data, sealed: true}
		archives[ai] = []splicePart{{fragment: spliceFragment{first: bases[ai], count: r.Count, payload: payload}, slot: int(bases[ai] / int64(r.Step) % int64(r.Count)), count: r.Count}}
	}
	keys, err := (&ContinuationCodec{}).writeReferencedArchives(context.Background(), m, archives, bases, &store.NativeCombineClass{}, func() time.Time { return time.Unix(now, 0) })
	if err != nil {
		t.Fatal(err)
	}
	kv := mapContinuationIO{}
	for _, item := range keys {
		kv[string(item.Key)] = item.Value
	}
	return m, kv, now
}

func TestReferenceRetirementDifferential(t *testing.T) {
	for _, method := range []store.AggregationMethod{store.Average, store.Sum, store.Last, store.Min, store.Max, store.First} {
		t.Run(fmt.Sprint(method), func(t *testing.T) {
			m, seed, now := retirementFixture(t, method)
			a, b := copyIO(seed), copyIO(seed)
			codecs := []*ContinuationCodec{{keepReferenceRetirementReads: true, DirectLateWrites: true}, {DirectLateWrites: true}}
			bits := []uint64{0, 1 << 63, 1, 0x7ff8000000000012, 0x7ff0000000000000, 0xfff0000000000000, 0x3ff0000000000001}
			for round := range 12 {
				stamp := now + int64(round*120*60)
				input := make([]store.Point, 120)
				for i := range input {
					input[i] = store.Point{Timestamp: stamp - int64(120-i)*60, Value: math.Float64frombits(bits[(i+round)%len(bits)])}
				}
				if round%3 == 0 {
					input = append(input, store.Point{Timestamp: now - 7200, Value: float64(round)}, input[0])
				}
				for i, kv := range []*countingIO{a, b} {
					if err := codecs[i].Update(context.Background(), kv, m, input, -1, func() time.Time { return time.Unix(stamp+1, 0) }, nil); err != nil {
						t.Fatal(round, i, err)
					}
				}
				if err := sameKV(a.mapContinuationIO, b.mapContinuationIO); err != nil {
					t.Fatal(round, err)
				}
			}
		})
	}
}

func TestReferenceRetirementMemoBoundaries(t *testing.T) {
	_, kv, _ := retirementFixture(t, store.Last)
	counted := &nameCountingKV{mapContinuationIO: kv}
	fs := newContinuationFiles(counted, false)
	fs.reuseReferenceRetirements = true
	d, err := fs.load("main.wsp")
	if err != nil {
		t.Fatal(err)
	}
	f := &continuationFile{fs: fs, data: d}
	key := continuationBodyKey(d.id, 'p', 0, 0, 1)
	before := counted.gets
	for range 128 {
		if err := f.retireReferences(key); err != nil {
			t.Fatal(err)
		}
	}
	if n := counted.gets - before; n > 3 {
		t.Fatal("repeated successful retirement reads", n)
	}
	for i := range 130 {
		if err := f.retireReferences(continuationBodyKey(d.id, 'p', 0, 0, 100+i)); err != nil {
			t.Fatal(err)
		}
	}
	if len(d.retiredReferenceGroups) > 64 {
		t.Fatal("unbounded memo")
	}
	// A malformed alias must fail again rather than memoizing its error.
	bad := continuationBodyKey(d.id, 'p', 0, 0, 2)
	alias := append([]byte(nil), bad...)
	alias[9] = 'a'
	saved := kv[string(alias)]
	kv[string(alias)] = []byte{99}
	if err := f.retireReferences(bad); !errors.Is(err, store.ErrFormat) {
		t.Fatal(err)
	}
	kv[string(alias)] = saved
	if err := f.retireReferences(bad); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkReferenceRetirementMemo(b *testing.B) {
	for _, keep := range []bool{true, false} {
		b.Run(fmt.Sprintf("memo=%v", !keep), func(b *testing.B) {
			db, now := openReferencedProbeCodec(b, true, &ContinuationCodec{ReuseBlockKeys: true, keepReferenceRetirementReads: keep})
			input := make([]store.Point, 0, 120)
			before := db.Stats()
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if i%referencedProbeMetrics == 0 {
					*now += 1200
				}
				if err := db.UpdateMany(context.Background(), referencedProbeName(i%referencedProbeMetrics), referencedProbeInput(input, i, *now)); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			after := db.Stats()
			b.ReportMetric(float64(after.TableCacheHits+after.TableCacheMisses-before.TableCacheHits-before.TableCacheMisses)/float64(b.N), "tables/op")
			b.ReportMetric(float64(after.CacheHits+after.CacheMisses-before.CacheHits-before.CacheMisses)/float64(b.N), "blocks/op")
		})
	}
}

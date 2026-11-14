package whisperio

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// shaSeries follows buckyd.digestSeries: big-endian timestamp/float bits for
// finite points in the half-open window. Each archive is read independently.
func shaSeries(series *store.Series, from, until int) [32]byte {
	h := sha256.New()
	var buf [16]byte
	for i, v := range series.Values {
		ts := series.FromTime + i*series.Step
		if ts < from || ts >= until || math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		binary.BigEndian.PutUint64(buf[:8], uint64(ts))
		binary.BigEndian.PutUint64(buf[8:], math.Float64bits(v))
		h.Write(buf[:])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func sumBenchExport(tb testing.TB, db *store.Store) (*sumTestIO, store.Metadata) {
	tb.Helper()
	records, err := db.ExportTransfer(context.Background(), []string{"range.metric"})
	if err != nil {
		tb.Fatal(err)
	}
	kv := &sumTestIO{mapContinuationIO: mapContinuationIO{}}
	for _, entry := range records[0].Keys {
		kv.mapContinuationIO[string(entry.Key)] = entry.Value
	}
	m, err := db.Metadata(context.Background(), "range.metric")
	if err != nil {
		tb.Fatal(err)
	}
	return kv, m
}

// Real archive shape: 700 days of history, 2d/28d/2y retentions, native writer,
// real exact cold packing. Values are deterministic, not a production capture.
// codec-value-B/name counts encoded values delivered by the storage adapter,
// rather than pretending that cached LSM/page reads are physical device reads.
func BenchmarkFullHistoryDigest(b *testing.B) {
	for _, kind := range []string{"float", "integer", "integer-long-retentions"} {
		b.Run(kind, func(b *testing.B) { benchmarkFullHistoryDigest(b, kind) })
	}
}

func benchmarkFullHistoryDigest(b *testing.B, kind string) {
	db, codec, now := rangeFetchFixtureSchema(b, sumFixtureValues(kind), kind == "integer-long-retentions")
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(int64(now), 0) }
	kv, m := sumBenchExport(b, db)
	for _, mode := range []string{"sha256", "sum-warm"} {
		// Independently determine encoded source bytes with the very same KV image.
		kv.DeletePrefix(sumRevisionKey)
		if mode == "sum-warm" {
			if _, err := codec.SumDigest(ctx, kv, m, 0, 0, clock); err != nil {
				b.Fatal(err)
			}
		}
		kv.bytes = 0
		if mode == "sha256" {
			for _, r := range m.Retentions {
				lo, hi := store.DigestBounds(r, now, 0, 0)
				series, err := codec.Fetch(ctx, kv, m, lo-1, hi, clock)
				if err != nil {
					b.Fatal(err)
				}
				shaSeries(series, lo, hi)
			}
		} else {
			if _, err := codec.SumDigest(ctx, kv, m, 0, 0, clock); err != nil {
				b.Fatal(err)
			}
		}
		sourceBytes := kv.bytes
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			before := db.Stats().FS.Bytes[store.FSRead][store.FSTable]
			cpu := rangeCPUTime()
			for b.Loop() {
				if mode == "sha256" {
					for _, r := range m.Retentions {
						lo, hi := store.DigestBounds(r, now, 0, 0)
						series, err := db.Fetch(ctx, m.Name, lo-1, hi)
						if err != nil {
							b.Fatal(err)
						}
						shaSeries(series, lo, hi)
					}

				} else {
					if _, err := db.SumDigest(ctx, m.Name, 0, 0); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportMetric(float64(sourceBytes), "codec-value-B/name")
			b.ReportMetric(float64(rangeCPUTime()-cpu)/float64(b.N), "cpu-ns/name")
			after := db.Stats().FS.Bytes[store.FSRead][store.FSTable]
			b.ReportMetric(float64(after-before)/float64(b.N), "table-read-B/name")
		})
	}
}

func TestSumDigestFixtureDiskOverhead(t *testing.T) {
	for _, kind := range []string{"float", "integer", "integer-long-retentions"} {
		t.Run(kind, func(t *testing.T) { sumFixtureDiskOverhead(t, kind) })
	}
}

func sumFixtureValues(kind string) func(int) float64 {
	if kind == "float" {
		return nil
	}
	// Integer gauges with realistic entropy, rather than wide float mantissas.
	// The same native history and exact cold packer produce ~1.63 bytes/point.
	return func(ts int) float64 {
		x := uint64(ts)
		x ^= x >> 30
		x *= 0xbf58476d1ce4e5b9
		x ^= x >> 27
		x *= 0x94d049bb133111eb
		x ^= x >> 31
		return float64(x % 256)
	}
}

func sumFixtureDiskOverhead(t *testing.T, kind string) {
	db, codec, now := rangeFetchFixtureSchema(t, sumFixtureValues(kind), kind == "integer-long-retentions")
	ctx := context.Background()
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	liveBytes := func() int64 {
		var n int64
		for _, v := range db.Stats().LevelBytes {
			n += v
		}
		return n
	}
	before := liveBytes()
	kv, m := sumBenchExport(t, db)
	started := time.Now()
	digest, err := db.SumDigest(ctx, m.Name, 0, 0)
	firstNanos := time.Since(started).Nanoseconds()
	if err != nil {
		t.Fatal(err)
	}
	// The exported copy is the exact persisted source, so cache entry counts and
	// raw overhead can be inspected without adding a production debug API.
	if _, err := codec.SumDigest(ctx, kv, m, 0, 0, func() time.Time { return time.Unix(int64(now), 0) }); err != nil {
		t.Fatal(err)
	}
	firstValueBytes := kv.bytes
	kv.bytes = 0
	if _, err := codec.SumDigest(ctx, kv, m, 0, 0, func() time.Time { return time.Unix(int64(now), 0) }); err != nil {
		t.Fatal(err)
	}
	warmValueBytes := kv.bytes
	var points uint64
	for _, a := range digest {
		points += a.Count
	}
	entries, payload, logical := 0, 0, 0
	for k, v := range kv.mapContinuationIO {
		if len(k) >= 2 && k[:2] == "ds" {
			if len(v) != 8 && len(v) != 14 {
				t.Fatal("invalid digest cache value size")
			}
			entries++
			payload += len(v)
			logical += 17 + len(k) + len(v)
		}
	}
	if entries < 2 || points == 0 {
		t.Fatal("fixture did not fill cached time buckets")
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	after := liveBytes()
	delta := after - before
	t.Logf("first-fill-ns=%d first-codec-value-B/name=%d warm-codec-value-B/name=%d points=%d entries=%d sum-payload=%d logical-key+value=%d before-live-SST=%d after-live-SST=%d delta-SST=%d payload-B/point=%.8f logical-B/point=%.8f actual-SST-B/point=%.8f actual-disk-pct=%.6f", firstNanos, firstValueBytes, warmValueBytes, points, entries, payload, logical, before, after, delta, float64(payload)/float64(points), float64(logical)/float64(points), float64(delta)/float64(points), 100*float64(delta)/float64(before))
	// This is a prototype budget on this fixture, not a guarantee for arbitrary
	// compression, schema density or small databases with fixed table overhead.
	if delta < 0 {
		t.Fatal("compaction changed baseline; disk delta is inconclusive")
	}
	// Sparse counts and clipped edges extend the dense-only cache; bound their
	// measured SST cost, including extra keys, on the same compact fixtures.
	if float64(delta)/float64(before) > .003 {
		t.Fatal("fixture exceeds 0.30% disk budget")
	}
	records, err := db.ExportTransfer(ctx, []string{m.Name})
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range records[0].Keys {
		if len(entry.Key) >= 2 && string(entry.Key[:2]) == store.ContinuationDigestPrefix {
			t.Fatal("export leaked local digest cache")
		}
		if !bytes.Equal(entry.Value, kv.mapContinuationIO[string(entry.Key)]) {
			t.Fatal("digest changed source bytes")
		}
	}
}

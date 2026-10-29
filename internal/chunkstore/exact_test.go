package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkDecodeExactFloatChunk(b *testing.B) {
	var input, output chunk
	for i := range input.Points {
		input.set(i, Point{Timestamp: 1_792_400_400 + int64(i)*60, Value: float64(i) + .1})
	}
	encoded := encodeExactChunk(&input)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := decodeChunk(encoded, &output); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if input != output {
		b.Fatal("decoded chunk differs from input")
	}
}

func TestExactFloatChunkPlaneOrder(t *testing.T) {
	values := []uint64{math.Float64bits(.1), 0x8000000000000000, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042}
	for count := 0; count <= chunkSlots; count++ {
		var input, output chunk
		for i := range count {
			input.set(i*73%chunkSlots, Point{Timestamp: 1_792_400_400 + int64(i)*60, Value: math.Float64frombits(values[i%len(values)])})
		}
		if err := decodeChunk(encodeExactChunk(&input), &output); err != nil {
			t.Fatalf("%d present slots: %v", count, err)
		}
		assertChunkEqual(t, &input, &output)
	}
}

func TestExactChunkValueDomains(t *testing.T) {
	cases := []struct {
		name    string
		bits    []uint64
		integer bool
	}{
		{"integers", []uint64{0, math.Float64bits(-1), math.Float64bits(1 << 53), math.Float64bits(-(1 << 53))}, true},
		{"float", []uint64{math.Float64bits(0.1), math.Float64bits(1e100)}, false},
		{"special", []uint64{0x8000000000000000, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042, 0xfff0000000000001}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var want chunk
			for i := range chunkSlots {
				want.set(i, Point{Timestamp: int64(i)*60 + 1700000000, Value: math.Float64frombits(tc.bits[i%len(tc.bits)])})
			}
			encoded := encodeExactChunk(&want)
			if (encoded[1]&exactChunkInteger != 0) != tc.integer {
				t.Fatal("incorrect encoding selection")
			}
			var got chunk
			if err := decodeChunk(encoded, &got); err != nil {
				t.Fatal(err)
			}
			assertChunkEqual(t, &want, &got)
		})
	}
}

func TestExactChunkSparseAndTimestampWrap(t *testing.T) {
	for _, integer := range []bool{false, true} {
		var want, got chunk
		want.Deltas = 3
		want.set(0, Point{Timestamp: math.MaxInt64, Value: 7})
		want.set(127, Point{Timestamp: math.MinInt64, Value: -9})
		if !integer {
			want.Points[0].Value = math.Float64frombits(0x8000000000000000)
		}
		if err := decodeChunk(encodeExactChunk(&want), &got); err != nil {
			t.Fatal(err)
		}
		assertChunkEqual(t, &want, &got)
	}
}

func TestExactChunkCorruptionDoesNotPublish(t *testing.T) {
	var c chunk
	for i := range chunkSlots {
		c.set(i, Point{Timestamp: int64(i), Value: float64(i)})
	}
	valid := encodeExactChunk(&c)
	for i := range valid {
		bad := bytes.Clone(valid)
		bad[i] ^= 0x80
		// Some Zstd frame metadata can change without changing its content.
		// Successful decoding must still return the exact original content;
		// failed decoding must leave the destination untouched.
		var destination chunk
		destination.set(17, Point{Timestamp: -1, Value: 99})
		before := destination
		if err := decodeChunk(bad, &destination); err == nil {
			assertChunkEqual(t, &c, &destination)
		} else {
			assertChunkEqual(t, &before, &destination)
		}
	}
	badFlags := bytes.Clone(valid)
	badFlags[1] ^= exactChunkInteger
	if err := decodeChunk(badFlags, &c); err == nil {
		t.Fatal("changed interpretation accepted")
	}
	for n := range len(valid) {
		if err := decodeChunk(valid[:n], &c); err == nil {
			t.Fatalf("truncation %d accepted", n)
		}
	}
}

func TestExactStoreFormatIsolationAndRecovery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := Options{ExactBlocks: true, CacheSize: 8 << 20, MemTableSize: 4 << 20, Now: func() time.Time { return time.Unix(1700000000, 0) }}
	s, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, err = s.Create(ctx, MetricConfig{Name: "metric", Retentions: []Retention{{Step: 1, Count: 3600}}, AggregationMethod: Last})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		value := float64(i)
		if i == 11 {
			value = math.Float64frombits(0x8000000000000000)
		}
		if err = s.UpdateMany(ctx, "metric", []Point{{Timestamp: 1699999900, Value: value}, {Timestamp: 1699999800, Value: 42}}); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	marker, err := os.ReadFile(filepath.Join(dir, "CHUNKSTORE"))
	if err != nil || string(marker) != formatMarkerExact {
		t.Fatalf("marker %q: %v", marker, err)
	}
	if other, err := Open(dir, Options{}); !errors.Is(err, ErrFormat) {
		if other != nil {
			other.Close()
		}
		t.Fatalf("legacy mode accepted exact store: %v", err)
	}
	s, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	snap, err := s.Snapshot(ctx, "metric")
	if err != nil {
		t.Fatal(err)
	}
	got := snap.Archives[0].Points
	if len(got) != 2 || got[0].Timestamp != 1699999800 || got[0].Value != 42 || math.Float64bits(got[1].Value) != 0x8000000000000000 {
		t.Fatalf("lost OOO/correction after compaction/reopen: %+v", got)
	}
	legacy := t.TempDir()
	old, err := Open(legacy, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(legacy, opts); !errors.Is(err, ErrFormat) {
		if other != nil {
			other.Close()
		}
		t.Fatalf("exact mode accepted legacy store: %v", err)
	}
}

func TestExactStoreRejectsEarlierExperimentalFormat(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "CHUNKSTORE")
	marker := []byte("go-carbon-chunks-v3\n")
	if err := os.WriteFile(path, marker, 0600); err != nil {
		t.Fatal(err)
	}
	for _, exact := range []bool{false, true} {
		db, err := Open(dir, Options{ExactBlocks: exact})
		if db != nil {
			db.Close()
			t.Fatal("opened incompatible experimental store")
		}
		if !errors.Is(err, ErrFormat) {
			t.Fatalf("unexpected rejection: %v", err)
		}
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(marker, after) {
		t.Fatal("rejection changed the marker", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatal("rejection modified the store", err)
	}
}

func FuzzExactChunkRoundTrip(f *testing.F) {
	f.Add(uint64(0), uint64(0x8000000000000000), int64(1), int64(-1))
	f.Fuzz(func(t *testing.T, a, b uint64, ta, tb int64) {
		var want, got chunk
		want.set(0, Point{Timestamp: ta, Value: math.Float64frombits(a)})
		want.set(127, Point{Timestamp: tb, Value: math.Float64frombits(b)})
		if err := decodeChunk(encodeExactChunk(&want), &got); err != nil {
			t.Fatal(err)
		}
		assertChunkEqual(t, &want, &got)
	})
}

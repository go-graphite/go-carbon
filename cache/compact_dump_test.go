package cache

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

// writeCheckpoint dumps c in format f as a stop does, with an input file
// holding wal, publishes the checkpoint and opens it.
func writeCheckpoint(t testing.TB, c *Cache, f recovery.Format, wal []*points.Points) (*recovery.Bundle, string) {
	t.Helper()
	dir, root := t.TempDir(), t.TempDir()
	builder := recovery.NewBuilder(nil)
	dump, err := recovery.NewWriterFormat(filepath.Join(dir, recovery.CacheName(f, "1.2.bin")), 0, 1<<20, builder, f)
	if err != nil {
		t.Fatal(err)
	}
	input, err := recovery.NewWriter(filepath.Join(dir, "input.1.2.bin"), 1, 4096, builder)
	if err != nil {
		t.Fatal(err)
	}
	// Visit every shard, batching the tiny fixture's shards into segments so
	// a few records do not each pin a full-size writer buffer. Both halves
	// still have more segments than workers, including empty pending ranges.
	const parts = 16
	err = dump.WriteSegments(2*parts, 4, func(seg int, out *recovery.Segment) error {
		if seg < parts {
			return c.DumpPendingRange(seg, parts, out)
		}
		start, end := (seg-parts)*ShardCount/parts, (seg-parts+1)*ShardCount/parts
		if f.Compact() {
			return c.DumpShardsCompactTo(start, end, out)
		}
		return c.DumpShards(start, end, out.WritePoints)
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range wal {
		if err = input.WritePoints(p); err != nil {
			t.Fatal(err)
		}
	}
	cacheFile, err := dump.Close()
	if err != nil {
		t.Fatal(err)
	}
	walFile, err := input.Close()
	if err != nil {
		t.Fatal(err)
	}
	index, err := recovery.WriteIndex(dir, builder)
	if err != nil {
		t.Fatal(err)
	}
	if err = recovery.Publish(dir, root, cacheFile, walFile, index, "id"); err != nil {
		t.Fatal(err)
	}
	bundle, err := recovery.OpenBundle(dir, root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bundle.Close() })
	return bundle, filepath.Join(dir, cacheFile.Name)
}

func randomCache(t testing.TB, metrics int) (*Cache, map[string][]points.Point) {
	t.Helper()
	r := rand.New(rand.NewPCG(7, 8))
	c := New()
	c.SetMaxSize(0)
	want := make(map[string][]points.Point)
	for i := range metrics {
		name := fmt.Sprintf("metric.%d", i)
		ts := int64(1_700_000_000)
		v := float64(r.IntN(100))
		for range 1 + r.IntN(30) {
			ts += 10 + int64(r.IntN(3))*int64(r.IntN(2))
			switch r.IntN(3) {
			case 0:
				v += float64(r.IntN(10))
			case 1:
				v = r.Float64()
			}
			c.Add(points.OnePoint(name, v, ts))
			want[name] = append(want[name], points.Point{Value: v, Timestamp: ts})
		}
	}
	// One in-flight batch: popped by the persister, not yet confirmed.
	if p, ok := c.PopNotConfirmed("metric.1"); ok {
		c.Add(points.OnePoint("metric.1", 42, 1_800_000_000))
		want["metric.1"] = append(append([]points.Point(nil), p.Data...), points.Point{Value: 42, Timestamp: 1_800_000_000})
	}
	return c, want
}

func sameValues(a, b []points.Point) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Timestamp != b[i].Timestamp || math.Float64bits(a[i].Value) != math.Float64bits(b[i].Value) {
			return false
		}
	}
	return true
}

// The cache's encoding is the compact dump's record payload: dumps, the read
// overlay and claims must reproduce every point, in every format.
func TestCompactDumpCheckpointRoundTrip(t *testing.T) {
	t.Parallel()
	for _, f := range []recovery.Format{recovery.FormatLegacy, recovery.FormatCompact, recovery.FormatBlocks} {
		t.Run(f.String(), func(t *testing.T) {
			t.Parallel()
			c, want := randomCache(t, 500)
			wal := []*points.Points{points.OnePoint("metric.2", 7, 1_900_000_000), points.OnePoint("new.metric", 8, 1_900_000_000)}
			want["metric.2"] = append(want["metric.2"], wal[0].Data...)
			want["new.metric"] = wal[1].Data
			bundle, path := writeCheckpoint(t, c, f, wal)
			if bundle.CacheFormat() != f {
				t.Fatal("checkpoint format", bundle.CacheFormat())
			}
			// Ordered restore of the dump file alone.
			restored := map[string][]points.Point{}
			visit := func(p *points.Points) { restored[p.Metric] = append(restored[p.Metric], p.Data...) }
			var err error
			switch f {
			case recovery.FormatCompact:
				err = recovery.ReadCompactFile(path, visit)
			case recovery.FormatBlocks:
				err = recovery.ReadBlocksFile(path, visit)
			default:
				err = points.ReadFromFile(path, visit)
			}
			if err != nil || len(restored) != len(want)-1 {
				t.Fatal("ordered restore", len(restored), err)
			}

			next := New()
			next.SetMaxSize(0)
			if err := next.AttachPendingRecovery(bundle); err != nil {
				t.Fatal(err)
			}
			// Read overlay before any claim, then a live write claiming
			// its history, then background recovery of the rest.
			for name, data := range want {
				if got := next.Get(name); !sameValues(got, data) {
					t.Fatal("read overlay", name, len(got), len(data))
				}
			}
			next.Add(points.OnePoint("metric.3", 1, 2_000_000_000))
			want["metric.3"] = append(want["metric.3"], points.Point{Value: 1, Timestamp: 2_000_000_000})
			if err := next.RecoverPending(make(chan struct{}), 0); err != nil {
				t.Fatal(err)
			}
			for name, data := range want {
				if got := next.Get(name); !sameValues(got, data) {
					t.Fatal("claimed history", name, len(got), len(data))
				}
			}
			// A claimed compact item keeps appending after the copied bytes.
			next.Add(points.OnePoint("metric.4", 2, 2_100_000_000))
			if got := next.Get("metric.4"); !sameValues(got, append(want["metric.4"], points.Point{Value: 2, Timestamp: 2_100_000_000})) {
				t.Fatal("append after claim")
			}

			// A stop during recovery copies unclaimed metrics into the next
			// dump in its own format, whatever the checkpoint's: a release
			// writing blocks takes over a compact checkpoint.
			other := map[recovery.Format]recovery.Format{recovery.FormatLegacy: recovery.FormatCompact,
				recovery.FormatCompact: recovery.FormatBlocks, recovery.FormatBlocks: recovery.FormatLegacy}[f]
			pending := New()
			pending.SetMaxSize(0)
			if err := pending.AttachPendingRecovery(bundle); err != nil {
				t.Fatal(err)
			}
			again, _ := writeCheckpoint(t, pending, other, nil)
			for name, data := range want {
				if name == "metric.3" {
					data = data[:len(data)-1]
				}
				slot, ok, err := again.Find(name)
				if err != nil || !ok {
					t.Fatal("rewritten checkpoint lacks", name, err)
				}
				p, err := again.Read(slot)
				if err != nil || !sameValues(p.Data, data) {
					t.Fatal("rewritten checkpoint", name, err)
				}
			}
		})
	}
}

func TestBlocksDumpWithConcurrentReads(t *testing.T) {
	t.Parallel()
	c, want := randomCache(t, 500)
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for {
				for name, data := range want {
					if !sameValues(c.Get(name), data) {
						t.Error("concurrent read differs", name)
						return
					}
				}
				select {
				case <-stop:
					return
				default:
				}
			}
		})
	}
	defer func() { close(stop); readers.Wait() }()
	bundle, _ := writeCheckpoint(t, c, recovery.FormatBlocks, nil)
	for name, data := range want {
		slot, found, err := bundle.Find(name)
		if err != nil || !found {
			t.Fatal("dump lookup", name, found, err)
		}
		p, err := bundle.Read(slot)
		if err != nil || !sameValues(p.Data, data) {
			t.Fatal("dump with concurrent reads differs", name, err)
		}
	}
}

// Points the cache encoder appends decode with the dump's checked decoder to
// the same values and encoder state, and the dump's encoder writes the same
// bytes as the cache's.
func TestCacheEncodingIsCompactDumpEncoding(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(1, 1))
	reader := bufio.NewReaderSize(nil, points.MB)
	for range 1000 {
		var enc compactPoints
		var data []points.Point
		ts := int64(r.Uint64() >> 2)
		for range 1 + r.IntN(40) {
			ts += int64(r.IntN(40)) - 5
			v := math.Float64frombits(r.Uint64())
			if r.IntN(3) == 0 {
				v = float64(r.IntN(5))
			}
			enc.append(v, ts)
			data = append(data, points.Point{Value: v, Timestamp: ts})
		}
		if got := recovery.AppendCompactPoints(nil, data); string(got) != string(enc.buf) {
			t.Fatal("dump encoder differs from the cache encoder")
		}
		raw := recovery.AppendCompactRecord(nil, "m", enc.buf, enc.n)
		reader.Reset(bytes.NewReader(raw))
		var got []points.Point
		if err := recovery.ReadCompact(reader, func(p *points.Points) { got = p.Data }); err != nil || !sameValues(got, data) {
			t.Fatal("decoded points differ", err)
		}
	}
}

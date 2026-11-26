package v2core

// BenchmarkRenderPerPart measures what one render pays per live seal part: the
// production read cursor (sstable reader with a shared block cache, tableCursor,
// headerCursor), the metadata envelope seek and the data run seek of one name,
// as RollupSnapshot.eachPartRecord does per part. Parts are written by the
// production seal writer from the production planner. Names are random and the
// block cache is small, so most seeks miss, as in a render working set larger
// than the cache. RPP_NAMES (default 104000, about one owner's names) and
// RPP_CACHE_MIB (default 8) and RPP_POINTS per name (default 3) pick the shape.
// Run: go test -run '^$' -bench RenderPerPart ./internal/v2core/

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func BenchmarkRenderPerPart(b *testing.B) {
	names, cacheMiB, points := 104000, 8, 3
	if v := os.Getenv("RPP_NAMES"); v != "" {
		names, _ = strconv.Atoi(v)
	}
	if v := os.Getenv("RPP_CACHE_MIB"); v != "" {
		cacheMiB, _ = strconv.Atoi(v)
	}
	if v := os.Getenv("RPP_POINTS"); v != "" {
		points, _ = strconv.Atoi(v)
	}
	const maxParts = 15
	shape := sealCPUShape{"single10s", []sealrollup.Archive{{Step: 10, Count: 8640}}, sealrollup.Last, points, 10}
	r, batches, _ := sealCPUFixture(b, shape, names)
	plan, err := r.planParts(SealInput{Batches: batches})
	if err != nil {
		b.Fatal(err)
	}
	defer plan.Abort()
	if len(plan.Parts) != 1 {
		b.Fatalf("plan has %d parts, want one scope", len(plan.Parts))
	}
	dir := b.TempDir()
	cache := pebble.NewCache(int64(cacheMiB) << 20)
	defer cache.Unref()
	readers := make([]*sstable.Reader, 0, maxParts)
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	var stored uint64
	for p := 0; p < maxParts; p++ {
		part := plan.Parts[0]
		part.Spec.Source.Incarnation, part.Spec.Source.Stream = v2part.PartID{1}, "bench"
		path := filepath.Join(dir, fmt.Sprintf("p%02d.sst", p))
		d, err := writePlannedRollupTuples(path, part)
		if err != nil {
			b.Fatal(err)
		}
		stored = d.Bytes
		f, err := os.Open(path)
		if err != nil {
			b.Fatal(err)
		}
		readable, err := sstable.NewSimpleReadable(f)
		if err != nil {
			b.Fatal(err)
		}
		reader, err := sstable.NewReader(readable, sstable.ReaderOptions{Cache: cache})
		if err != nil {
			b.Fatal(err)
		}
		readers = append(readers, reader)
	}
	open := func(r *sstable.Reader) *headerCursor {
		it, err := r.NewIterWithBlockPropertyFiltersAndContextEtc(b.Context(), nil, nil, nil, false, true, nil, sstable.TrivialReaderProvider{Reader: r})
		if err != nil {
			b.Fatal(err)
		}
		return newHeaderCursor(&tableCursor{it: it}, nil)
	}
	// Each name's first run key and its envelope key, from the logical stream.
	var firsts, envs [][]byte
	c := open(readers[0])
	var first []byte
	for ok := c.First(); ok; ok = c.Next() {
		k := c.Key()
		if isEnvelopeKey(k, partKeyTailOf(k)) {
			if first != nil {
				firsts, envs = append(firsts, first), append(envs, bytes.Clone(k))
			}
			first = nil
		} else if first == nil {
			first = bytes.Clone(k)
		}
	}
	if err := c.Error(); err != nil {
		b.Fatal(err)
	}
	_ = c.Close()
	if len(envs) != names {
		b.Fatalf("found %d envelopes, want %d", len(envs), names)
	}
	b.Logf("seal part: %d names, %d bytes, %d data blocks", names, stored, readers[0].Properties.NumDataBlocks)
	for _, n := range []int{3, 7, 15} {
		b.Run(fmt.Sprintf("parts%d", n), func(b *testing.B) {
			rng := rand.New(rand.NewSource(int64(n)))
			for i := 0; i < b.N; i++ {
				name := rng.Intn(names)
				for _, r := range readers[:n] {
					c := open(r)
					if !c.SeekGE(envs[name]) || !bytes.Equal(c.Key(), envs[name]) {
						b.Fatalf("envelope %d: %v", name, c.Error())
					}
					if _, _, err := decodeRollupMetadataHeader(c.Value()); err != nil {
						b.Fatal(err)
					}
					if !c.SeekGE(firsts[name]) || !bytes.Equal(c.Key(), firsts[name]) || len(c.Value()) == 0 {
						b.Fatalf("run %d: %v", name, c.Error())
					}
					_ = c.Close()
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/part")
			b.ReportMetric(float64(stored), "sealB/part")
		})
	}
}

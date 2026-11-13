package chunkstore

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/golang/snappy"
)

// Scale down the cache and index together to reproduce an uncachable large
// Snappy top-level index. Real SST iterators must return exactly the same bytes.
func recoveryIndexFixture(tb testing.TB) (vfs.FS, []byte, int) {
	tb.Helper()
	fs := vfs.NewMem()
	f, err := fs.Create("/index.sst")
	if err != nil {
		tb.Fatal(err)
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{Comparer: storeComparer, MergerName: exactChunkMerger.Name, Compression: sstable.SnappyCompression, BlockSize: 128, IndexBlockSize: 128})
	prefix := "m/" + strings.Repeat("component.", 24)
	var first []byte
	for i := 0; i < 16000; i++ {
		key := []byte(fmt.Sprintf("%s%08d", prefix, i))
		if i == 0 {
			first = key
		}
		if err = w.Set(key, []byte("exact stored value")); err != nil {
			tb.Fatal(err)
		}
	}
	if err = w.Close(); err != nil {
		tb.Fatal(err)
	}
	f, err = fs.Open("/index.sst")
	if err != nil {
		tb.Fatal(err)
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		tb.Fatal(err)
	}
	r, err := sstable.NewReader(readable, sstable.ReaderOptions{Comparer: storeComparer, MergerName: exactChunkMerger.Name})
	if err != nil {
		tb.Fatal(err)
	}
	layout, err := r.Layout()
	if err != nil {
		tb.Fatal(err)
	}
	raw := make([]byte, layout.TopIndex.Length)
	if _, err = f.ReadAt(raw, int64(layout.TopIndex.Offset)); err != nil {
		tb.Fatal(err)
	}
	decoded, err := snappy.DecodedLen(raw)
	if err != nil {
		tb.Fatal(err)
	}
	if err = r.Close(); err != nil {
		tb.Fatal(err)
	}
	return fs, first, decoded
}

func BenchmarkRecoveryIndexCache(b *testing.B) {
	fs, key, size := recoveryIndexFixture(b)
	// Old shards cannot retain the top index; new shards can. Keep total
	// capacity fixed and preserve a substantial number of independent locks.
	const oldShards, newShards = 768, 356
	budget := int64(size * 512)
	for _, shards := range []int{oldShards, newShards} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			c := pebble.NewCacheWithShards(budget, shards)
			defer c.Unref()
			f, err := fs.Open("/index.sst")
			if err != nil {
				b.Fatal(err)
			}
			readable, err := sstable.NewSimpleReadable(f)
			if err != nil {
				b.Fatal(err)
			}
			r, err := sstable.NewReader(readable, sstable.ReaderOptions{Cache: c, Comparer: storeComparer, MergerName: exactChunkMerger.Name})
			if err != nil {
				b.Fatal(err)
			}
			defer r.Close()
			var failures atomic.Int64
			read := func() {
				it, e := r.NewIter(nil, nil)
				if e != nil {
					failures.Add(1)
					return
				}
				k, v := it.First()
				if k == nil || string(k.UserKey) != string(key) || string(v.InPlaceValue()) != "exact stored value" {
					failures.Add(1)
				}
				if it.Close() != nil {
					failures.Add(1)
				}
			}
			read()
			before := c.Metrics()
			b.ReportAllocs()
			b.ResetTimer()
			var next atomic.Int64
			var wg sync.WaitGroup
			for range 32 {
				wg.Go(func() {
					for {
						i := next.Add(1)
						if i > int64(b.N) {
							return
						}
						read()
					}
				})
			}
			wg.Wait()
			b.StopTimer()
			if failures.Load() != 0 {
				b.Fatal("stored bytes differ")
			}
			after := c.Metrics()
			b.ReportMetric(float64(after.Misses-before.Misses)/float64(b.N), "block-misses/op")
			b.ReportMetric(float64(size), "index-bytes")
		})
	}
}

func TestRecoveryIndexCacheRetainsLargeIndex(t *testing.T) {
	fs, key, size := recoveryIndexFixture(t)
	budget := int64(size) * 512
	for _, shards := range []int{768, 356} {
		c := pebble.NewCacheWithShards(budget, shards)
		f, err := fs.Open("/index.sst")
		if err != nil {
			t.Fatal(err)
		}
		readable, err := sstable.NewSimpleReadable(f)
		if err != nil {
			t.Fatal(err)
		}
		r, err := sstable.NewReader(readable, sstable.ReaderOptions{Cache: c, Comparer: storeComparer, MergerName: exactChunkMerger.Name})
		if err != nil {
			t.Fatal(err)
		}
		read := func() {
			it, e := r.NewIter(nil, nil)
			if e != nil {
				t.Fatal(e)
			}
			k, v := it.First()
			if k == nil || string(k.UserKey) != string(key) || string(v.InPlaceValue()) != "exact stored value" {
				t.Fatal("stored bytes changed")
			}
			if e = it.Close(); e != nil {
				t.Fatal(e)
			}
		}
		read()
		before := c.Metrics()
		for range 32 {
			read()
		}
		after := c.Metrics()
		if shards == 356 && after.Misses != before.Misses {
			t.Fatalf("index not retained: %+v -> %+v", before, after)
		}
		if shards == 768 && after.Misses-before.Misses != 32 {
			t.Fatal("fixture did not reproduce one index miss per lookup")
		}
		if after.Size > budget {
			t.Fatal("cache exceeded its byte budget")
		}
		if err = r.Close(); err != nil {
			t.Fatal(err)
		}
		c.Unref()
	}
}

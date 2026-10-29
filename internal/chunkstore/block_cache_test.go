package chunkstore

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

// A real range-deletion SST exercises the failure seen on the 192-core canary:
// a block larger than an individual cache shard is rejected on every read even
// when the total cache has ample capacity. Compare identical encoded tombstones
// through repeated concurrent reads with the same total cache budget.
func TestExactCacheRetainsLargeRangeDeletionBlock(t *testing.T) {
	fs := vfs.NewMem()
	f, err := fs.Create("/ranges.sst")
	if err != nil {
		t.Fatal(err)
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{Compression: sstable.NoCompression})
	const tombstones = 12000
	for i := range tombstones {
		start := []byte(fmt.Sprintf("%08d/range-start", i))
		end := []byte(fmt.Sprintf("%08d/range-stop", i))
		if err := w.DeleteRange(start, end); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	const budget = 64 << 20
	var oracle [32]byte
	for _, bounded := range []bool{false, true} {
		c := pebble.NewCacheWithShards(budget, 768)
		var ranges *pebble.Cache
		if bounded {
			c.Unref()
			c, ranges, _ = newExactBlockCaches(budget, 0)
		}
		func() {
			defer c.Unref()
			if ranges != nil {
				defer ranges.Unref()
			}
			f, err := fs.Open("/ranges.sst")
			if err != nil {
				t.Fatal(err)
			}
			readable, err := sstable.NewSimpleReadable(f)
			if err != nil {
				t.Fatal(err)
			}
			r, err := sstable.NewReader(readable, sstable.ReaderOptions{Cache: c, RangeDelCache: ranges})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			layout, err := r.Layout()
			if err != nil || layout.RangeDel.Length <= budget/768 || layout.RangeDel.Length >= budget/4 {
				t.Fatalf("fixture must exceed the old shard capacity and fit the new one: %+v, %v", layout, err)
			}
			read := func() ([32]byte, error) {
				it, err := r.NewRawRangeDelIter()
				if err != nil {
					return [32]byte{}, err
				}
				defer it.Close()
				h := sha256.New()
				count := 0
				var b [8]byte
				for span := it.First(); span != nil; span = it.Next() {
					h.Write(span.Start)
					h.Write(span.End)
					for _, key := range span.Keys {
						binary.LittleEndian.PutUint64(b[:], key.Trailer)
						h.Write(b[:])
					}
					count++
				}
				if count != tombstones {
					return [32]byte{}, fmt.Errorf("got %d tombstones, want %d", count, tombstones)
				}
				return [32]byte(h.Sum(nil)), it.Error()
			}
			warm, err := read()
			if err != nil {
				t.Fatal(err)
			}
			if !bounded {
				oracle = warm
			} else if warm != oracle {
				t.Fatal("cache policy changed range deletions")
			}
			measured := c
			if ranges != nil {
				measured = ranges
				if c.MaxSize()+ranges.MaxSize() != budget {
					t.Fatal("split caches exceeded the configured budget")
				}
			}
			before := measured.Metrics()
			var wg sync.WaitGroup
			const readers, rounds = 8, 4
			for range readers {
				wg.Go(func() {
					for range rounds {
						got, err := read()
						if err != nil || got != oracle {
							t.Errorf("concurrent range read differs: %x: %v", got, err)
						}
					}
				})
			}
			wg.Wait()
			after := measured.Metrics()
			if bounded {
				if after.Hits-before.Hits != readers*rounds || after.Misses != before.Misses || after.Size > budget {
					t.Fatalf("large block was not retained within budget: before=%+v, after=%+v", before, after)
				}
			} else if after.Misses-before.Misses != readers*rounds {
				t.Fatalf("fixture did not reproduce rejected oversized cache block: before=%+v, after=%+v", before, after)
			}
		}()
	}
}

// Two databases may share only their deletion cache. Their primary cache IDs
// and physical file numbers can coincide, but one database's tombstones must
// never be returned to the other. Also exercise close/reopen, compaction and
// release of the caller's cache reference while both databases remain open.
func TestRangeDeletionCacheSeparatesDatabases(t *testing.T) {
	fs := vfs.NewMem()
	ranges := pebble.NewCacheWithShards(16<<20, 4)
	owned := true
	defer func() {
		if owned {
			ranges.Unref()
		}
	}()
	for round := range 2 {
		func() {
			aCache, bCache := pebble.NewCache(16<<20), pebble.NewCache(16<<20)
			defer aCache.Unref()
			defer bCache.Unref()
			a, err := pebble.Open("/a", &pebble.Options{FS: fs, Cache: aCache, RangeDelCache: ranges})
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			b, err := pebble.Open("/b", &pebble.Options{FS: fs, Cache: bCache, RangeDelCache: ranges})
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			if round == 1 {
				ranges.Unref()
				owned = false
			}
			if round == 0 {
				for _, db := range []*pebble.DB{a, b} {
					if err := db.Set([]byte("a1"), []byte("value"), pebble.Sync); err != nil {
						t.Fatal(err)
					}
					if err := db.Set([]byte("x1"), []byte("alive"), pebble.Sync); err != nil {
						t.Fatal(err)
					}
				}
				if err := a.DeleteRange([]byte("a0"), []byte("a3"), pebble.Sync); err != nil {
					t.Fatal(err)
				}
				for _, span := range [][2]string{{"a0", "a0z"}, {"a2", "a3"}} {
					if err := b.DeleteRange([]byte(span[0]), []byte(span[1]), pebble.Sync); err != nil {
						t.Fatal(err)
					}
				}
				for _, db := range []*pebble.DB{a, b} {
					if err := db.Flush(); err != nil {
						t.Fatal(err)
					}
				}
			}
			for range 8 {
				if _, closer, err := a.Get([]byte("a1")); !errors.Is(err, pebble.ErrNotFound) {
					if closer != nil {
						_ = closer.Close()
					}
					t.Fatalf("deleted value reappeared: %v", err)
				}
				value, closer, err := b.Get([]byte("a1"))
				if err != nil {
					t.Fatalf("another database's deletion hid this value: %v", err)
				}
				if string(value) != "value" {
					t.Errorf("wrong value: %q", value)
				}
				if err := closer.Close(); err != nil {
					t.Fatal(err)
				}
			}
			for _, db := range []*pebble.DB{a, b} {
				if err := db.Compact([]byte("a"), []byte("zz"), true); err != nil {
					t.Fatal(err)
				}
			}
		}()
	}
}

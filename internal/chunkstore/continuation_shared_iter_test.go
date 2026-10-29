package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"
)

// An update walking and probing through the shared iterator must observe
// exactly what fresh lookups observe: reads before the batch's first write and
// after later writes, reads from inside a Visit callback (also after the
// callback wrote), time-bucketed keys, and reads around batch range deletions. Each
// round compares both ios over identical batches on a metric whose committed
// versions are spread over the memtable, L0 sublevels and lower levels, with
// neighbouring generations inside the tables that hold it.
func TestContinuationSharedIteratorMatchesPointLookups(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _ := openTestStore(t, t.TempDir(), Options{fs: vfs.NewMem(), ExactBlocks: true, BloomFilterBits: 10, MemTableSize: 1 << 20,
		L0TargetFileSize: 2 << 10, TargetFileSize: 4 << 10, LBaseMaxBytes: 16 << 10, L0CompactionThreshold: 8})
	defer closeTestStore(t, s)
	m := Metadata{ID: 41, Generation: 3}
	neighbours := []Metadata{{ID: 40, Generation: 9}, {ID: 41, Generation: 2}, {ID: 41, Generation: 4}, {ID: 42, Generation: 1}}
	file := func(kind byte, n int) string {
		return fmt.Sprintf("f\x00\x00\x00\x00\x00\x00\x00\x01%c\x00%c", kind, '0'+n)
	}
	var keys []string
	for n := 0; n < 4; n++ {
		keys = append(keys, file('H', n), file('b', n), file('c', n), file('p', n))
	}
	keys = append(keys, file('h', 0), "n/main.wsp", "n/main.wsp.ooo", "sequence", "", "\xff\xff")
	for _, bucket := range []uint32{0, 7} {
		keys = append(keys, string(ContinuationBucketKey(bucket, []byte(file('c', 1)))), string(ContinuationBucketKey(bucket, []byte(file('c', 2)))))
	}
	prefixes := []string{"", "f", file('H', 0)[:10], file('c', 0)[:10], "n/", "absent/", string(ContinuationBucketKey(7, nil)), string(ContinuationBucketKey(7, []byte("f")))}
	rng := rand.New(rand.NewSource(202610252240))
	value := func(round, step int) []byte {
		return []byte(fmt.Sprintf("r%d-s%d-%s", round, step, string(rune('a'+rng.Intn(26)))))
	}
	type read struct {
		value []byte
		err   string
	}
	result := func(v []byte, err error) read {
		if err != nil {
			return read{err: err.Error()}
		}
		return read{value: v}
	}
	for round := 0; round < 24; round++ {
		seed := s.db.NewIndexedBatch()
		for _, n := range neighbours {
			io := newContinuationIO(ctx, seed, seed, n)
			for i := 0; i < 8; i++ {
				if err := io.Set([]byte(keys[rng.Intn(len(keys))]), value(round, -i)); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := seed.Commit(pebble.Sync); err != nil {
			t.Fatal(err)
		}
		_ = seed.Close()
		batches := [2]*pebble.Batch{s.db.NewIndexedBatch(), s.db.NewIndexedBatch()}
		ios := [2]*continuationIO{newContinuationIO(ctx, batches[0], batches[0], m).withIterator(), newContinuationIO(ctx, batches[1], batches[1], m)}
		both := func(f func(io *continuationIO) error) {
			t.Helper()
			for _, io := range ios {
				if err := f(io); err != nil {
					t.Fatal(err)
				}
			}
		}
		for step := 0; step < 160; step++ {
			key, prefix := []byte(keys[rng.Intn(len(keys))]), []byte(prefixes[rng.Intn(len(prefixes))])
			switch op := rng.Intn(20); {
			case op < 5:
				v := value(round, step)
				both(func(io *continuationIO) error { return io.Set(key, v) })
			case op < 7:
				both(func(io *continuationIO) error { return io.Delete(key) })
			case op < 8:
				both(func(io *continuationIO) error { return io.DeletePrefix(prefix) })
			case op < 9 && rng.Intn(4) == 0:
				// The range tombstone DeletePrefix falls back to past its limit.
				both(func(io *continuationIO) error {
					start := io.key(prefix)
					return io.batch.DeleteRange(start, prefixEnd(start), nil)
				})
			case op < 14:
				// The shared io probes through its iterator or gets with a
				// fresh lookup; the other io's reads are all fresh lookups.
				var got [2]read
				got[0] = result(ios[0].Get(key))
				if op%2 == 0 {
					got[0] = result(ios[0].Probe(key))
				}
				got[1] = result(ios[1].Get(key))
				if !reflect.DeepEqual(got[0], got[1]) {
					t.Fatalf("round %d step %d read %q: shared %+v, lookups %+v", round, step, key, got[0], got[1])
				}
			default:
				nested, write, probe := []byte(keys[rng.Intn(len(keys))]), rng.Intn(3) == 0, rng.Intn(2) == 0
				v := value(round, step)
				var got [2][]read
				for i, io := range ios {
					err := io.Visit(prefix, func(k, value []byte) error {
						got[i] = append(got[i], read{value: append(append([]byte(nil), k...), value...)})
						if write {
							if err := io.Set(nested, v); err != nil {
								return err
							}
						}
						if probe {
							got[i] = append(got[i], result(io.Probe(nested)))
						} else {
							got[i] = append(got[i], result(io.Get(nested)))
						}
						return nil
					})
					if err != nil {
						got[i] = append(got[i], read{err: err.Error()})
					}
				}
				if !reflect.DeepEqual(got[0], got[1]) {
					t.Fatalf("round %d step %d visit %q: shared %+v, lookups %+v", round, step, prefix, got[0], got[1])
				}
			}
		}
		if ios[0].iter == nil {
			t.Fatal("shared io read without its iterator")
		}
		if err := errors.Join(ios[0].Close(), batches[0].Commit(pebble.Sync), batches[0].Close(), batches[1].Close()); err != nil {
			t.Fatal(err)
		}
		if round%3 != 2 {
			if err := s.db.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		if round%9 == 8 {
			prefix := continuationPrefix(m.ID, m.Generation)
			if err := s.db.Compact(prefix, prefixEnd(prefix), false); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// The store comparer differs from Pebble's default only by Split, under the
// same name. Tables written under either must open under the other, and point
// reads by SeekPrefixGE must find exactly the keys Get finds while consulting
// the bloom filters, so neither an upgrade nor a rollback misreads a table.
func TestStoreComparerSharesTablesWithDefaultComparer(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	open := func(dir string, comparer *pebble.Comparer) *pebble.DB {
		t.Helper()
		policy := bloom.FilterPolicy(10)
		levels := make([]pebble.LevelOptions, numLevels)
		for i := range levels {
			levels[i] = pebble.LevelOptions{FilterPolicy: policy, FilterType: pebble.TableFilter}
		}
		db, err := pebble.Open(dir, &pebble.Options{FS: fs, DisableAutomaticCompactions: true, Comparer: comparer, Levels: levels, Filters: map[string]pebble.FilterPolicy{policy.Name(): policy}})
		if err != nil {
			t.Fatal(err)
		}
		return db
	}
	key := func(i int) []byte { return []byte(fmt.Sprintf("u%06d", i)) }
	for _, writer := range []*pebble.Comparer{pebble.DefaultComparer, storeComparer} {
		dir := t.TempDir()
		db := open(dir, writer)
		// Even keys only, in two L0 tables: L6 tables are read without filters.
		for half := 0; half < 2; half++ {
			for i := half; i < 2000; i += 2 {
				if err := db.Set(key(2*i), key(2*i), pebble.NoSync); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db = open(dir, storeComparer)
		iter, err := db.NewIter(nil)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 4000; i++ {
			want := i%2 == 0
			if found := iter.SeekPrefixGE(key(i)); found != want || found && !bytes.Equal(iter.Value(), key(i)) {
				t.Fatalf("writer %v key %d: SeekPrefixGE found=%v", writer.Split != nil, i, found)
			}
			value, closer, err := db.Get(key(i))
			if (err == nil) != want {
				t.Fatalf("writer %v key %d: Get %v", writer.Split != nil, i, err)
			}
			if err == nil {
				if !bytes.Equal(value, key(i)) {
					t.Fatalf("writer %v key %d: Get %q", writer.Split != nil, i, value)
				}
				_ = closer.Close()
			}
		}
		if filtered := db.Metrics().Filter.Hits; filtered == 0 {
			t.Fatalf("writer %v: no read skipped a table by its filter", writer.Split != nil)
		}
		if err := errors.Join(iter.Close(), db.Close()); err != nil {
			t.Fatal(err)
		}
		// A binary without Split still opens and reads the tables.
		db = open(dir, pebble.DefaultComparer)
		if value, closer, err := db.Get(key(2)); err != nil || !bytes.Equal(value, key(2)) {
			t.Fatalf("writer %v: default comparer read %q %v", writer.Split != nil, value, err)
		} else {
			_ = closer.Close()
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

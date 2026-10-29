package pebble

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

// The ordinary iterator keeps the original range-fragmentation path and is
// the independent oracle for point lookups, including retained snapshots.
func checkPointGetAgainstIterator(t *testing.T, snapshot *Snapshot, keys []string) {
	t.Helper()
	for _, key := range keys {
		iter, err := snapshot.NewIter(nil)
		if err != nil {
			t.Fatal(err)
		}
		found := iter.SeekGE([]byte(key)) && bytes.Equal(iter.Key(), []byte(key))
		value, closer, err := snapshot.Get([]byte(key))
		if found {
			if err != nil || !bytes.Equal(value, iter.Value()) {
				t.Fatalf("Get(%q) = %q, %v; iterator = %q", key, value, err, iter.Value())
			}
		} else if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get(%q) = %q, %v; iterator is absent", key, value, err)
		}
		if closer != nil {
			if err := closer.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if err := iter.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGetBeforeMemTableRangeDeletionMatchesIterator(t *testing.T) {
	db, err := Open("db", &Options{FS: vfs.NewMem(), MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	keys := []string{"a/a", "m/metric", "u/a", "u/m", "u/n", "u/z", "z/z", "zz/", "zzz/", "missing"}
	for _, key := range keys[:len(keys)-1] {
		if err := db.Set([]byte(key), []byte(key), NoSync); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	old := db.NewSnapshot()
	defer old.Close()
	for _, span := range [][2]string{{"u/a", "u/n"}, {"u/m", "u/z"}, {"m/metric", "u/a"}, {"a/", "zz/"}, {"z/", "z/a"}} {
		if err := db.DeleteRange([]byte(span[0]), []byte(span[1]), NoSync); err != nil {
			t.Fatal(err)
		}
		current := db.NewSnapshot()
		checkPointGetAgainstIterator(t, current, keys)
		current.Close()
		checkPointGetAgainstIterator(t, old, keys)
		if err := db.Set([]byte(span[0]), []byte("replacement"), NoSync); err != nil {
			t.Fatal(err)
		}
		current = db.NewSnapshot()
		checkPointGetAgainstIterator(t, current, keys)
		current.Close()
	}
}

func TestGetRangeBoundWithConcurrentPublication(t *testing.T) {
	db, err := Open("db", &Options{FS: vfs.NewMem(), MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	keys := []string{"a/a", "m/metric", "u/a", "u/m", "u/n", "z/z"}
	var wg sync.WaitGroup
	errorsFound := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			batch := db.NewBatch()
			err := batch.Set([]byte("m/metric"), []byte(fmt.Sprint(i)), nil)
			if err == nil {
				if i%2 == 0 {
					err = batch.DeleteRange([]byte("u/a"), []byte("u/z"), nil)
				} else {
					err = batch.DeleteRange([]byte("a/"), []byte("z/"), nil)
				}
			}
			if err == nil {
				err = batch.Commit(NoSync)
			}
			batch.Close()
			if err != nil {
				errorsFound <- err
				return
			}
			if i%20 == 19 {
				if err := db.Flush(); err != nil {
					errorsFound <- err
					return
				}
			}
		}
	}()
	defer wg.Wait()
	for i := 0; i < 200; i++ {
		snapshot := db.NewSnapshot()
		checkPointGetAgainstIterator(t, snapshot, keys)
		snapshot.Close()
	}
	wg.Wait()
	select {
	case err := <-errorsFound:
		t.Fatal(err)
	default:
	}
}

func TestGetRangeUpperBoundConcurrentExtensions(t *testing.T) {
	db, err := Open("db", &Options{FS: vfs.NewMem(), MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var keys []string
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("u/%03d", i)
		keys = append(keys, key)
		if err := db.Set([]byte(key), []byte(key), NoSync); err != nil {
			t.Fatal(err)
		}
	}
	old := db.NewSnapshot()
	defer old.Close()
	var wg sync.WaitGroup
	errorsFound := make(chan error, 8)
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 64; i++ {
				start := []byte(fmt.Sprintf("u/%03d", (i*17+worker)%100))
				end := []byte(fmt.Sprintf("u/%03d", (i*17+worker)%100+1+worker))
				err := db.DeleteRange(start, end, NoSync)
				// Mutating the input also checks the bound owns its bytes.
				for i := range end {
					end[i] = 0
				}
				if err != nil {
					errorsFound <- err
					return
				}
			}
		}(worker)
	}
	for i := 0; i < 40; i++ {
		snapshot := db.NewSnapshot()
		checkPointGetAgainstIterator(t, snapshot, keys)
		snapshot.Close()
		checkPointGetAgainstIterator(t, old, keys)
	}
	wg.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	snapshot := db.NewSnapshot()
	defer snapshot.Close()
	checkPointGetAgainstIterator(t, snapshot, keys)
}

func BenchmarkNewGenerationGetWithSparseRangeDeletes(b *testing.B) {
	benchmarkPointGetWithSparseRangeDeletes(b, "u/zzzz-new-generation")
}

func BenchmarkMetadataGetWithSparseRangeDeletes(b *testing.B) {
	benchmarkPointGetWithSparseRangeDeletes(b, "m/metric")
}

func benchmarkPointGetWithSparseRangeDeletes(b *testing.B, name string) {
	db, err := Open("db", &Options{FS: vfs.NewMem(), MemTableSize: 64 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	key := []byte(name)
	if err := db.Set(key, []byte("metadata"), NoSync); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 2048; i++ {
		start := []byte(fmt.Sprintf("u/%08d/a", i))
		end := []byte(fmt.Sprintf("u/%08d/z", i))
		if err := db.DeleteRange(start, end, NoSync); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%128 == 0 {
			if err := db.DeleteRange([]byte("u/recent/a"), []byte("u/recent/z"), NoSync); err != nil {
				b.Fatal(err)
			}
		}
		_, closer, err := db.Get(key)
		if err != nil {
			b.Fatal(err)
		}
		if err := closer.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

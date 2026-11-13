package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
)

func TestTransferWALChunksPreserveKeys(t *testing.T) {
	for _, limit := range []int{128, 1 << 20} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			fs := vfs.NewMem()
			db, err := pebble.Open("/store", &pebble.Options{FS: fs})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			s := &Store{db: db}
			batch := db.NewBatch()
			defer batch.Close()
			want := make(map[string][]byte)
			for i := 0; i < 40; i++ {
				key := fmt.Sprintf("unpublished-%03d", i)
				value := bytes.Repeat([]byte{byte(i)}, 17)
				if i == 20 {
					value = bytes.Repeat([]byte{byte(i)}, 256)
				} // One indivisible oversized value.
				want[key] = value
				if err := batch.Set([]byte(key), value, nil); err != nil {
					t.Fatal(err)
				}
			}
			commits, size, err := s.commitTransferBatch(context.Background(), batch, limit)
			if err != nil {
				t.Fatal(err)
			}
			if limit == 128 && commits < 10 {
				t.Fatalf("unbounded commits: %d", commits)
			}
			if limit == 1<<20 && commits != 1 {
				t.Fatalf("small transfer: %d commits", commits)
			}
			if size < uint64(len(batch.Repr())) {
				t.Fatalf("lost encoded bytes: %d", size)
			}
			// Read the durable WAL, independently of the helper's counters:
			// every multi-key record must respect the byte bound. Oversized
			// single values remain intact, and every staged key remains readable.
			if err := db.LogData(nil, pebble.Sync); err != nil {
				t.Fatal(err)
			}
			files, err := fs.List("/store")
			if err != nil {
				t.Fatal(err)
			}
			var walCommits uint64
			for _, name := range files {
				if !strings.HasSuffix(name, ".log") {
					continue
				}
				num, err := strconv.ParseUint(strings.TrimSuffix(name, ".log"), 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				f, err := fs.Open("/store/" + name)
				if err != nil {
					t.Fatal(err)
				}
				reader := record.NewReader(f, pebble.FileNum(num))
				for {
					r, err := reader.Next()
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					data, err := io.ReadAll(r)
					if err != nil {
						t.Fatal(err)
					}
					b := &pebble.Batch{}
					if err := b.SetRepr(data); err != nil {
						t.Fatal(err)
					}
					if b.Count() == 0 {
						continue
					} // Sync barrier.
					walCommits++
					if len(data) > limit && b.Count() != 1 {
						t.Fatalf("WAL record exceeds %d bytes: %d bytes, %d keys", limit, len(data), b.Count())
					}
				}
				f.Close()
			}
			if walCommits != commits {
				t.Fatalf("WAL commits=%d, reported=%d", walCommits, commits)
			}

			it, err := db.NewIter(nil)
			if err != nil {
				t.Fatal(err)
			}
			defer it.Close()
			count := 0
			for ok := it.First(); ok; ok = it.Next() {
				value, exists := want[string(it.Key())]
				if !exists || !bytes.Equal(value, it.Value()) {
					t.Fatalf("changed staged key %q", it.Key())
				}
				count++
			}
			if err := it.Error(); err != nil {
				t.Fatal(err)
			}
			if count != len(want) {
				t.Fatalf("got %d keys, want %d", count, len(want))
			}
		})
	}
}

func TestTransferWALChunksCancelledBeforeCommit(t *testing.T) {
	db, err := pebble.Open("/store", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{db: db}
	for _, limit := range []int{32, 1 << 20} {
		batch := db.NewBatch()
		if err := batch.Set([]byte("unpublished"), bytes.Repeat([]byte("v"), 128), nil); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		n, size, err := s.commitTransferBatch(ctx, batch, limit)
		batch.Close()
		if !errors.Is(err, context.Canceled) || n != 0 || size != 0 {
			t.Fatalf("cancel: %d %d %v", n, size, err)
		}
	}
	it, err := db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	if it.First() {
		t.Fatal("cancelled transfer wrote keys")
	}
}

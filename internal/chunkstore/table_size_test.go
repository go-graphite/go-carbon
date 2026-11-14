package chunkstore

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestTableSizeDefaultsAndOverrides(t *testing.T) {
	for _, tc := range []struct {
		name       string
		options    Options
		l0, l1, l6 int64
	}{
		{"small", Options{ExactBlocks: true, MemTableSize: 64 << 20}, 2 << 20, 4 << 20, 128 << 20},
		{"large", Options{ExactBlocks: true, MemTableSize: 2 << 30}, 16 << 20, 16 << 20, 128 << 20},
		{"bounded", Options{ExactBlocks: true, MemTableSize: 3 << 30}, 16 << 20, 16 << 20, 128 << 20},
		{"legacy", Options{MemTableSize: 2 << 30}, 2 << 20, 4 << 20, 128 << 20},
		{"explicit", Options{ExactBlocks: true, MemTableSize: 2 << 30, TargetFileSize: 8 << 20}, 8 << 20, 8 << 20, 8 << 20},
		{"l0 override", Options{ExactBlocks: true, MemTableSize: 2 << 30, L0TargetFileSize: 2 << 20}, 2 << 20, 16 << 20, 128 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.options.fs = vfs.NewMem()
			s, err := Open("store", tc.options)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			o := s.pebbleOptions
			if o.Level(0).TargetFileSize != tc.l0 || o.Level(1).TargetFileSize != tc.l1 || o.Level(6).TargetFileSize != tc.l6 {
				t.Fatalf("table sizes %d/%d/%d, want %d/%d/%d", o.Level(0).TargetFileSize, o.Level(1).TargetFileSize, o.Level(6).TargetFileSize, tc.l0, tc.l1, tc.l6)
			}
		})
	}
}

// Exercise the actual flush splitter with the same encoded points, then
// compare every stored byte before and after compaction. Random point bits
// keep compression from hiding the file-count difference.
func TestLargeMemtableTablesPreserveEncodedPoints(t *testing.T) {
	var stores []*Store
	for _, target := range []int64{2 << 20, 0} {
		s, err := Open("store", Options{fs: vfs.NewMem(), ExactBlocks: true, MemTableSize: 1 << 30, L0TargetFileSize: target, LBaseMaxBytes: 1 << 30, L0CompactionThreshold: 100, L0StopWritesThreshold: 200})
		if err != nil {
			t.Fatal(err)
		}
		stores = append(stores, s)
		defer s.Close()
	}
	rng := rand.New(rand.NewSource(7))
	const count = 8192
	expected := make([][]byte, count)
	key := make([]byte, 9)
	key[0] = 'x'
	for i := range count {
		var c chunk
		for j := range chunkSlots {
			c.set(j, Point{Timestamp: 1700000000 + int64(j)*60, Value: math.Float64frombits(rng.Uint64())})
		}
		expected[i] = encodeExactChunk(&c)
		binary.BigEndian.PutUint64(key[1:], uint64(i))
		for _, s := range stores {
			if err := s.db.Set(key, expected[i], pebble.NoSync); err != nil {
				t.Fatal(err)
			}
		}
	}
	files := make([]int, 2)
	for i, s := range stores {
		if err := s.db.Flush(); err != nil {
			t.Fatal(err)
		}
		tables, err := s.db.SSTables()
		if err != nil {
			t.Fatal(err)
		}
		files[i] = len(tables[0])
	}
	if files[1]*2 > files[0] {
		t.Fatalf("larger tables did not reduce flush files: %v", files)
	}
	t.Logf("flush files: default 2MiB=%d, scaled=%d", files[0], files[1])
	check := func() {
		for i, want := range expected {
			binary.BigEndian.PutUint64(key[1:], uint64(i))
			for _, s := range stores {
				got, closer, err := s.db.Get(key)
				if err != nil {
					t.Fatal(err)
				}
				same := bytes.Equal(want, got)
				closer.Close()
				if !same {
					t.Fatalf("encoded point bytes changed at %d", i)
				}
			}
		}
	}
	check()
	for _, s := range stores {
		if err := s.db.Compact([]byte("x"), []byte("y"), false); err != nil {
			t.Fatal(err)
		}
	}
	check()
}

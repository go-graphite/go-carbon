package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"testing"
)

// Compare the old single-ingest operation and bounded ingestion on precisely
// the same immutable keys, including raw IEEE-754 payloads and table splits.
func TestBoundedTransferIngestDifferential(t *testing.T) {
	ctx := context.Background()
	var keys []RelayoutKV
	bits := []uint64{0, 1 << 63, 0x7ff8000000000042, math.Float64bits(math.Inf(1)), math.Float64bits(math.Inf(-1)), 1}
	for bucket := range 162 {
		count := 1
		if bucket == 0 {
			count = 140
		}
		for i := range count {
			key := make([]byte, 13)
			key[0] = continuationBucketKeyPrefix
			binary.BigEndian.PutUint32(key[1:5], uint32(bucket))
			binary.BigEndian.PutUint64(key[5:], uint64(i))
			value := make([]byte, 64<<10)
			for j := 0; j < len(value); j += 8 {
				binary.BigEndian.PutUint64(value[j:], bits[(i+j/8+bucket)%len(bits)])
			}
			keys = append(keys, RelayoutKV{key, value})
		}
	}
	open := func() *Store {
		s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
		t.Cleanup(func() { closeTestStore(t, s) })
		return s
	}
	old, bounded := open(), open()
	// Reference retains the old namespace grouping with an oversized first table.
	var oldPaths []string
	for start := 0; start < len(keys); {
		end := start + 1
		for end < len(keys) && bytes.Equal(keys[start].Key[:5], keys[end].Key[:5]) {
			end++
		}
		path := old.fs.PathJoin(old.dir, fmt.Sprintf("oracle-%d.sst", start))
		if err := old.writeTransferTable(ctx, path, keys[start:end]); err != nil {
			t.Fatal(err)
		}
		oldPaths = append(oldPaths, path)
		start = end
	}
	if _, err := old.db.IngestWithStats(oldPaths); err != nil {
		t.Fatal(err)
	}
	stage := &transferStage{keys: keys, capture: func([]byte, []byte) error { return nil }, verify: func(context.Context) error { return nil }}
	paths, err := bounded.writeTransferTables(ctx, 1, stage)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 163 {
		t.Fatalf("wanted oversized bucket split: %d tables", len(paths))
	}
	if err := bounded.ingestTransferTables(ctx, paths); err != nil {
		t.Fatal(err)
	}
	a, err := old.db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := bounded.db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	av, bv := a.First(), b.First()
	for av && bv {
		if !bytes.Equal(a.Key(), b.Key()) || !bytes.Equal(a.Value(), b.Value()) {
			t.Fatalf("stored bytes differ at %x", a.Key())
		}
		av, bv = a.Next(), b.Next()
	}
	if av != bv || a.Error() != nil || b.Error() != nil {
		t.Fatal("iterator results differ", a.Error(), b.Error())
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := bounded.ingestTransferTables(canceled, []string{"must-not-open"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

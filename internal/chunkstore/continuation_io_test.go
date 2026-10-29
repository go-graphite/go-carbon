package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestFreshContinuationIOMatchesIndexedBatchAndReopen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dirs := []string{t.TempDir(), t.TempDir()}
	var stores []*Store
	var batches []*pebble.Batch
	var writers []ContinuationIO
	fs := vfs.NewMem()
	metric := Metadata{ID: 1, Generation: 2}
	for i, dir := range dirs {
		s, _ := openTestStore(t, dir, Options{ExactBlocks: true, fs: fs})
		stores = append(stores, s)
		old := newContinuationIO(ctx, s.db, s.db.NewIndexedBatch(), Metadata{ID: 1, Generation: 1})
		if err := old.Set([]byte("main/old"), []byte("retain-other-generation")); err != nil {
			t.Fatal(err)
		}
		if err := old.batch.Commit(pebble.Sync); err != nil {
			t.Fatal(err)
		}
		old.batch.Close()
		batch := s.db.NewIndexedBatch()
		batches = append(batches, batch)
		if i == 0 {
			writers = append(writers, newContinuationIO(ctx, batch, batch, metric))
		} else {
			writers = append(writers, newFreshContinuationIO(ctx, batch, metric))
		}
	}
	defer func() {
		for i := range stores {
			if batches[i] != nil {
				_ = batches[i].Close()
			}
			closeTestStore(t, stores[i])
		}
	}()
	read := func(io ContinuationIO, prefix string) map[string]string {
		t.Helper()
		values := make(map[string]string)
		if err := io.Visit([]byte(prefix), func(key, value []byte) error {
			values[string(key)] = string(value)
			if len(key) > 0 {
				key[0] ^= 255
			}
			if len(value) > 0 {
				value[0] ^= 255
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return values
	}
	keys := []string{"", "header", "main/0", "main/1", "main/10", "ooo/0", "ooo/1", "\x00\xff"}
	prefixes := []string{"", "main/", "ooo/", "absent/", "\x00"}
	rng := rand.New(rand.NewSource(120220261023))
	for step := 0; step < 400; step++ {
		key := keys[rng.Intn(len(keys))]
		operation := rng.Intn(4)
		prefix := prefixes[rng.Intn(len(prefixes))]
		for _, writer := range writers {
			var err error
			switch operation {
			case 0, 1:
				input := []byte(fmt.Sprintf("value-%d", step))
				err = writer.Set([]byte(key), input)
				input[0] ^= 255
			case 2:
				err = writer.Delete([]byte(key))
			case 3:
				err = writer.DeletePrefix([]byte(prefix))
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, key := range keys {
			left, le := writers[0].Get([]byte(key))
			right, re := writers[1].Get([]byte(key))
			if !bytes.Equal(left, right) || errors.Is(le, ErrNotFound) != errors.Is(re, ErrNotFound) || (le == nil) != (re == nil) {
				t.Fatalf("step %d key %q: %q/%v != %q/%v", step, key, left, le, right, re)
			}
			if len(right) > 0 {
				right[0] ^= 255
				again, err := writers[1].Get([]byte(key))
				if err != nil || !bytes.Equal(left, again) {
					t.Fatal("fresh reads alias their batch")
				}
			}
		}
		for _, prefix := range prefixes {
			if left, right := read(writers[0], prefix), read(writers[1], prefix); !reflect.DeepEqual(left, right) {
				t.Fatalf("step %d prefix %q: %#v != %#v", step, prefix, left, right)
			}
		}
	}
	for _, writer := range writers {
		if err := writer.Set([]byte("main/final"), []byte("committed")); err != nil {
			t.Fatal(err)
		}
	}
	want := read(writers[0], "")
	for i, batch := range batches {
		if err := batch.Commit(pebble.Sync); err != nil {
			t.Fatal(err)
		}
		batch.Close()
		batches[i] = nil
		closeTestStore(t, stores[i])
		stores[i], _ = openTestStore(t, dirs[i], Options{ExactBlocks: true, fs: fs})
		if got := read(newContinuationIO(ctx, stores[i].db, nil, metric), ""); !reflect.DeepEqual(got, want) {
			t.Fatalf("reopen changed staged state: %#v != %#v", got, want)
		}
		old, err := newContinuationIO(ctx, stores[i].db, nil, Metadata{ID: 1, Generation: 1}).Get([]byte("main/old"))
		if err != nil || string(old) != "retain-other-generation" {
			t.Fatal("fresh generation modified its predecessor")
		}
	}
}

func TestContinuationIOAtomicFilesNamespaceAndRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	s, _ := openTestStore(t, dir, Options{ExactBlocks: true})
	m := Metadata{ID: 1, Generation: 1}
	other := Metadata{ID: 2, Generation: 1}
	seed := s.db.NewIndexedBatch()
	io := newContinuationIO(ctx, seed, seed, m)
	for _, pair := range [][2]string{{"header", "old-header"}, {"main/0", "old-main"}, {"ooo/0", "old-ooo"}} {
		if err := io.Set([]byte(pair[0]), []byte(pair[1])); err != nil {
			t.Fatal(err)
		}
	}
	if err := newContinuationIO(ctx, seed, seed, other).Set([]byte("main/0"), []byte("other-metric")); err != nil {
		t.Fatal(err)
	}
	if err := seed.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	_ = seed.Close()
	snap := s.db.NewSnapshot()
	defer func() {
		if snap != nil {
			_ = snap.Close()
		}
	}()
	old := newContinuationIO(ctx, snap, nil, m)
	batch := s.db.NewIndexedBatch()
	current := newContinuationIO(ctx, batch, batch, m)
	for _, pair := range [][2]string{{"header", "new-header"}, {"main/0", "new-main"}} {
		if err := current.Set([]byte(pair[0]), []byte(pair[1])); err != nil {
			t.Fatal(err)
		}
	}
	if err := current.DeletePrefix([]byte("ooo/")); err != nil {
		t.Fatal(err)
	}
	assertValue := func(io ContinuationIO, key, want string) {
		t.Helper()
		got, err := io.Get([]byte(key))
		if err != nil || string(got) != want {
			t.Fatalf("%s: %q != %q: %v", key, got, want, err)
		}
		if len(got) > 0 {
			got[0] ^= 255
		}
		again, err := io.Get([]byte(key))
		if err != nil || string(again) != want {
			t.Fatal("returned bytes alias database storage")
		}
	}
	assertValue(current, "header", "new-header")
	assertValue(old, "header", "old-header")
	assertValue(newContinuationIO(ctx, s.db, nil, m), "main/0", "old-main")
	if _, err := current.Get([]byte("ooo/0")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("indexed batch did not observe sidecar deletion: %v", err)
	}
	if err := current.Set([]byte("ooo/1"), []byte("new-ooo")); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	_ = batch.Close()
	assertValue(old, "ooo/0", "old-ooo")
	assertValue(newContinuationIO(ctx, s.db, nil, m), "ooo/1", "new-ooo")
	assertValue(newContinuationIO(ctx, s.db, nil, other), "main/0", "other-metric")
	if _, err := newContinuationIO(ctx, s.db, nil, Metadata{ID: 1, Generation: 2}).Get([]byte("main/0")); !errors.Is(err, ErrNotFound) {
		t.Fatal("different metric generation leaked")
	}
	if err := old.Set([]byte("header"), nil); err == nil {
		t.Fatal("snapshot accepted a write")
	}
	var keys []string
	if err := newContinuationIO(ctx, s.db, nil, m).Visit(nil, func(key, value []byte) error {
		keys = append(keys, string(key))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(keys, []string{"header", "main/0", "ooo/1"}) {
		t.Fatalf("namespace iteration escaped or retained deleted keys: %v", keys)
	}
	_ = snap.Close()
	snap = nil
	closeTestStore(t, s)
	s, _ = openTestStore(t, dir, Options{ExactBlocks: true})
	defer closeTestStore(t, s)
	assertValue(newContinuationIO(ctx, s.db, nil, m), "header", "new-header")
	assertValue(newContinuationIO(ctx, s.db, nil, m), "main/0", "new-main")
	assertValue(newContinuationIO(ctx, s.db, nil, m), "ooo/1", "new-ooo")
	aborted := s.db.NewIndexedBatch()
	if err := newContinuationIO(ctx, aborted, aborted, m).Set([]byte("header"), []byte("uncommitted")); err != nil {
		t.Fatal(err)
	}
	_ = aborted.Close()
	assertValue(newContinuationIO(ctx, s.db, nil, m), "header", "new-header")
}

func TestContinuationPointBlocksRetainAllBits(t *testing.T) {
	var input PointBlock
	values := []float64{math.Copysign(0, -1), math.Float64frombits(0x7ff800000000042a), math.Inf(1), math.Inf(-1), .1, 1e-300, 1e100, 9007199254740991}
	for i, v := range values {
		slot := i * 17
		input.Present[slot/64] |= uint64(1) << (slot % 64)
		input.Points[slot] = Point{Timestamp: int64(1792692000 + i), Value: v}
	}
	encoded := EncodePointBlock(&input)
	var output PointBlock
	if err := DecodePointBlock(encoded, &output); err != nil {
		t.Fatal(err)
	}
	if input.Present != output.Present {
		t.Fatal("physical positions changed")
	}
	for i := range input.Points {
		if input.Points[i].Timestamp != output.Points[i].Timestamp || math.Float64bits(input.Points[i].Value) != math.Float64bits(output.Points[i].Value) {
			t.Fatalf("point %d changed", i)
		}
	}
	encoded[len(encoded)-1] ^= 1
	if err := DecodePointBlock(encoded, &output); err == nil {
		t.Fatal("corrupted continuation point block accepted")
	}
}

func batchRangeDeletes(t *testing.T, b *pebble.Batch) int {
	t.Helper()
	n := 0
	for r := b.Reader(); ; {
		kind, _, _, ok, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return n
		}
		if kind == pebble.InternalKeyKindRangeDelete {
			n++
		}
	}
}

func TestContinuationDeletePrefixAvoidsRangeTombstones(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true})
	defer closeTestStore(t, s)
	m := Metadata{ID: 7, Generation: 3}
	seed := s.db.NewIndexedBatch()
	io := newContinuationIO(ctx, seed, seed, m)
	for _, key := range []string{"c/0/1/a", "c/0/1/b", "c/0/2/a", "header"} {
		if err := io.Set([]byte(key), []byte("v-"+key)); err != nil {
			t.Fatal(err)
		}
	}
	if err := newContinuationIO(ctx, seed, seed, Metadata{ID: 7, Generation: 4}).Set([]byte("c/0/1/a"), []byte("next-generation")); err != nil {
		t.Fatal(err)
	}
	if err := seed.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	_ = seed.Close()

	batch := s.db.NewIndexedBatch()
	defer batch.Close()
	current := newContinuationIO(ctx, batch, batch, m)
	// A key written earlier in the same batch must be deleted as well.
	if err := current.Set([]byte("c/0/1/c"), []byte("batch-only")); err != nil {
		t.Fatal(err)
	}
	if err := current.DeletePrefix([]byte("c/0/1/")); err != nil {
		t.Fatal(err)
	}
	if !current.changed {
		t.Fatal("prefix deletion did not mark the batch changed")
	}
	if n := batchRangeDeletes(t, batch); n != 0 {
		t.Fatalf("small prefix wrote %d range tombstones", n)
	}
	if err := current.Set([]byte("c/0/1/a"), []byte("rewritten")); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	var keys []string
	if err := newContinuationIO(ctx, s.db, nil, m).Visit(nil, func(key, value []byte) error {
		keys = append(keys, string(key)+"="+string(value))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"c/0/1/a=rewritten", "c/0/2/a=v-c/0/2/a", "header=v-header"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("visible keys %v != %v", keys, want)
	}
	if got, err := newContinuationIO(ctx, s.db, nil, Metadata{ID: 7, Generation: 4}).Get([]byte("c/0/1/a")); err != nil || string(got) != "next-generation" {
		t.Fatalf("deletion escaped its generation: %q %v", got, err)
	}
}

func TestContinuationDeletePrefixFallsBackToRangeTombstone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, fs: vfs.NewMem()})
	defer closeTestStore(t, s)
	m := Metadata{ID: 9, Generation: 1}
	seed := s.db.NewBatchWithSize((continuationPointDeleteLimit + 1) * 48)
	io := newContinuationIO(ctx, seed, seed, m)
	key := make([]byte, len("big/"), 32)
	copy(key, "big/")
	var decimal [20]byte
	value := []byte("x")
	for i := 0; i <= continuationPointDeleteLimit; i++ {
		digits := strconv.AppendInt(decimal[:0], int64(i), 10)
		key = append(key[:len("big/")], "00000"[:max(0, 5-len(digits))]...)
		key = append(key, digits...)
		if err := io.Set(key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := seed.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	_ = seed.Close()
	for _, separate := range []bool{false, true} {
		batch := s.db.NewIndexedBatch()
		var current *continuationIO
		if separate {
			// A reader that cannot see this batch's writes keeps the range tombstone.
			current = newContinuationIO(ctx, s.db, batch, m)
			if err := current.Set([]byte("small/batch-only"), []byte("x")); err != nil {
				t.Fatal(err)
			}
			if err := current.DeletePrefix([]byte("small/")); err != nil {
				t.Fatal(err)
			}
		} else if current = newContinuationIO(ctx, batch, batch, m); current.DeletePrefix([]byte("big/")) != nil {
			t.Fatal("delete failed")
		}
		if n := batchRangeDeletes(t, batch); n != 1 {
			t.Fatalf("separate=%v: %d range tombstones, want 1", separate, n)
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			t.Fatal(err)
		}
		_ = batch.Close()
	}
	var keys []string
	if err := newContinuationIO(ctx, s.db, nil, m).Visit(nil, func(key, _ []byte) error {
		keys = append(keys, string(key))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("keys survived deletion: %d (first %q)", len(keys), keys[0])
	}
}

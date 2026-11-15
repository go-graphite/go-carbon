package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func continuationForwardFixture(t testing.TB) (*Store, Metadata, [][]byte) {
	t.Helper()
	s, err := Open("/forward", Options{fs: vfs.NewMem(), ExactBlocks: true, MemTableSize: 1 << 20, CacheSize: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	m := Metadata{ID: 41, Generation: 1}
	keys := make([][]byte, 512)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("f/points/%06d", i))
	}
	for round := range 4 {
		b := s.db.NewIndexedBatch()
		io := newContinuationIO(context.Background(), b, b, m)
		for i, key := range keys {
			if i%3 == 0 {
				continue
			}
			if err := io.Set(key, []byte(fmt.Sprintf("%d:%d", round, i))); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Commit(pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		if err := s.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	return s, m, keys
}

func TestContinuationBorrowedIteratorPointLookupParity(t *testing.T) {
	s, m, keys := continuationForwardFixture(t)
	view := s.db.NewReadView()
	defer view.Close()
	iter, err := view.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	fast := newContinuationIO(context.Background(), view, nil, m).withBorrowedIterator(iter)
	reference := newContinuationIO(context.Background(), view, nil, m)
	var held [][]byte
	var expected [][]byte
	check := func(key []byte) {
		t.Helper()
		got, ge := fast.Probe(key)
		want, we := reference.Get(key)
		if !bytes.Equal(got, want) || !errors.Is(ge, we) {
			t.Fatalf("key %q: %q/%v vs %q/%v", key, got, ge, want, we)
		}
		held = append(held, got)
		expected = append(expected, bytes.Clone(want))
	}
	// Consecutive keys, holes, repeated keys, reverse seeks, EOF and rewinds.
	for _, key := range keys {
		check(key)
		check(key)
	}
	check([]byte("zzzz"))
	for i := len(keys) - 1; i >= 0; i-- {
		check(keys[i])
	}
	rng := rand.New(rand.NewSource(391))
	for range 2000 {
		check(keys[rng.Intn(len(keys))])
	}
	// A Visit also moves the borrowed iterator. The following point read must
	// still agree with Get, including after a callback makes a nested read.
	if err := fast.Visit([]byte("f/points/000"), func(k, v []byte) error { check(keys[300]); return nil }); err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		check(key)
	}
	for i := range held {
		if !bytes.Equal(held[i], expected[i]) {
			t.Fatal("later seek mutated an owned value")
		}
	}
}

func BenchmarkContinuationBorrowedAdjacentKeys(b *testing.B) {
	s, m, keys := continuationForwardFixture(b)
	for _, forward := range []bool{false, true} {
		b.Run(fmt.Sprintf("forward=%t", forward), func(b *testing.B) {
			view := s.db.NewReadView()
			defer view.Close()
			iter, err := view.NewIter(nil)
			if err != nil {
				b.Fatal(err)
			}
			defer iter.Close()
			prefix := continuationPrefix(m.ID, m.Generation)
			full := make([][]byte, len(keys))
			for i, key := range keys {
				full[i] = append(bytes.Clone(prefix), key...)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				for _, key := range full {
					var found bool
					if forward {
						found = seekContinuationKey(iter, key)
					} else {
						found = iter.SeekGE(key)
					}
					if found && bytes.Equal(iter.Key(), key) {
						_ = iter.Value()
					}
				}
				if err := iter.Error(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

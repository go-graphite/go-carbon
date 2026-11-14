package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

type iteratorDigestCodec struct{ bucketCodec }

func (iteratorDigestCodec) SumDigest(_ context.Context, io ContinuationIO, _ Metadata, _, _ int, _ func() time.Time, _ ...int) ([]SumArchive, error) {
	cached := io.(*digestCacheIO)
	for _, key := range [][]byte{[]byte("plain"), []byte("absent"), ContinuationBucketKey(7, []byte("g"))} {
		got, gotErr := cached.Probe(key)
		want, wantErr := cached.Get(key)
		if !bytes.Equal(got, want) || !errors.Is(gotErr, wantErr) {
			return nil, fmt.Errorf("snapshot probe differs from Get: %q", key)
		}
	}
	if cached.iter == nil {
		return nil, errors.New("digest reopened tables instead of sharing an iterator")
	}
	return []SumArchive{{Sum: "exact", Count: 1}}, nil
}
func TestSumDigestSharesSnapshotIterator(t *testing.T) {
	s, _ := openTestStore(t, "/store", Options{fs: vfs.NewMem(), ExactBlocks: true, Continuation: iteratorDigestCodec{}})
	defer closeTestStore(t, s)
	m := createTestMetric(t, s, "snapshot.digest")
	for round := range 3 {
		got, err := s.SumDigest(context.Background(), m.Name, 0, 0)
		if err != nil || len(got) != 1 || got[0].Sum != "exact" {
			t.Fatalf("digest %d: %+v/%v", round, got, err)
		}
		if err := s.Flush(); err != nil {
			t.Fatal(err)
		}
	}
}

// The workload is many unchanged or absent classic groups in a single immutable
// metric snapshot. Repeated fresh Gets must reopen the same tables at each key.
func BenchmarkSnapshotPointProbes(b *testing.B) {
	s, err := Open("/store", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 4 << 20, MemTableSize: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	m := Metadata{ID: 41, Generation: 1}
	keys := make([][]byte, 512)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("f/points/%06d", i))
	}
	for round := range 8 {
		batch := s.db.NewIndexedBatch()
		io := newContinuationIO(ctx, batch, batch, m)
		for i, key := range keys {
			if i%3 == 0 {
				continue
			}
			if err := io.Set(key, []byte(fmt.Sprintf("%d:%d", round, i))); err != nil {
				b.Fatal(err)
			}
		}
		if err := batch.Commit(pebble.NoSync); err != nil {
			b.Fatal(err)
		}
		batch.Close()
		if err := s.Flush(); err != nil {
			b.Fatal(err)
		}
	}
	for _, shared := range []bool{false, true} {
		b.Run(fmt.Sprintf("shared=%v", shared), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				view := s.db.NewReadView()
				io := newContinuationIO(ctx, view, nil, m)
				if shared {
					io.withIterator()
				}
				for _, key := range keys {
					if _, err := io.Probe(key); err != nil && !errors.Is(err, ErrNotFound) {
						b.Fatal(err)
					}
				}
				if err := errors.Join(io.Close(), view.Close()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

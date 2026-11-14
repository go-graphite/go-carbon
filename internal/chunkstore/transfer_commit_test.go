package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestTransferCommitChunks(t *testing.T) {
	db, err := pebble.Open("chunks", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	s := &Store{db: db}
	batch := db.NewBatch()
	defer batch.Close()
	// Several chunks, including a value larger than the chunk limit.
	for i := 0; i < 100; i++ {
		value := bytes.Repeat([]byte{byte(i)}, 73)
		if i == 50 {
			value = bytes.Repeat([]byte{byte(i)}, 2048)
		}
		if err := batch.Set([]byte(fmt.Sprintf("k%03d", i)), value, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.commitTransferBatch(context.Background(), batch, 256); err != nil {
		t.Fatal(err)
	}
	r := batch.Reader()
	for {
		_, k, want, ok, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		got, closer, err := db.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		equal := bytes.Equal(got, want)
		if err := closer.Close(); err != nil {
			t.Fatal(err)
		}
		if !equal {
			t.Fatalf("changed value for %q", k)
		}
	}
	// A cancellation must not submit even the first chunk.
	canceled := db.NewBatch()
	defer canceled.Close()
	if err := canceled.Set([]byte("cancelled"), []byte("value"), nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.commitTransferBatch(ctx, canceled, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if _, closer, err := db.Get([]byte("cancelled")); !errors.Is(err, pebble.ErrNotFound) {
		if closer != nil {
			closer.Close()
		}
		t.Fatalf("cancelled batch visible: %v", err)
	}
}

// Compare the old whole-batch path with bounded staging while an unrelated
// foreground writer uses the same commit pipeline. Report its average and
// maximum commit latency, as well as staging throughput. The in-memory WAL
// deliberately excludes device fsync latency from this memtable-apply test.
func BenchmarkTransferCommitForeground(b *testing.B) {
	for _, limit := range []int{64 << 20, transferCommitBytes} {
		b.Run(fmt.Sprintf("bytes-%d", limit), func(b *testing.B) {
			db, err := pebble.Open("bench", &pebble.Options{FS: vfs.NewMem(), MemTableSize: 64 << 20, MaxConcurrentCompactions: func() int { return 2 }})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			s := &Store{db: db}
			var stop atomic.Bool
			var count, total, maximum atomic.Int64
			var wg sync.WaitGroup
			var latencies []int64
			wg.Add(1)
			go func() {
				defer wg.Done()
				for !stop.Load() {
					p := db.NewBatch()
					if err := p.Set([]byte("live"), []byte("live-value"), nil); err != nil {
						panic(err)
					}
					start := time.Now()
					if err := p.Commit(pebble.NoSync); err != nil {
						panic(err)
					}
					ns := time.Since(start).Nanoseconds()
					latencies = append(latencies, ns)
					p.Close()
					count.Add(1)
					total.Add(ns)
					for old := maximum.Load(); ns > old; old = maximum.Load() {
						if maximum.CompareAndSwap(old, ns) {
							break
						}
					}
				}
			}()
			value := bytes.Repeat([]byte("x"), 128)
			var key [9]byte
			b.SetBytes(65536 * 137)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p := db.NewBatch()
				for j := 0; j < 65536; j++ {
					key[0] = 'i'
					binary.BigEndian.PutUint64(key[1:], uint64(j))
					if err := p.Set(key[:], value, nil); err != nil {
						b.Fatal(err)
					}
				}
				var commitErr error
				if limit == 64<<20 {
					commitErr = p.Commit(pebble.NoSync)
				} else {
					commitErr = s.commitTransferBatch(context.Background(), p, limit)
				}
				if commitErr != nil {
					b.Fatal(commitErr)
				}
				p.Close()
			}
			b.StopTimer()
			stop.Store(true)
			wg.Wait()
			if n := count.Load(); n > 0 {
				slices.Sort(latencies)
				b.ReportMetric(float64(latencies[(len(latencies)-1)*99/100]), "live-p99-ns")
				b.ReportMetric(float64(total.Load())/float64(n), "live-ns/commit")
				b.ReportMetric(float64(maximum.Load()), "live-max-ns")
			}
		})
	}
}

// A prepared merge can be larger than its input staging data. Do not acquire
// all of its stripes and submit that whole group to the live commit queue.
func TestTransferLargePublicationSplitsBeforeLock(t *testing.T) {
	db, err := pebble.Open("publication", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	p := db.NewBatch()
	defer p.Close()
	if err := p.Set([]byte("data"), make([]byte, transferCommitBytes+1), nil); err != nil {
		t.Fatal(err)
	}
	s := &Store{db: db}
	committed, err := s.commitPreparedTransfer(context.Background(), &preparedTransfer{batch: p, metas: make([]Metadata, 2)})
	if committed || !errors.Is(err, errTransferSplit) {
		t.Fatalf("committed=%v err=%v", committed, err)
	}
}

func TestTransferPublicationRoundsPreservePendingParts(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 8 << 20})
	defer closeTestStore(t, s)
	parts := make([]*preparedTransfer, 3)
	for i := range parts {
		m := createTestMetric(t, s, fmt.Sprintf("round.%d", i))
		p := s.db.NewBatch()
		defer p.Close()
		if err := p.Set([]byte(fmt.Sprintf("round-data-%d", i)), bytes.Repeat([]byte{byte(i)}, 600<<10), nil); err != nil {
			t.Fatal(err)
		}
		parts[i] = &preparedTransfer{batch: p, expected: []Metadata{m}, metas: []Metadata{m}, epochs: s.transferEpochs([]Metadata{m})}
	}
	for first := range parts {
		pending := make([]int, len(parts)-first)
		for j := range pending {
			pending[j] = first + j
		}
		outcomes, err := s.commitTransferParts(context.Background(), parts, pending)
		if err != nil {
			t.Fatal(err)
		}
		if len(outcomes) != len(pending) {
			t.Fatalf("lost pending outcomes: %v", outcomes)
		}
		for j, outcome := range outcomes {
			want := partBusy
			if j == 0 {
				want = partCommitted
			}
			if outcome != want {
				t.Fatalf("round %d part %d: %v want %v", first, j, outcome, want)
			}
		}
		for i := range parts {
			_, closer, err := s.db.Get([]byte(fmt.Sprintf("round-data-%d", i)))
			if i <= first {
				if err != nil {
					t.Fatal(err)
				}
				closer.Close()
			} else if !errors.Is(err, pebble.ErrNotFound) {
				if closer != nil {
					closer.Close()
				}
				t.Fatalf("future part %d published early: %v", i, err)
			}
		}
	}
}

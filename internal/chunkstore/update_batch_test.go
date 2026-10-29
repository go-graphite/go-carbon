package chunkstore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

type batchTestCodec struct{ ContinuationCodec }

func (batchTestCodec) Create(_ context.Context, io ContinuationIO, _ Metadata, _ func() time.Time) error {
	return io.Set([]byte("point"), uint64Bytes(0))
}
func (batchTestCodec) Update(ctx context.Context, io ContinuationIO, _ Metadata, input []Point, _ int, _ func() time.Time, _ *ContinuationMaintenance) error {
	value, err := io.Get([]byte("point"))
	if err != nil {
		return err
	}
	sum := math.Float64frombits(binary.BigEndian.Uint64(value))
	for _, p := range input {
		if p.Value < 0 {
			return errors.New("injected prepare error")
		}
		sum += p.Value
	}
	return io.Set([]byte("point"), uint64Bytes(math.Float64bits(sum)))
}
func (batchTestCodec) StateKey(key []byte) bool { return string(key) == "point" }
func (batchTestCodec) StatePrefix([]byte) bool  { return false }

func TestUpdateBatchDurability(t *testing.T) {
	for _, interval := range []time.Duration{0, time.Hour} {
		t.Run(fmt.Sprint(interval), func(t *testing.T) {
			ctx := context.Background()
			strict := vfs.NewStrictMem()
			fs := &walSyncFS{FS: strict}
			db, _ := openTestStore(t, "/store", Options{fs: fs, ExactBlocks: true, Continuation: batchTestCodec{}, SyncInterval: interval, MetadataCacheSize: 64 << 20})
			var updates []MetricUpdate
			for _, name := range []string{"a", "b", "a"} {
				if name != "a" || len(updates) == 0 {
					createTestMetric(t, db, name)
				}
				updates = append(updates, MetricUpdate{Name: name, Points: []Point{{Value: 1}}})
			}
			// Preserve catalog in both modes; then crash with a grouped update pending.
			if err := db.syncWAL(); err != nil {
				t.Fatal(err)
			}
			before := fs.syncs.Load()
			if err := db.UpdateBatch(ctx, updates, nil); err != nil {
				t.Fatal(err)
			}
			want := before
			if interval == 0 {
				want++
			}
			if fs.syncs.Load() != want {
				t.Fatalf("syncs %d want %d", fs.syncs.Load(), want)
			}
			for _, name := range []string{"a", "b"} {
				got, err := db.Metadata(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				revision := uint64(2)
				if name == "a" {
					revision++
				}
				if got.Revision != revision {
					t.Fatalf("%s revision %d", name, got.Revision)
				}
			}
			closeForRecovery(t, db, strict, true)
			recovered, _ := openTestStore(t, "/store", Options{fs: strict, ExactBlocks: true, Continuation: batchTestCodec{}, MetadataCacheSize: 64 << 20})
			defer closeTestStore(t, recovered)
			for _, name := range []string{"a", "b"} {
				m, err := recovered.Metadata(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				value, err := recovered.newContinuationIO(ctx, recovered.db, nil, m).Get([]byte("point"))
				if err != nil {
					t.Fatal(err)
				}
				want := float64(0)
				revision := uint64(1)
				if interval == 0 {
					want = 1
					revision = 2
					if name == "a" {
						want = 2
						revision = 3
					}
				}
				if math.Float64frombits(binary.BigEndian.Uint64(value)) != want || m.Revision != revision {
					t.Fatalf("%s recovered %+v value %x", name, m, value)
				}
			}
		})
	}
}

func TestUpdateBatchPrepareFailureIsAtomic(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, Continuation: batchTestCodec{}, MetadataCacheSize: 64 << 20})
	defer closeTestStore(t, db)
	for _, name := range []string{"a", "b"} {
		createTestMetric(t, db, name)
	}
	before := db.Stats().StateCache.Installs
	if err := db.UpdateBatch(ctx, []MetricUpdate{{Name: "a", Points: []Point{{Value: 2}}}, {Name: "b", Points: []Point{{Value: -1}}}}, nil); err == nil {
		t.Fatal("prepare error lost")
	}
	if db.Stats().StateCache.Installs != before {
		t.Fatal("failed batch published cache state")
	}
	for _, name := range []string{"a", "b"} {
		m, err := db.Metadata(ctx, name)
		if err != nil || m.Revision != 1 {
			t.Fatalf("partial update %+v %v", m, err)
		}
		value, err := db.newContinuationIO(ctx, db.db, nil, m).Get([]byte("point"))
		if err != nil || binary.BigEndian.Uint64(value) != 0 {
			t.Fatal("partial data commit")
		}
	}
}

func TestUpdateBatchConcurrentOrdering(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, Continuation: batchTestCodec{}, MetadataCacheSize: 64 << 20})
	defer closeTestStore(t, db)
	names := []string{"a", "b"}
	for _, name := range names {
		createTestMetric(t, db, name)
	}
	var wg sync.WaitGroup
	for worker := range 3 {
		wg.Go(func() {
			for range 20 {
				if worker == 0 {
					for _, name := range names {
						if err := db.UpdateMany(ctx, name, []Point{{Value: 1}}); err != nil {
							t.Error(err)
						}
					}
				} else {
					first, second := names[worker%2], names[1-worker%2]
					if err := db.UpdateBatch(ctx, []MetricUpdate{{Name: first, Points: []Point{{Value: 1}}}, {Name: second, Points: []Point{{Value: 1}}}}, nil); err != nil {
						t.Error(err)
					}
				}
			}
		})
	}
	wg.Wait()
	for _, name := range names {
		m, err := db.Metadata(ctx, name)
		if err != nil || m.Revision != 61 {
			t.Fatalf("lost ordered write %+v %v", m, err)
		}
		value, err := db.newContinuationIO(ctx, db.db, nil, m).Get([]byte("point"))
		if err != nil || math.Float64frombits(binary.BigEndian.Uint64(value)) != 60 {
			t.Fatal("concurrent update lost")
		}
	}
}

func BenchmarkPerCallCommit(b *testing.B) {
	for _, syncWAL := range []bool{false, true} {
		for _, size := range []int{1, 8} {
			b.Run(fmt.Sprintf("sync=%v/group=%d", syncWAL, size), func(b *testing.B) {
				interval := time.Hour
				if syncWAL {
					interval = 0
				}
				db, err := Open(b.TempDir(), Options{SyncInterval: interval, CacheSize: 8 << 20, MemTableSize: 4 << 20})
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()
				// One 128-byte changed header and one revision per update; all remaining
				// cost is allocation/indexing, WAL submission and applying the same keys.
				keys := make([][]byte, 2*size)
				for i := range keys {
					keys[i] = []byte(fmt.Sprintf("benchmark/%04d", i))
				}
				value := make([]byte, 128)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					batch := db.db.NewIndexedBatch()
					for _, key := range keys {
						if err := batch.Set(key, value, nil); err != nil {
							b.Fatal(err)
						}
					}
					if err := db.commit(batch); err != nil {
						b.Fatal(err)
					}
					batch.Close()
				}
				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/update")
			})
		}
	}
}

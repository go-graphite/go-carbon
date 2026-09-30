package persister

import (
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
)

func TestStoreMutexDistributionWithinCacheShard(t *testing.T) {
	c := cache.New()
	shard := c.GetShard("test.store.lock_distribution")
	const samples = 256
	locks := make(map[uint64]struct{})
	matched := 0
	for candidate := 0; candidate < 1<<20 && matched < samples; candidate++ {
		metric := fmt.Sprintf("test.store.metric.%d", candidate)
		if c.GetShard(metric) != shard {
			continue
		}
		index := storeMutexIndex(metric)
		if index >= storeMutexCount {
			t.Fatalf("mutex index %d is outside the lock array", index)
		}
		locks[index] = struct{}{}
		matched++
	}
	if matched != samples {
		t.Fatalf("found %d same-shard metrics; want %d", matched, samples)
	}
	// The noop queue emits one cache shard at a time. Reusing its low hash bits
	// restricts every such group to only 32 of the 32768 persister locks.
	if len(locks) < samples/2 {
		t.Fatalf("%d same-shard metrics use only %d store locks; want at least %d", samples, len(locks), samples/2)
	}
	t.Logf("%d same-shard metrics use %d store locks", samples, len(locks))
}

func TestStoreSerializesSameMetric(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("compressed=%t", compressed), func(t *testing.T) {
			dir := t.TempDir()
			pending := &fakeCache{}
			p := newOOOTestPersister(t, dir, pending)
			p.SetCompressed(compressed)
			p.schemas[0].Compressed = &compressed
			// File locks must not mask a regression in the in-process store lock.
			p.SetFLock(false)

			const metric = "test.store_serialization"
			base := int(time.Now().Unix()) - 3600
			pending.add(metric, int64(base), 0)
			p.store(metric)

			var sequence, active atomic.Int32
			var overlap atomic.Bool
			p.pop = func(name string) (*points.Points, bool) {
				if active.Add(1) != 1 {
					overlap.Store(true)
				}
				i := sequence.Add(1)
				runtime.Gosched()
				return &points.Points{
					Metric: name,
					Data: []points.Point{{
						Timestamp: int64(base) + int64(i),
						Value:     float64(i),
					}},
				}, true
			}
			p.confirm = func(*points.Points) { active.Add(-1) }

			const workers = 32
			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(workers)
			for i := 0; i < workers; i++ {
				go func() {
					defer wg.Done()
					<-start
					p.store(metric)
				}()
			}
			close(start)
			wg.Wait()

			if overlap.Load() {
				t.Error("same-metric stores overlapped between pop and confirm")
			}
			if got := active.Load(); got != 0 {
				t.Errorf("%d stores were not confirmed", got)
			}
			if got := sequence.Load(); got != workers {
				t.Errorf("performed %d stores; want %d", got, workers)
			}
			path := filepath.Join(dir, "test", "store_serialization.wsp")
			for i := 0; i <= workers; i++ {
				if got := fetchValue(t, path, base+i); got != float64(i) {
					t.Errorf("value at offset %d = %v; want %d", i, got, i)
				}
			}
		})
	}
}

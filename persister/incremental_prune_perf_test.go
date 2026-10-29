package persister

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
)

type reclaimFixtureStore struct {
	IncrementalMetricStore
	completed atomic.Int64
	total     int64
	done      chan struct{}
}

func (s *reclaimFixtureStore) CompleteFileCleanup(ctx context.Context, m store.Metadata) error {
	if err := s.IncrementalMetricStore.CompleteFileCleanup(ctx, m); err != nil {
		return err
	}
	if s.completed.Add(1) == s.total {
		close(s.done)
	}
	return nil
}

// This fixture measures real snapshot conversion, ownership publication, batched
// unlink/sync and catalog completion. An overlay supplies the no-prune baseline;
// both paths must preserve every source value and reclaim every regular file.
func TestConversionReclaimThroughputFixture(t *testing.T) {
	if os.Getenv("GO_CARBON_PRUNE_PERF") != "1" {
		t.Skip("opt-in conversion/reclaim performance fixture")
	}
	stream := os.Getenv("GO_CARBON_PRUNE_STREAM") == "1"
	const metrics, dirs, workers = 512, 64, 16
	for trial := 0; trial < 5; trial++ {
		t.Run(fmt.Sprint(trial), func(t *testing.T) {
			p, _, db, _ := incrementalPersisterFixture(t)
			defer db.Close()
			ctx := context.Background()
			source := filepath.Join(p.rootPath, "metric.wsp")
			want, err := whisperio.ReadSnapshot(ctx, "metric", source)
			if err != nil {
				t.Fatal(err)
			}
			main, err := os.ReadFile(source)
			if err != nil {
				t.Fatal(err)
			}
			sidecar, err := os.ReadFile(whisper.OutOfOrderSidecarPath(source))
			if err != nil {
				t.Fatal(err)
			}
			names := make([]string, metrics)
			for i := range names {
				names[i] = fmt.Sprintf("bench.d%03d.leaf.m%04d", i%dirs, i)
				path, err := p.incrementalPath(names[i])
				if err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, main, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(whisper.OutOfOrderSidecarPath(path), sidecar, 0600); err != nil {
					t.Fatal(err)
				}
			}
			main, sidecar = nil, nil
			wrapped := &reclaimFixtureStore{IncrementalMetricStore: db, total: metrics, done: make(chan struct{})}
			p.incrementalStore = wrapped
			p.SetConversionIOSlots(64)
			delay := time.Hour
			if stream {
				delay = time.Nanosecond
			}
			p.SetDeferredReclaim(delay)
			defer p.StopDeferredReclaim()
			tasks := make(chan int, workers)
			var wg sync.WaitGroup
			began := time.Now()
			count := metrics
			if stream {
				count = dirs
			}
			for i := 0; i < workers; i++ {
				wg.Go(func() {
					for task := range tasks {
						if stream {
							for metric := task; metric < metrics; metric += dirs {
								if _, err := p.ConvertFile(ctx, names[metric]); err != nil {
									t.Error(err)
								}
							}
							select {
							case p.reclaim.wake <- struct{}{}:
							default:
							}
						} else if _, err := p.ConvertFile(ctx, names[task]); err != nil {
							t.Error(err)
						}
					}
				})
			}
			for i := 0; i < count; i++ {
				tasks <- i
			}
			close(tasks)
			wg.Wait()
			if t.Failed() {
				return
			}
			staged := time.Now()
			r := p.reclaim
			if !stream {
				r.mu.Lock()
				for _, q := range r.dirs {
					for i := range q.jobs {
						q.jobs[i].due = 0
					}
				}
				heap.Init(&r.ready)
				r.mu.Unlock()
				r.wake <- struct{}{}
			}
			select {
			case <-wrapped.done:
			case <-time.After(60 * time.Second):
				t.Fatal("reclaim did not drain")
			}
			p.StopDeferredReclaim()
			finished := time.Now()
			for _, name := range names {
				got, err := db.Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				want.Metadata.Name = name
				if err := store.EqualSnapshots(want, got); err != nil {
					t.Fatal(err)
				}
				if got.Metadata.FileMigration != store.FileReclaimed {
					t.Fatal("cleanup not durable")
				}
				assertOriginalsReclaimed(t, p, name)
			}
			_, parentErr := os.Stat(filepath.Join(p.rootPath, "bench"))
			reclaimTime := finished.Sub(staged).Seconds()
			if stream {
				reclaimTime = finished.Sub(began).Seconds()
			}
			report := map[string]any{"stream": stream, "metrics": metrics, "trial": trial, "conversion_seconds": staged.Sub(began).Seconds(), "reclaim_seconds": reclaimTime, "total_seconds": finished.Sub(began).Seconds(), "reclaim_per_second": metrics / reclaimTime, "conversions_per_second": metrics / finished.Sub(began).Seconds(), "parent_remaining": parentErr == nil}
			data, _ := json.Marshal(report)
			t.Log("PRUNE_FIXTURE " + string(data))
		})
	}
}

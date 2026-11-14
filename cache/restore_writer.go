package cache

import (
	"sync"

	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/tags"
	"github.com/zeebo/xxh3"
)

const maxRestorePendingPoints = 1 << 20 // 16 MiB of decoded points, plus one oversized record

// RestoreWriter admits old dump history with receivers closed. Each cache shard
// has exactly one consumer, preserving metric and per-shard journal order while
// an unrelated shard is busy with writeout. Add takes ownership of its argument.
// Flush must finish before syncing the input log or retiring a source file.
// Close joins all workers. The producer must serialize Add, Flush and Close.
type RestoreWriter struct {
	cache         *Cache
	queues        []chan *points.Points
	workers       sync.WaitGroup
	pending       sync.WaitGroup
	mu            sync.Mutex
	space         *sync.Cond
	pendingPoints int
}

// RecoveryDraining reports whether startup is draining a finite history prefix.
func (c *Cache) RecoveryDraining() bool { return c.recoveryDrain.Load() }

// NewRestoreWriter invokes consume concurrently, keeping one consumer per cache
// shard. The callback must accept owned records and be safe across shards.
func (c *Cache) NewRestoreWriter(workers int, consume func(*points.Points)) *RestoreWriter {
	w := &RestoreWriter{cache: c, queues: make([]chan *points.Points, min(max(workers, 1), shardCount))}
	w.space = sync.NewCond(&w.mu)
	for i := range w.queues {
		// One queued record per worker bounds read-ahead independently of dump size.
		q := make(chan *points.Points, 1)
		w.queues[i] = q
		w.workers.Go(func() {
			for p := range q {
				weight := restoreWeight(p)
				consume(p)
				w.mu.Lock()
				w.pendingPoints -= weight
				w.space.Broadcast()
				w.mu.Unlock()
				w.pending.Done()
			}
		})
	}
	return w
}

func (w *RestoreWriter) Add(p *points.Points) {
	name := p.Metric
	// Match intake's name-limit-before-normalization policy. Invalid names still
	// go through ordinary admission for its error/refusal accounting.
	if len(name) <= points.MaxMetricNameBytes && w.cache.settings.Load().(*cacheSettings).tagsEnabled {
		if normalized, err := tags.Normalize(name); err == nil {
			name = normalized
		}
	}
	shard := xxh3.HashString(name) & (shardCount - 1)
	w.mu.Lock()
	for w.pendingPoints+restoreWeight(p) > maxRestorePendingPoints {
		w.space.Wait()
	}
	w.pendingPoints += restoreWeight(p)
	w.mu.Unlock()
	w.pending.Add(1)
	w.queues[int(shard)%len(w.queues)] <- p
}

func (w *RestoreWriter) Flush() { w.pending.Wait() }

func (w *RestoreWriter) Close() {
	for _, q := range w.queues {
		close(q)
	}
	w.workers.Wait()
}

// An oversized record runs alone; the reader already owns its allocation.
func restoreWeight(p *points.Points) int { return min(max(len(p.Data), 1), maxRestorePendingPoints) }

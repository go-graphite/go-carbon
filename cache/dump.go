package cache

import (
	"errors"
	"io"
	"iter"
	"sync"
	"sync/atomic"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

// visitLocked calls write for each in-flight batch of the read-locked shard,
// then for each queued metric. Queued points are decoded into scratch, which
// write must not retain.
func (shard *Shard) visitLocked(scratch *points.Points, write func(*points.Points) error) error {
	for _, p := range shard.notConfirmed[:shard.notConfirmedUsed] {
		if p != nil {
			if err := write(p); err != nil {
				return err
			}
		}
	}
	for _, values := range shard.items.all {
		scratch.Metric = values.Metric
		scratch.Data = values.compact.appendDecoded(scratch.Data[:0])
		if err := write(scratch); err != nil {
			return err
		}
	}
	scratch.Metric = ""
	return nil
}

func (c *Cache) Dump(w io.Writer) error {
	return c.DumpShards(0, shardCount, func(p *points.Points) error {
		_, err := p.WriteTo(w)
		return err
	})
}

// DumpPoints visits a stable cache after persisters have stopped and input has
// been diverted. Each callback completes while its shard is read-locked and
// must not retain its argument. Saved metrics that recovery has not claimed
// yet are included.
func (c *Cache) DumpPoints(write func(*points.Points) error) error {
	if err := c.dumpPending(write); err != nil {
		return err
	}
	return c.DumpShards(0, shardCount, write)
}

func (c *Cache) DumpBinary(w io.Writer) error {
	var buffer []byte
	return c.DumpPoints(func(p *points.Points) error {
		buffer = p.AppendBinary(buffer[:0])
		n, err := w.Write(buffer)
		if err == nil && n != len(buffer) {
			err = io.ErrShortWrite
		}
		return err
	})
}

// ShardCount is the number of independent cache shards for DumpShards.
const ShardCount = shardCount

// VisitMetricNames visits currently cached and in-flight names while input and
// persistence may continue. Names can repeat or disappear after the visit; this
// is a warmup hint, not an authoritative snapshot. visit runs under a shard's
// read lock and must not write to the cache.
func (c *Cache) VisitMetricNames(visit func(string) error) error {
	for _, shard := range c.data {
		shard.mu.RLock()
		err := shard.visitNamesLocked(visit)
		shard.mu.RUnlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// VisitMetricNamesParallel is VisitMetricNames on up to workers goroutines,
// each visiting whole shards; visit must be safe for concurrent use. It
// returns one of the errors visit returned.
func (c *Cache) VisitMetricNamesParallel(workers int, visit func(string) error) error {
	workers = max(1, min(workers, shardCount))
	var next atomic.Int64
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := int(next.Add(1) - 1); i < shardCount; i = int(next.Add(1) - 1) {
				shard := c.data[i]
				shard.mu.RLock()
				err := shard.visitNamesLocked(visit)
				shard.mu.RUnlock()
				if err != nil {
					errs[w] = err
					next.Store(shardCount)
					return
				}
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// MetricNames returns the names of the metrics the cache holds, shard by
// shard, for warming storage ahead of the persister. Each shard's names are
// copied under its read lock, so the sequence's consumer holds no cache lock;
// like VisitMetricNames, it is a hint, not a snapshot.
func (c *Cache) MetricNames() iter.Seq[string] {
	return func(yield func(string) bool) {
		var names []string
		for _, shard := range c.data {
			names = names[:0]
			shard.mu.RLock()
			for name := range shard.items.all {
				names = append(names, name)
			}
			shard.mu.RUnlock()
			for _, name := range names {
				if !yield(name) {
					return
				}
			}
		}
	}
}

// PendingNames returns the names of every metric an attached pending recovery
// holds, claimed or not, in the order RecoverPending claims them, for warming
// storage ahead of the persister. It is empty without a pending recovery.
func (c *Cache) PendingNames() iter.Seq[string] {
	return func(yield func(string) bool) {
		r := c.pending.Load()
		if r == nil {
			return
		}
		_ = r.bundle.VisitMetrics(func(_ uint64, name string) error {
			if !yield(name) {
				return errStopNames
			}
			return nil
		})
	}
}

var errStopNames = errors.New("names no longer wanted")

// visitNamesLocked visits names like visitLocked, without decoding points.
func (shard *Shard) visitNamesLocked(visit func(string) error) error {
	for _, p := range shard.notConfirmed[:shard.notConfirmedUsed] {
		if p != nil {
			if err := visit(p.Metric); err != nil {
				return err
			}
		}
	}
	for name := range shard.items.all {
		if err := visit(name); err != nil {
			return err
		}
	}
	return nil
}

// DumpShardsCompact visits shards [lo, hi) like DumpShards, passing each
// record in the existing recovery.FormatCompact byte encoding. The bit-packed
// memory format is deliberately never persisted, so older binaries can read
// every dump after a rollback. write must not retain data.
func (c *Cache) DumpShardsCompact(lo, hi int, write func(metric string, data []byte, n int) error) error {
	var scratch []byte
	var decoded []points.Point
	for _, shard := range c.data[lo:hi] {
		shard.mu.RLock()
		err := shard.visitCompactLocked(&scratch, &decoded, write)
		shard.mu.RUnlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// DumpShardsCompactTo transcodes queued points into the stable disk codec.
// Segment must copy the reused scratch, including for block dumps.
func (c *Cache) DumpShardsCompactTo(lo, hi int, out *recovery.Segment) error {
	return c.DumpShardsCompact(lo, hi, out.WriteCompact)
}

func (shard *Shard) visitCompactLocked(scratch *[]byte, decoded *[]points.Point, write func(string, []byte, int) error) error {
	for _, p := range shard.notConfirmed[:shard.notConfirmedUsed] {
		if p != nil && len(p.Data) > 0 {
			*scratch = recovery.AppendCompactPoints((*scratch)[:0], p.Data)
			if err := write(p.Metric, *scratch, len(p.Data)); err != nil {
				return err
			}
		}
	}
	for name, values := range shard.items.all {
		if values.compact.n > 0 {
			*decoded = values.compact.appendDecoded((*decoded)[:0])
			*scratch = recovery.AppendCompactPoints((*scratch)[:0], *decoded)
			if err := write(name, *scratch, len(*decoded)); err != nil {
				return err
			}
		}
	}
	return nil
}

// DumpShards visits shards [lo, hi) like DumpPoints, without pending metrics.
// Each metric lives in exactly one shard, so disjoint ranges share no metric.
func (c *Cache) DumpShards(lo, hi int, write func(*points.Points) error) error {
	var scratch points.Points
	for _, shard := range c.data[lo:hi] {
		shard.mu.RLock()
		err := shard.visitLocked(&scratch, write)
		shard.mu.RUnlock()
		if err != nil {
			return err
		}
	}
	return nil
}

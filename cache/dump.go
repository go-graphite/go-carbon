package cache

import (
	"io"

	"github.com/go-graphite/go-carbon/points"
)

func (c *Cache) Dump(w io.Writer) error {
	for i := 0; i < shardCount; i++ {
		shard := c.data[i]
		shard.mu.RLock()

		for _, p := range shard.notConfirmed[:shard.notConfirmedUsed] {
			if p == nil {
				continue
			}
			if _, err := p.WriteTo(w); err != nil {
				shard.mu.RUnlock()
				return err
			}
		}

		for _, p := range shard.items {
			if _, err := p.WriteTo(w); err != nil {
				shard.mu.RUnlock()
				return err
			}
		}

		shard.mu.RUnlock()
	}

	return nil
}

// DumpPoints visits a stable cache after persisters have stopped and input has
// been diverted. Each callback completes while its shard is read-locked. Saved
// metrics that recovery has not claimed yet are included.
func (c *Cache) DumpPoints(write func(*points.Points) error) error {
	if err := c.dumpPending(write); err != nil {
		return err
	}
	for _, shard := range c.data {
		shard.mu.RLock()
		for _, p := range shard.notConfirmed[:shard.notConfirmedUsed] {
			if p != nil {
				if err := write(p); err != nil {
					shard.mu.RUnlock()
					return err
				}
			}
		}
		for _, p := range shard.items {
			if err := write(p); err != nil {
				shard.mu.RUnlock()
				return err
			}
		}
		shard.mu.RUnlock()
	}
	return nil
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

// DumpPending writes saved metrics recovery has not claimed (see DumpPoints).
func (c *Cache) DumpPending(write func(*points.Points) error) error { return c.dumpPending(write) }

// ShardCount is the number of independent cache shards for DumpShards.
const ShardCount = shardCount

// DumpShards visits shards [lo, hi) like DumpPoints, without pending metrics.
// Each metric lives in exactly one shard, so disjoint ranges share no metric.
func (c *Cache) DumpShards(lo, hi int, write func(*points.Points) error) error {
	for _, shard := range c.data[lo:hi] {
		shard.mu.RLock()
		for _, p := range shard.notConfirmed[:shard.notConfirmedUsed] {
			if p != nil {
				if err := write(p); err != nil {
					shard.mu.RUnlock()
					return err
				}
			}
		}
		for _, p := range shard.items {
			if err := write(p); err != nil {
				shard.mu.RUnlock()
				return err
			}
		}
		shard.mu.RUnlock()
	}
	return nil
}

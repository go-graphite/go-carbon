package cache

import (
	"context"
	"errors"
)

// FenceEmptyMetric excludes intake, pop and confirm through publication. An
// in-flight batch remains in notConfirmed until durable writer confirmation.
// Callers acquire the store stripe first, matching confirmation lock order.
func (c *Cache) FenceEmptyMetric(ctx context.Context, name string, publish func() error) error {
	shard := c.GetShard(name)
	if !shard.mu.TryRLock() {
		return errors.New("finite patch cache shard busy")
	}
	defer shard.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	// Never perform a recovery index read while excluding intake. An active
	// recovery source or an unusually large in-flight shard is unavailable,
	// even if it might belong to other names. This bounds fence work.
	if r := c.pending.Load(); r != nil && r.remaining.Load() != 0 {
		return errors.New("finite patch refuses active recovery source")
	}
	if shard.notConfirmedUsed > 256 {
		return errors.New("finite patch in-flight census exceeds bound")
	}
	if shard.items.get(name) != nil {
		return errors.New("finite patch refuses queued, recovered or in-flight input")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, p := range shard.notConfirmed[:shard.notConfirmedUsed] {
		if p != nil && p.Metric == name {
			return errors.New("finite patch refuses in-flight input")
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return publish()
}

package cache

import "io"

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

func (c *Cache) DumpBinary(w io.Writer) error {
	for i := 0; i < shardCount; i++ {
		shard := c.data[i]
		shard.mu.Lock()

		for _, p := range shard.notConfirmed[:shard.notConfirmedUsed] {
			if p == nil {
				continue
			}
			if _, err := p.WriteBinaryTo(w); err != nil {
				shard.mu.Unlock()
				return err
			}
		}

		for _, p := range shard.items {
			if _, err := p.WriteBinaryTo(w); err != nil {
				shard.mu.Unlock()
				return err
			}
		}

		shard.mu.Unlock()
	}

	return nil
}

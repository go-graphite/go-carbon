package chunkstore

import (
	"context"
	"time"

	"github.com/go-graphite/go-carbon/internal/phasecpu"
)

// MetadataCached is an advisory scheduling hint for authoritative state.
// Provisional legacy checkpoint records count as cold: the writeout coordinator
// offers them to ValidateMetadataCheckpoint before admitting their writes.
// Ordinary readers/writers still perform their authoritative consistency checks.
func (s *Store) MetadataCached(name string) bool {
	c := s.stateCache
	if c == nil {
		return false
	}
	sh := &c.shards[s.metricStripe(name)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	_, slot := sh.find(c.hash(name), name)
	return sh.entries[name] != nil || slot >= 0 && sh.slots[slot].epoch&provisionalState == 0
}

// PrewarmWriteout loads a bounded queue window without taking metric locks.
// It does not validate provisional checkpoint records; callers offer the same
// window to ValidateMetadataCheckpoint first. Failed/raced/refused fills leave
// the ordinary cold write path as the fallback. The caller admits this CPU as
// required writeout work before calling.
func (s *Store) PrewarmWriteout(ctx context.Context, names []string) {
	c := s.stateCache
	if c == nil || c.bytes.Load() >= int64(float64(c.budget())*prewarmFullFraction) {
		return
	}
	ctx, end, ok := s.prewarm.begin(ctx)
	if !ok {
		return
	}
	defer end()
	phasecpu.Set(phasecpu.MetadataCache)
	defer phasecpu.Set(phasecpu.Other)
	started := time.Now()
	defer func() { c.prewarmNanos.Add(uint64(time.Since(started))) }()
	loader, _ := s.continuation.(ContinuationStateLoader)
	if c.keys == nil {
		loader = nil
	}
	var stats PrewarmStats
	for len(names) > 0 && ctx.Err() == nil {
		n := min(len(names), DefaultPrewarmBatch)
		batch := make([]string, 0, n)
		for _, name := range names[:n] {
			if !c.has(s.metricStripe(name), name) {
				batch = append(batch, name)
			}
		}
		stats.Names += int64(len(batch))
		s.prewarmBatch(ctx, c, loader, batch, &stats)
		names = names[n:]
	}
	c.recordPrewarm(stats)
}

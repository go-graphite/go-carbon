package cache

import (
	"sync/atomic"

	"github.com/go-graphite/go-carbon/points"
)

// RecoveryStream identifies one ordered durable input stream. A metric must
// remain on the same stream until its queued and in-flight batches persist.
// Sources can be reclaimed only as a contiguous prefix of this stream.
type RecoveryStream struct{ next atomic.Uint64 }
type RecoverySource struct {
	stream   *RecoveryStream
	sequence uint64
	journal  bool
	pending  atomic.Int64
}

func (s *RecoveryStream) Next() *RecoverySource {
	return &RecoverySource{stream: s, sequence: s.next.Add(1)}
}

// NextJournal identifies records whose durable authority is the continuous
// input log itself. Ordinary capture files must not pin/relog that journal.
func (s *RecoveryStream) NextJournal() *RecoverySource {
	source := s.Next()
	source.journal = true
	return source
}
func (r recoverySourceRefs) needsJournal() bool {
	if r.first != nil && r.first.journal {
		return true
	}
	for _, source := range r.rest {
		if source.journal {
			return true
		}
	}
	return false
}
func (s *RecoverySource) Pending() int64  { return s.pending.Load() }
func (c *Cache) HasPendingRecovery() bool { return c.pending.Load() != nil }

// AddRecoveredFrom retains the earliest source for the actual cache-batch
// identity, including replacements/retries. No file I/O or callback runs under
// the cache lock. The source owner polls Pending and reclaims only a prefix.
func (c *Cache) AddRecoveredFrom(p *points.Points, source *RecoverySource) {
	c.addWithRestore(p, true, true, false, nil, source)
}

// A batch can contain one previous epoch and its current partition. Retain the
// earliest token in each stream; prefix retirement protects later tokens.
type recoverySourceRefs struct {
	first *RecoverySource
	rest  []*RecoverySource
}

func (r recoverySourceRefs) each(visit func(*RecoverySource)) {
	if r.first != nil {
		visit(r.first)
	}
	for _, s := range r.rest {
		visit(s)
	}
}

// AddCapturedFrom keeps the normal batching clock for an owner already at
// its live frontier. Its record is already durable; shared admission has
// reserved capacity, so applying live drop quotas again would lose that record.
func (c *Cache) AddCapturedFrom(p *points.Points, source *RecoverySource) {
	c.addWithRestore(p, false, true, false, nil, source)
}
func (s *Shard) trackSource(p *points.Points, source *RecoverySource) {
	if source == nil {
		return
	}
	if s.recoverySources == nil {
		s.recoverySources = make(map[*points.Points]recoverySourceRefs)
	}
	refs := s.recoverySources[p]
	switch {
	case refs.first == nil:
		refs.first = source
		source.pending.Add(1)
	case refs.first.stream == source.stream:
		if refs.first.sequence <= source.sequence {
			return
		}
		refs.first.pending.Add(-1)
		refs.first = source
		source.pending.Add(1)
	default:
		for i, old := range refs.rest {
			if old.stream == source.stream {
				if old.sequence <= source.sequence {
					return
				}
				old.pending.Add(-1)
				refs.rest[i] = source
				source.pending.Add(1)
				s.recoverySources[p] = refs
				return
			}
		}
		refs.rest = append(refs.rest, source)
		source.pending.Add(1)
	}
	s.recoverySources[p] = refs
}
func (s *Shard) releaseSource(p *points.Points) {
	refs := s.recoverySources[p]
	refs.each(func(source *RecoverySource) { source.pending.Add(-1) })
	delete(s.recoverySources, p)
	if len(s.recoverySources) == 0 {
		s.recoverySources = nil
	}
}
func (s *Shard) moveSource(from, to *points.Points) {
	refs := s.recoverySources[from]
	refs.each(func(source *RecoverySource) { s.trackSource(to, source) })
	s.releaseSource(from)
}

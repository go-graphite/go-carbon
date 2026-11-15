package whisperio

import (
	"bytes"
	"context"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// OwnerHeads is an experimental adapter retaining decoded physical groups.
// It keeps the existing virtual open, archive state machine, encoding and WALs.
// Each owner serializes preparation; the store still owns transaction ordering.
// Retained state becomes visible only through the database commit callback.
type OwnerHeads struct {
	owners               []headOwner
	budget               int64
	hits, reads, decodes atomic.Uint64
}
type headOwner struct {
	sync.Mutex
	entries map[string]retainedHead
	bytes   int64
}
type retainedHead struct {
	scope                    *byte
	id, generation, revision uint64
	groups                   map[string]*store.PointBlock
	bytes                    int64
}

// NewOwnerHeads returns nil when owners is zero. A zero byte budget enables
// owner assignment alone, independently of decoded-group retention. The budget
// covers retained state with conservative map/allocator overhead; ordinary
// operation scratch and in-flight commit candidates are additional and bounded
// by the caller's existing worker/transaction admission.
func NewOwnerHeads(owners int, budget int64) *OwnerHeads {
	if owners <= 0 {
		return nil
	}
	owners = min(owners, 1024)
	return &OwnerHeads{owners: make([]headOwner, owners), budget: max(0, budget) / int64(owners)}
}

// Counts reports retained-group hits, backing group reads and actual decodes,
// including write-through refresh decodes. No virtual opens are avoided.
func (p *OwnerHeads) Counts() (hits, reads, decodes uint64) {
	return p.hits.Load(), p.reads.Load(), p.decodes.Load()
}

func (p *OwnerHeads) owner(name string) *headOwner {
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	return &p.owners[h.Sum64()%uint64(len(p.owners))]
}

func (p *OwnerHeads) update(c *ContinuationCodec, ctx context.Context, kv store.ContinuationIO, m store.Metadata, input []store.Point, retention int, clock func() time.Time, maintenance *store.ContinuationMaintenance) error {
	o := p.owner(m.Name)
	o.Lock()
	defer o.Unlock()
	observer, transactional := kv.(store.ContinuationCommitObserver)
	if !transactional || observer.ContinuationCacheScope() == nil {
		return c.updateWithSumCache(ctx, kv, m, input, retention, clock, maintenance)
	}
	head := &ownerHeadSession{pool: p, groups: make(map[string]*store.PointBlock)}
	if old, ok := o.entries[m.Name]; ok && old.scope == observer.ContinuationCacheScope() && old.id == m.ID && old.generation == m.Generation && old.revision == m.Revision {
		for k, v := range old.groups {
			head.groups[k] = v
			head.bytes += headGroupCharge(k)
		}
	}
	owned := &ownerHeadIO{ContinuationIO: kv, head: head}
	if err := c.updateWithSumCache(ctx, owned, m, input, retention, clock, maintenance); err != nil {
		return err
	}
	head.encoded = nil
	if p.budget == 0 {
		return nil
	}
	// The candidate is private until the entire store batch has committed. On
	// failure the callback and candidate die with the transaction.
	observer.AfterContinuationCommit(func(committed store.Metadata) {
		o.Lock()
		defer o.Unlock()
		charge := head.bytes + int64(len(m.Name)) + 512
		if charge > p.budget {
			return
		}
		if old, ok := o.entries[m.Name]; ok {
			o.bytes -= old.bytes
			delete(o.entries, m.Name)
		}
		if o.bytes+charge > p.budget {
			o.entries = nil
			o.bytes = 0
		}
		if o.entries == nil {
			o.entries = make(map[string]retainedHead)
		}
		o.entries[m.Name] = retainedHead{observer.ContinuationCacheScope(), committed.ID, committed.Generation, committed.Revision, head.groups, charge}
		o.bytes += charge
	})
	return nil
}

type ownerHeadSession struct {
	pool     *OwnerHeads
	groups   map[string]*store.PointBlock
	encoded  map[string][]byte
	bytes    int64
	attempts int
}

// Charge exceeds PointBlock's 2064 bytes and includes key/map/allocator space.
func headGroupCharge(key string) int64 { return 4096 + int64(len(key)) }
func (s *ownerHeadSession) clear()     { clear(s.groups); clear(s.encoded); s.bytes = 0 }
func (s *ownerHeadSession) nextAttempt() {
	if s == nil {
		return
	}
	s.attempts++
	if s.attempts > 1 {
		s.clear()
	}
}
func (s *ownerHeadSession) point(key []byte) *store.PointBlock {
	if s == nil {
		return nil
	}
	b := s.groups[string(key)]
	if b == nil {
		return nil
	}
	s.pool.hits.Add(1)
	owned := *b
	return &owned
}
func (s *ownerHeadSession) forget(key []byte) {
	if s == nil {
		return
	}
	if _, ok := s.groups[string(key)]; ok {
		s.bytes -= headGroupCharge(string(key))
		delete(s.groups, string(key))
		delete(s.encoded, string(key))
	}
}
func (s *ownerHeadSession) remember(key []byte, b *store.PointBlock) {
	if s == nil {
		return
	}
	s.forget(key)
	charge := headGroupCharge(string(key))
	// Bound each transaction candidate as well as each owner's retained set.
	if charge+512 > s.pool.budget {
		return
	}
	if s.bytes+charge+512 > s.pool.budget || len(s.groups) >= 64 {
		s.clear()
	}
	owned := *b
	s.groups[string(key)] = &owned
	s.bytes += charge
}

type ownerHeadIO struct {
	store.ContinuationIO
	head *ownerHeadSession
}

func (o *ownerHeadIO) TimeBuckets() bool {
	b, ok := o.ContinuationIO.(store.ContinuationTimeBuckets)
	return ok && b.TimeBuckets()
}
func (o *ownerHeadIO) ContinuationRevision() uint64 {
	b, ok := o.ContinuationIO.(store.ContinuationRevision)
	if ok {
		return b.ContinuationRevision()
	}
	return 0
}
func (o *ownerHeadIO) Probe(k []byte) ([]byte, error) {
	if p, ok := o.ContinuationIO.(store.ContinuationProber); ok {
		return p.Probe(k)
	}
	return o.Get(k)
}
func (o *ownerHeadIO) Set(k, v []byte) error {
	if err := o.ContinuationIO.Set(k, v); err != nil {
		return err
	}
	// Typed point writes supply both the immutable decoded group and the exact
	// bytes the codec emitted. Overlay commits may reuse them only on equality.
	if expected, ok := o.head.encoded[string(k)]; ok && bytes.Equal(expected, v) {
		return nil
	}
	// Unknown writes conservatively refresh from final encoded bytes.
	if _, ok := o.head.groups[string(k)]; ok {
		o.head.forget(k)
		var b store.PointBlock
		o.head.pool.decodes.Add(1)
		if store.DecodePointBlock(v, &b) == nil {
			o.head.remember(k, &b)
		}
	}
	return nil
}
func (o *ownerHeadIO) Delete(k []byte) error { o.head.forget(k); return o.ContinuationIO.Delete(k) }
func (o *ownerHeadIO) DeletePrefix(p []byte) error {
	for k := range o.head.groups {
		if bytes.HasPrefix([]byte(k), p) {
			o.head.forget([]byte(k))
		}
	}
	return o.ContinuationIO.DeletePrefix(p)
}

// retainEncoded follows only the adapter's private overlays. A generic mutation
// can never attach a decoded object to different bytes without a full decode.
func retainEncoded(kv store.ContinuationIO, key, value []byte, block *store.PointBlock) {
	var s *ownerHeadSession
	switch v := kv.(type) {
	case *ownerHeadIO:
		s = v.head
	case *continuationOverlay:
		retainEncoded(v.base, key, value, block)
		return
	default:
		return
	}
	s.remember(key, block)
	if _, ok := s.groups[string(key)]; !ok {
		return
	}
	if s.encoded == nil {
		s.encoded = make(map[string][]byte)
	}
	s.encoded[string(key)] = value
}

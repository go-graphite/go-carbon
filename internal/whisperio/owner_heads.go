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

// OwnerHeads retains exclusively owned resumable writers and decoded groups.
// The archive state machine, encoding, database format and WALs are unchanged.
// Each owner serializes preparation; the store still owns transaction ordering.
// Retained state becomes visible only through the database commit callback.
type OwnerHeads struct {
	owners               []headOwner
	budget               int64
	hits, reads, decodes atomic.Uint64
	opens                atomic.Uint64
}
type headOwner struct {
	sync.Mutex
	entries map[string]retainedHead
	bytes   int64
}
type retainedHead struct {
	scope                    *byte
	id, generation, revision uint64
	session                  *ownerHeadSession
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
// including write-through refresh decodes.
func (p *OwnerHeads) Counts() (hits, reads, decodes uint64) {
	return p.hits.Load(), p.reads.Load(), p.decodes.Load()
}

// Opens reports actual virtual writer opens on the instrumented update path.
func (p *OwnerHeads) Opens() uint64 { return p.opens.Load() }

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
	// Checkout removes the only committed reference before any private state is
	// advanced. A failed transaction has nothing to restore or publish. The
	// database remains the source of truth and the next update opens it normally.
	head := &ownerHeadSession{pool: p, groups: make(map[string]headGroup)}
	if old, ok := o.entries[m.Name]; ok {
		delete(o.entries, m.Name)
		o.bytes -= old.bytes
		if old.scope == observer.ContinuationCacheScope() && old.id == m.ID && old.generation == m.Generation && old.revision == m.Revision {
			head = old.session
			head.attempts = 0
		}
	}
	owned := &ownerHeadIO{ContinuationIO: kv, head: head}
	if err := c.updateWithSumCache(ctx, owned, m, input, retention, clock, maintenance); err != nil {
		return err
	}
	clear(head.encoded)
	head.detach()
	if p.budget == 0 {
		return nil
	}
	// The candidate is private until the entire store batch has committed. On
	// failure the callback and candidate die with the transaction.
	observer.AfterContinuationCommit(func(committed store.Metadata) {
		o.Lock()
		defer o.Unlock()
		fixed := head.resumeCharge() + int64(len(m.Name)) + 512
		if fixed > p.budget {
			head.resume = nil
			fixed = int64(len(m.Name)) + 512
		}
		// Prefer the resumable encoder over cold decoded groups. Evict groups
		// individually rather than periodically dropping the whole hot writer.
		for head.bytes+fixed > p.budget && len(head.groups) > 0 {
			head.evictGroup()
		}
		charge := head.bytes + fixed
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
		o.entries[m.Name] = retainedHead{observer.ContinuationCacheScope(), committed.ID, committed.Generation, committed.Revision, head, charge}
		o.bytes += charge
	})
	return nil
}

type headGroup struct {
	block *store.PointBlock
	used  uint64
}

type ownerHeadSession struct {
	pool     *OwnerHeads
	groups   map[string]headGroup
	encoded  map[string][]byte
	bytes    int64
	attempts int
	sequence uint64
	resume   *continuationHead
}

// Charge exceeds PointBlock's 2064 bytes and includes key/map/allocator space.
func headGroupCharge(key string) int64 { return 4096 + int64(len(key)) }
func (s *ownerHeadSession) clear()     { clear(s.groups); clear(s.encoded); s.bytes = 0; s.resume = nil }
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
	if b.block == nil {
		return nil
	}
	s.sequence++
	b.used = s.sequence
	s.groups[string(key)] = b
	s.pool.hits.Add(1)
	return b.block
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
	s.sequence++
	if s.groups[string(key)].block != nil {
		s.groups[string(key)] = headGroup{b, s.sequence}
		return
	}
	charge := headGroupCharge(string(key))
	// Bound each transaction candidate as well as each owner's retained set.
	if charge+512 > s.pool.budget {
		return
	}
	for s.bytes+charge+512 > s.pool.budget || len(s.groups) >= 64 {
		s.evictGroup()
	}
	s.groups[string(key)] = headGroup{b, s.sequence}
	s.bytes += charge
}

// The set is capped at 64 groups; a bounded scan avoids a list allocation per
// group while preserving active tails when header state consumes the allowance.
func (s *ownerHeadSession) evictGroup() {
	var oldest string
	var used uint64
	found := false
	for key, group := range s.groups {
		if !found || group.used < used {
			oldest, used, found = key, group.used, true
		}
	}
	if found {
		s.forget([]byte(oldest))
	}
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

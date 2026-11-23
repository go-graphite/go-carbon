package v2core

import (
	"bytes"
	"cmp"
	"container/heap"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/namefp"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
)

// RollupPolicy binds an immutable schema to its portable canonical identity.
// It is persisted in the producer catalog before journal references are synced.
type RollupPolicy struct {
	Name   partsformat.ID
	Group  partsformat.Group
	Config sealrollup.Config
}

func EncodeRollupPolicy(p RollupPolicy) ([]byte, error) {
	if p.Name == "" || p.Group == "" {
		return nil, errors.New("rollup identity/group required")
	}
	if _, _, err := partsformat.GroupRange(p.Group); err != nil {
		return nil, err
	}
	if len(p.Name) > partsformat.MaxNameBytes {
		return nil, errors.New("rollup name too long")
	}
	if err := p.Config.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(p)
}

// RollupMetadata accompanies archive replacements in the same atomic ingest.
// LastUpdate is the original admission clock in nanoseconds, clamped against
// backward clocks just as chunkstore activity is. Revision starts at creation.
type RollupMetadata struct {
	// BaselineOrder certifies original classic slots installed before intake.
	BaselineOrder uint64 `json:",omitempty"`

	// ResetOrder masks every older physical slot in all parts, including copies.
	// The producer order frontier is retained across expiry and restart.
	ResetOrder uint64 `json:",omitempty"`
	Expired    bool   `json:",omitempty"`

	Version    uint8
	ID         uint64
	Policy     []byte
	Frontier   sealrollup.Frontier
	LastUpdate int64
	Revision   uint64
}

// rollupMetric retains identity and routing; samples live in immutable parts.
type rollupMetric struct {
	*PolicyIdentity
	vshard uint16
}

type rollupCoverage struct {
	spec partsformat.Spec
	seen map[[32]byte]bool
}
type coreRollups struct {
	db          *pebble.DB // nil only for standalone, non-durable planner fixtures
	identities  *PolicyCatalog
	lanes       []rollupPlannerLane
	parallelism int
	expire      func(RollupPolicy, RollupMetadata, time.Time) bool
	now         func() time.Time
	// expiryTTL, set with expire, resolves a name's expiry window once
	// (at install, and again for all names when a returned until passes)
	// into ttl[slot], in ns (<= 0: never). expiries files each slot once under
	// expAt[slot], the second floor((LastUpdate+ttl)/1s): no later than expire
	// can first fire, since LastUpdate only moves forward. Entries under any
	// other second are stale. A pass drains only seconds at or before now and
	// asks expire only about names overdue by ttl, so it reads no other name.
	expiryTTL  func(RollupPolicy, time.Time) (time.Duration, time.Time)
	ttl        []int64
	expAt      []int64
	ttlUntil   int64
	expiries   calendar
	expDrained []int64
	expRefile  []uint32

	stage     func(string)
	metrics   map[uint64]*rollupMetric
	taken     []uint64
	due       []uint32
	planNow   int64
	planClock time.Time
	// names holds each installed name's fingerprint; pendingNames those of
	// this plan's fresh names. Both are pointer-free sets that read no name
	// bytes and hold no slot, so renumber moves no entry.
	names namefp.Table
	// stages is the seal's pointer-free work list; stageMetrics[i] is the
	// metric of stages[i], the only pointer per stage.
	stages       []rollupStage
	stageMetrics []*rollupMetric
	pendingNames namefp.Table
	// sweepKeys/sweepSpare are the seal's sorted batch keys and radix spare,
	// seen the idle pass's bitset of staged IDs: pointer-free, so retaining
	// them between seals adds nothing for the GC to scan.
	sweepKeys  []uint64
	sweepSpare []uint64
	seen       []uint64
	samples    []sealrollup.Sample
	batches    []sealrollup.Batch
	files      map[string][][]plannedKV
	arena      []byte
	scratch    *rollupScratch // held from planParts through Commit/Abort
	pending    bool
	coverage   map[string]*rollupCoverage

	// Owner-local slots, in rank order. dense holds every ranked installed
	// metric in canonical (group, vshard, length, name) order, so a seal
	// sorts stages by uint32 rank instead of name bytes; unranked holds those
	// installed since the last renumber, in arrival order. Slot s < len(dense)
	// is dense[s], rank s+1; slot len(dense)+k is unranked[k]. ids[s] is the
	// slot's NumericID (deadSlot once adoption replaced its metric) and
	// meta[s] its committed scalars. install leaves the slot as a hint on the
	// metric's identity; renumber moves ids and meta with their metric and
	// re-stores the hints. ids and meta are pointer-free and sized by this
	// owner's names, not by the engine-wide ID space.
	dense    []*rollupMetric
	unranked []*rollupMetric
	ids      []uint64
	meta     []metricMetadata
	// dictDirty reports a dictionary append no successful seal Sync covers
	// yet; generation is this plan's dictionary generation (0: name keys).
	dictDirty  bool
	generation uint64
}

// rankSlack bounds how many installed-but-unranked metrics a planner carries
// relative to its dense slice before one sequential renumber. Unranked stages
// stay exact (binary search plus name tie-break), so this only trades the
// renumber pass (one touch per name) against per-stage search cost.
const rankSlack = 64

// Archive max-u32 is reserved within the core's GCP3 rollup experiment for the
// per-metric policy/frontier/activity envelope. Other archives contain canonical
// EncodeRecord values; BlockStart is the physical slot, Partition is expiry day.
// UUID precedes the metric, so each sealed version has a disjoint immutable key.
const rollupMetadataArchive = uint32(math.MaxUint32)

func newCoreRollups() *coreRollups {
	return &coreRollups{identities: &PolicyCatalog{}, metrics: map[uint64]*rollupMetric{}, expiries: calendar{slots: map[int64][]uint32{}}}
}

// install is the only writer of metrics and names. It indexes every installed
// name and files its inactivity expiry, when enabled. meta becomes id's scalars.
func (r *coreRollups) install(id uint64, m *rollupMetric, meta metricMetadata) {
	old := r.metrics[id]
	r.metrics[id] = m
	s := uint32(len(r.ids))
	if old != nil {
		s = r.slotOf(id, old)
	}
	switch {
	case old != nil && int(s) >= len(r.dense):
		r.unranked[int(s)-len(r.dense)] = m // an unranked old hands m its slot
	case old == m:
	default:
		if old != nil {
			// The next renumber drops it; ttl 0 keeps it off the expiry calendar.
			r.ids[s], r.ttl[s], s = deadSlot, 0, uint32(len(r.ids))
		}
		if id == deadSlot {
			panic("v2core: rollup NumericID collides with deadSlot")
		}
		r.ids, r.meta, r.unranked = append(r.ids, id), append(r.meta, metricMetadata{}), append(r.unranked, m)
		r.ttl, r.expAt = append(r.ttl, 0), append(r.expAt, 0)
	}
	r.meta[s] = meta
	m.ownerSlot.Store(s + 1)
	if old != m {
		r.names.Put(m.fingerprint(), 0) // a set: the slot is m's ownerSlot
	}
	if r.expire != nil {
		r.resolveTTL(s, m, r.clock())
		r.fileExpiry(s)
	}
}

// setExpiryTTL turns on the expiry calendar, filing every installed name.
func (r *coreRollups) setExpiryTTL(f func(RollupPolicy, time.Time) (time.Duration, time.Time)) {
	r.expiryTTL = f
	now := r.clock()
	for slot, id := range r.ids {
		if id != deadSlot {
			r.resolveTTL(uint32(slot), r.slotMetric(uint32(slot)), now)
			r.fileExpiry(uint32(slot))
		}
	}
}

func (r *coreRollups) clock() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

// resolveTTL caches slot's expiry window and tracks when any answer expires.
func (r *coreRollups) resolveTTL(slot uint32, m *rollupMetric, now time.Time) {
	ttl, until := r.expiryTTL(m.rollupPolicy(), now)
	r.ttl[slot] = int64(ttl)
	if !until.IsZero() && (r.ttlUntil == 0 || until.UnixNano() < r.ttlUntil) {
		r.ttlUntil = until.UnixNano()
	}
}

// reresolve re-resolves every name's window once now reaches the earliest
// until a resolution returned (a rule stopped applying).
func (r *coreRollups) reresolve(now time.Time) {
	if r.ttlUntil == 0 || now.UnixNano() < r.ttlUntil {
		return
	}
	r.ttlUntil = 0
	for slot, id := range r.ids {
		if id != deadSlot {
			r.resolveTTL(uint32(slot), r.slotMetric(uint32(slot)), now)
			r.fileExpiry(uint32(slot))
		}
	}
}

// expiryOverdue reports whether slot is idle for its cached window at ns
// (now - LastUpdate >= ttl): the only names expire is asked about.
func (r *coreRollups) expiryOverdue(slot uint32, ns int64) bool {
	meta := r.meta[slot]
	return r.ttl[slot] > 0 && meta.LastUpdate != 0 && !meta.Expired && ns-meta.LastUpdate >= r.ttl[slot]
}

// fileExpiry files slot under its deadline second unless a live entry at or
// before it exists already.
func (r *coreRollups) fileExpiry(slot uint32) {
	meta, ttl := r.meta[slot], r.ttl[slot]
	if ttl <= 0 || meta.LastUpdate == 0 || meta.Expired {
		return
	}
	key := (meta.LastUpdate + ttl) / int64(time.Second)
	if at := r.expAt[slot]; at != 0 && at <= key {
		return
	}
	r.expAt[slot] = key
	r.expiries.file(key, slot)
}

// expiryDue reports whether expire fires for a live entry due by now.
func (r *coreRollups) expiryDue(now time.Time) bool {
	r.reresolve(now)
	ns := now.UnixNano()
	for _, key := range r.expiries.due(now.Unix(), 0, nil) {
		for _, slot := range r.expiries.slots[key] {
			if r.expAt[slot] == key && r.expiryOverdue(slot, ns) {
				if m := r.slotMetric(slot); r.expire(m.rollupPolicy(), m.metadata(r.meta[slot]), now) {
					return true
				}
			}
		}
	}
	return false
}

// commitExpiries drops the drained seconds and refiles their live slots at
// their current deadlines: later for written names, the same overdue second
// for names expire refused, none for expired names.
func (r *coreRollups) commitExpiries() {
	refile := r.expRefile[:0]
	for _, key := range r.expDrained {
		for _, slot := range r.expiries.slots[key] {
			if r.expAt[slot] == key {
				r.expAt[slot] = 0
				refile = append(refile, slot)
			}
		}
	}
	r.expiries.drop(r.planNow)
	for _, slot := range refile {
		r.fileExpiry(slot)
	}
	r.expRefile = refile
}

// slotMetric is the metric installed in slot: dense below len(dense),
// unranked above.
func (r *coreRollups) slotMetric(slot uint32) *rollupMetric {
	if d := uint32(len(r.dense)); slot < d {
		return r.dense[slot]
	}
	return r.unranked[slot-uint32(len(r.dense))]
}

// expirySlots appends, once each, the expiry-due slots filed under the drained
// seconds, except those whose ID skip reports (already staged by batches).
func (r *coreRollups) expirySlots(skip func(uint64) bool, dst []uint32) []uint32 {
	words := (len(r.ids) + 63) / 64
	taken := slices.Grow(r.taken[:0], words)[:words]
	first := len(dst)
	expiry := r.expire != nil
	if expiry {
		ns := r.planClock.UnixNano()
		for _, key := range r.expDrained {
			for _, slot := range r.expiries.slots[key] {
				if r.expAt[slot] != key || taken[slot>>6]&(1<<(slot&63)) != 0 || skip(r.ids[slot]) || !r.expiryOverdue(slot, ns) {
					continue
				}
				if m := r.slotMetric(slot); r.expire(m.rollupPolicy(), m.metadata(r.meta[slot]), r.planClock) {
					taken[slot>>6] |= 1 << (slot & 63)
					dst = append(dst, slot)
				}
			}
		}
	}
	for _, slot := range dst[first:] {
		taken[slot>>6] = 0
	}
	r.taken = taken
	return dst
}

// calendar files slots under a second. heap holds each filed second once,
// earliest first, so a pass visits only the seconds at or before now.
type calendar struct {
	slots map[int64][]uint32
	heap  seconds
}

type seconds []int64

func (h seconds) Len() int           { return len(h) }
func (h seconds) Less(i, j int) bool { return h[i] < h[j] }
func (h seconds) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *seconds) Push(x any)        { *h = append(*h, x.(int64)) }
func (h *seconds) Pop() any          { x := (*h)[len(*h)-1]; *h = (*h)[:len(*h)-1]; return x }

func (c *calendar) file(sec int64, slot uint32) {
	filed, ok := c.slots[sec]
	if !ok {
		heap.Push(&c.heap, sec)
	}
	c.slots[sec] = append(filed, slot)
}

// due appends the filed seconds at or before now from the heap subtree at i.
// Below a later second the heap holds only later ones, so none is visited.
func (c *calendar) due(now int64, i int, dst []int64) []int64 {
	if i < len(c.heap) && c.heap[i] <= now {
		dst = c.due(now, 2*i+2, c.due(now, 2*i+1, append(dst, c.heap[i])))
	}
	return dst
}

// drop deletes every filed second at or before now.
func (c *calendar) drop(now int64) {
	for len(c.heap) > 0 && c.heap[0] <= now {
		delete(c.slots, heap.Pop(&c.heap).(int64))
	}
}

// needsMetadataSeal reports whether a not-yet-installed metric must seal before
// the next idle deadline: expiry needs its inactivity window anchored, and a
// coarse archive needs metadata before it can close. A single-archive name
// without expiry has neither, so it waits for the ordinary seal.
func (r *coreRollups) needsMetadataSeal(b *coreBatch) bool {
	if r.expire != nil || b.Registered == nil {
		return true
	}
	return len(b.Registered.schema().Archives) > 1
}

// pendingMetadataDue reports whether a pending name that is not installed yet
// needs a metadata seal. It visits only positions appended since its last
// clear pass, and resolves catalog handles only for names not installed.
func (r *coreRollups) pendingMetadataDue(h *pendingHead, lookup func(uint64) *ApplicationPolicy) bool {
	due := false
	h.rawView(lookup).IDs(h.scanned, h.n, func(_ int, id uint64, cold *coreBatch) bool {
		if r.metrics[id] != nil {
			return true
		}
		b := coreBatch{ID: id}
		if cold != nil {
			b.Registered, b.Policy = cold.Registered, cold.Policy
		} else {
			b.Registered = lookup(id)
		}
		due = r.needsMetadataSeal(&b)
		return !due
	})
	if !due {
		h.scanned = h.n
	}
	return due
}

// noteNewNames extends the head's new-name list over positions appended since
// its last pass: each row ID that is not installed, once, with its name hash.
// Installed names have sealed metadata in every snapshot published after their
// install, which readers resolve by name. Only these IDs touch the catalog.
// The caller excludes a concurrent Commit (the owner loop, or sealPubMu). A
// nil r (no rollups) has nothing installed.
func (r *coreRollups) noteNewNames(h *pendingHead, lookup func(uint64) *ApplicationPolicy) {
	h.rawView(lookup).IDs(h.freshScanned, h.n, func(_ int, id uint64, cold *coreBatch) bool {
		if cold != nil || (r != nil && r.metrics[id] != nil) {
			return true
		}
		if _, ok := h.freshIDs[id]; ok {
			return true
		}
		policy := lookup(id)
		if policy == nil {
			return true // a plain row without rollups has no name to request
		}
		if h.freshIDs == nil {
			h.freshIDs = map[uint64]struct{}{}
		}
		h.freshIDs[id] = struct{}{}
		h.fresh = append(h.fresh, pendingFresh{hash: policy.NameHash(), id: id})
		return true
	})
	h.freshScanned = h.n
}

func (r *coreRollups) metric(id uint64, policy []byte) (*rollupMetric, error) {
	h, err := r.identities.intern(id, "", policy)
	if err != nil {
		return nil, err
	}
	return newRollupMetric(h), nil
}

func newRollupMetric(h *PolicyIdentity) *rollupMetric {
	return &rollupMetric{PolicyIdentity: h, vshard: h.vshard}
}

// freshMetadata is the committed state of a name no seal has installed yet.
var freshMetadata = compactMetadata(RollupMetadata{Version: 1})

// committed returns the scalars s starts from: its slot's, or fresh ones.
func (r *coreRollups) committed(s *rollupStage) metricMetadata {
	if s.fresh {
		return freshMetadata
	}
	return r.meta[s.slot]
}

// rollupStage is one name's seal work. It is pointer-free, so the retained
// stage buffer adds nothing for the GC to scan: the metric is
// stageMetrics[i], the batches are batches[batch:batch+batches] of the
// planner's batch storage, and the records are records[record:] of the
// planning lane's record arena (see rollupPlannerLane.stageRecords). next
// holds the compact scalars; the identity supplies ID, policy digest and
// definition when encoding. slot is the owner slot of an installed metric,
// unset when fresh.
type rollupStage struct {
	sortKey         uint64
	next            metricMetadata
	batch, batches  uint32
	record, records uint32
	// versions superseded slot versions follow the records in the arena.
	versions uint32
	slot     uint32
	vshard   uint16
	fresh    bool
}

type rollupPlannerLane struct {
	ranked    []rollupRankRef
	spare     []rollupRankRef
	counts    []int
	output    rollupOutput
	workspace sealrollup.Workspace
	// records is the current chunk of the lane's record arena; tuples point
	// into it, so a chunk never moves during a plan. A plan that does not fit
	// starts a chunk twice as large; full chunks wait in chunks, base is the
	// arena index of records[0], and release sizes one chunk for the total.
	records []sealrollup.Record
	chunks  [][]sealrollup.Record
	bases   []uint32
	base    uint32
	err     error
}

// stageRecords returns the slot winners this lane planned for s. Its capacity
// extends over the stage's superseded versions, which only part files hold.
func (l *rollupPlannerLane) stageRecords(s *rollupStage) []sealrollup.Record {
	i, c := s.record, l.records
	if i < l.base {
		k := sort.Search(len(l.bases), func(k int) bool { return l.bases[k] > i }) - 1
		i, c = i-l.bases[k], l.chunks[k]
	} else {
		i -= l.base
	}
	return c[i : i+s.records : i+s.records+s.versions]
}

func (r *coreRollups) planParts(in coreSealInput) (coreSealPlan, error) {
	if r.pending {
		return coreSealPlan{}, errors.New("rollup plan still owns seal arenas")
	}
	r.takeScratch()
	r.pending = true
	returned := false
	defer func() {
		if !returned {
			r.release()
		}
	}()
	view := in.Pending
	if view.Len() == 0 {
		view = PendingBatches(in.Batches)
	}
	dict := r.identities.partDict()
	if uint64(view.Len()) > math.MaxUint32 {
		return coreSealPlan{}, errors.New("too many seal batches")
	}
	stages, metrics := r.stages[:0], r.stageMetrics[:0]

	if len(r.unranked) > 0 && len(r.unranked)*rankSlack >= len(r.dense) {
		r.renumber()
	}
	// One stable sort of the frozen view's applications by ID makes each
	// name's applications one run in arrival order; the sweep stages a name
	// once per run and reads its points in place. The keys and the idle
	// bitset are pointer-free scratch retained between seals.
	keys, spare, maxID, wide, sampleCount := view.sealKeys(r.sweepKeys, r.sweepSpare)
	r.sweepKeys, r.sweepSpare = keys, spare
	keyID := func(k uint64) uint64 {
		if wide {
			return view.idAt(int(k & sealKeyPosMask))
		}
		return k >> 32
	}
	runs := 0
	for i := range keys {
		if i == 0 || keyID(keys[i]) != keyID(keys[i-1]) {
			runs++
		}
	}
	stages, metrics = slices.Grow(stages, runs), slices.Grow(metrics, runs)
	r.stages, r.stageMetrics = stages, metrics
	idle := r.expire != nil || r.db != nil
	seen := r.seen[:0]
	if words := sealSeenWords(maxID, len(keys)); idle && words != 0 {
		seen = slices.Grow(seen, words)[:words]
	}
	r.seen = seen
	// Samples and application descriptors live in retained planner storage.
	// Every original boundary/order/clock remains distinct.
	r.samples = sealScratch(r.samples, sampleCount)[:sampleCount]
	samples := r.samples
	r.batches = sealScratch(r.batches, len(keys))[:len(keys)]
	batchStorage := r.batches
	offset, batch := 0, 0
	for i := 0; i < len(keys); {
		id := keyID(keys[i])
		j := i + 1
		for j < len(keys) && keyID(keys[j]) == id {
			j++
		}
		first := view.app(int(keys[i] & sealKeyPosMask))
		// Row applications carry no handle: the catalog resolves it once per
		// name. Cold applications keep their own.
		var registered *ApplicationPolicy
		if view.lookup != nil {
			registered = view.lookup(id)
		}
		firstRegistered, firstPolicy := registered, []byte(nil)
		if first.cold != nil {
			firstRegistered, firstPolicy = first.cold.Registered, first.cold.Policy
		}
		// A ranked name whose identity carries this planner's slot reaches
		// its metric through dense. Fresh and unranked names, policy-bytes
		// batches and hints another planner overwrote probe the metrics map.
		var m *rollupMetric
		slot, hinted := r.hintSlot(firstRegistered, id)
		rk := uint32(0)
		if hinted && int(slot) < len(r.dense) {
			m, rk = r.dense[slot], slot+1
		} else if m = r.metrics[id]; m != nil && !hinted {
			slot = r.slotOf(id, m)
		}
		fresh := m == nil
		meta := freshMetadata
		if !fresh {
			meta = r.meta[slot]
		}
		if fresh {
			if h := firstRegistered; h != nil && h.catalog == r.identities && h.id == id {
				m = newRollupMetric(h)
			} else {
				policy := firstPolicy
				if h != nil {
					policy = h.definitionBytes()
				}
				var err error
				m, err = r.metric(id, policy)
				if err != nil {
					return coreSealPlan{}, err
				}
			}
			// A fresh ID is not installed, and the sweep stages each ID once,
			// so any holder of this fingerprint is another ID for the name.
			fp := m.fingerprint()
			if _, ok := r.names.Get(fp); ok {
				return coreSealPlan{}, errors.New("canonical name has multiple producer IDs")
			}
			if _, ok := r.pendingNames.Get(fp); ok {
				return coreSealPlan{}, errors.New("canonical name has multiple pending IDs")
			}
			r.pendingNames.Put(fp, uint32(len(stages)))
		}
		stages = append(stages, rollupStage{next: meta, slot: slot, fresh: fresh, batch: uint32(batch), batches: uint32(j - i)})
		metrics = append(metrics, m)
		s := &stages[len(stages)-1]
		s.next.Expired = false
		if rk != 0 && dict == nil {
			s.vshard, s.sortKey = m.vshard, uint64(rk)<<1
		} else {
			r.stageKey(s, m, dict)
		}
		if len(seen) != 0 {
			seen[id>>6] |= 1 << (id & 63)
		}
		for k := i; k < j; k++ {
			a := first
			if k > i {
				a = view.app(int(keys[k] & sealKeyPosMask))
			}
			reg, policy := registered, []byte(nil)
			if a.cold != nil {
				reg, policy = a.cold.Registered, a.cold.Policy
			}
			if (reg == nil || reg != m.PolicyIdentity) && !m.definitionEqual(policy) {
				return coreSealPlan{}, errors.New("stored rollup policy changed")
			}
			n := a.points()
			ps := samples[offset : offset+n : offset+n]
			a.samples(ps)
			offset += n
			batchStorage[batch] = sealrollup.Batch{Order: a.Order, Now: a.Clock, Points: ps}
			batch++
		}
		i = j
	}
	named := len(stages)
	// The existing maintenance pass stages inactivity expiry.
	now := time.Now()
	if r.now != nil {
		now = r.now()
	}
	r.planNow = now.Unix()
	r.planClock = now
	r.reresolve(now) // returns at once without expiry
	r.expDrained = r.expiries.due(r.planNow, 0, r.expDrained[:0])
	if r.expire != nil {
		r.due = r.expirySlots(func(id uint64) bool { return sealKeySeen(keys, seen, maxID, id, keyID) }, r.due[:0])
		for _, slot := range r.due {
			m := r.slotMetric(slot)
			stages, metrics = append(stages, rollupStage{next: r.meta[slot], slot: slot}), append(metrics, m)
			r.stageKey(&stages[len(stages)-1], m, dict)
		}
	}
	r.stages, r.stageMetrics = stages, metrics
	if err := r.expireIdleStages(now); err != nil {
		return coreSealPlan{}, err
	}
	// Every ID this seal's parts carry is durable before a part is written.
	r.generation = 0
	if dict != nil && len(stages) > 0 {
		if err := r.dictKeys(dict); err != nil {
			return coreSealPlan{}, err
		}
		if r.dictDirty {
			if err := dict.Sync(); err != nil {
				return coreSealPlan{}, err
			}
			r.dictDirty = false
		}
		if r.generation = dict.Generation(); r.generation == 0 {
			return coreSealPlan{}, errors.New("ID-keyed seal without a durable dictionary generation")
		}
	}
	var part partsformat.PartID
	if len(stages) > 0 {
		if _, err := rand.Read(part[:]); err != nil {
			return coreSealPlan{}, err
		}
	}
	out, err := r.planStages(part)
	if err != nil {
		return coreSealPlan{}, err
	}
	out.Names = named
	out.Abort = r.release
	out.Commit = r.commitPlan
	returned = true
	return out, nil
}

// expireIdleStages applies the expiry callback to the overdue idle stages, in
// stage order: callbacks retain their serial owner contract. Idle plans do not
// advance order, so the persisted reset frontier is already known here.
func (r *coreRollups) expireIdleStages(now time.Time) error {
	if r.expire == nil {
		return nil
	}
	ns := now.UnixNano()
	for i := range r.stages {
		s, m := &r.stages[i], r.stageMetrics[i]
		if s.batches == 0 && r.expiryOverdue(s.slot, ns) && r.expire(m.rollupPolicy(), m.PolicyIdentity.metadata(s.next), now) {
			// Older readers must reject reset authority rather than ignore its
			// fields and resurrect the covered slot history.
			s.next.Version = max(2, s.next.Version)
			s.next.Expired = true
			s.next.ResetOrder = s.next.Frontier.Order
			if s.next.Revision == math.MaxUint64 {
				return errors.New("metric revision exhausted")
			}
			s.next.Revision++
		}
	}
	return nil
}

// commitPlan publishes the staged plan's owner-local state, drops the drained
// expiry seconds, then installs fresh names and refiles inactivity deadlines.
func (r *coreRollups) commitPlan() {
	defer r.release()
	r.commitStages()
	expiry := r.expire != nil
	if expiry {
		r.commitExpiries()
	}
	for i := range r.stages {
		s, m := &r.stages[i], r.stageMetrics[i]
		if s.fresh {
			r.install(m.id, m, s.next)
			continue
		}
		if expiry {
			r.fileExpiry(s.slot) // a revived name, or one drained this plan
		}
	}
}

// ScanRollups reads one immutable core snapshot. It resolves each physical slot
// by logical timestamp, then class and source-local order, before time filters.
// The snapshot must include metadata and all archive SSTs from one publication.
// Every coarse window is computed from the stored versions as reads compute
// it (windowRecords).
func ScanRollups(snapshot *pebble.Snapshot, visit func(RollupPolicy, RollupMetadata, []sealrollup.Record) error) error {
	return scanRollupsWith(snapshot, nil, true, visit)
}

// rollupScanEntry folds one name's rollup rows as recovery does: the newest
// metadata by after(), and per physical slot the newest record by newerOrigin.
type rollupScanEntry struct {
	group     partsformat.Group
	identity  *PolicyIdentity
	meta      metricMetadata
	records   map[[2]int64]sealrollup.Record
	single    [1]sealrollup.Record
	hasSingle bool
	versions  []sealrollup.Record // every stored version, for ScanRollups only
}

// projectWindows replaces every coarse archive's slots in records (archive
// and slot order) with their windows computed from every visible stored
// version, as reads compute them (windowRecords).
func (e *rollupScanEntry) projectWindows(c sealrollup.Config, meta metricMetadata, records []sealrollup.Record) ([]sealrollup.Record, error) {
	if meta.Frontier.Order == 0 {
		return records, nil
	}
	byKey := func(r sealrollup.Record, k [2]int64) int {
		return cmp.Or(cmp.Compare(int64(r.Archive), k[0]), cmp.Compare(r.Slot, k[1]))
	}
	versions := slices.Clone(e.versions)
	slices.SortStableFunc(versions, func(x, y sealrollup.Record) int { return byKey(x, [2]int64{int64(y.Archive), y.Slot}) })
	load := func(l int, first, last int64, _ uint64) ([]sealrollup.Record, error) {
		lo, _ := slices.BinarySearchFunc(versions, [2]int64{int64(l), first}, byKey)
		hi, _ := slices.BinarySearchFunc(versions, [2]int64{int64(l), last}, byKey)
		var out []sealrollup.Record
		for _, r := range versions[lo:hi] {
			if r.Order > meta.ResetOrder {
				out = append(out, r)
			}
		}
		return out, nil
	}
	out := make([]sealrollup.Record, 0, len(records))
	for a := range c.Archives {
		lo, _ := slices.BinarySearchFunc(records, [2]int64{int64(a), 0}, byKey)
		hi, _ := slices.BinarySearchFunc(records, [2]int64{int64(a + 1), 0}, byKey)
		if a == 0 {
			out = append(out, records[lo:hi]...)
			continue
		}
		if err := windowRecords(c, meta, a, 0, c.Archives[a].Count, records[lo:hi], load, func(r sealrollup.Record) error {
			out = append(out, r)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func scanIdentity(identities *PolicyCatalog, id uint64, name string, definition []byte) (*PolicyIdentity, error) {
	if identities != nil {
		return identities.intern(id, name, definition)
	}
	var policy RollupPolicy
	if err := json.Unmarshal(definition, &policy); err != nil {
		return nil, err
	}
	prepared, err := sealrollup.Prepare(policy.Config)
	if err != nil {
		return nil, err
	}
	return detachedPolicy(id, policy, definition, prepared), nil
}

func (e *rollupScanEntry) addMetadata(identities *PolicyCatalog, k partsformat.Key, value []byte, definition *[]byte) error {
	m, slim, err := decodeRollupMetadataHeader(value)
	if err == nil && slim {
		m.Policy, err = appendRollupDefinition((*definition)[:0], k.Metric, k.Group, m.Frontier.Policy)
		*definition = m.Policy
	}
	if identities == nil {
		m.Policy = bytes.Clone(m.Policy)
	}
	if err != nil {
		return err
	}
	if !compactMetadata(m).after(e.meta) {
		return nil
	}
	h, err := scanIdentity(identities, m.ID, string(k.Metric), m.Policy)
	if err != nil {
		return err
	}
	if h.policy.prepared.Digest() != m.Frontier.Policy {
		return errors.New("rollup policy/frontier mismatch")
	}
	e.identity, e.meta = h, compactMetadata(m)
	return nil
}

// addRecord keeps the newest record per physical slot. Legacy singleton rows
// need no temporary decoded slice.
func (e *rollupScanEntry) addRecord(record sealrollup.Record) error {
	slot := [2]int64{int64(record.Archive), record.Slot}
	if e.records == nil {
		if !e.hasSingle {
			e.single[0], e.hasSingle = record, true
			return nil
		}
		old := e.single[0]
		if old.Archive == record.Archive && old.Slot == record.Slot {
			newer, err := newerOrigin(partOrigin{Class: LivePart}, record, partOrigin{Class: LivePart}, old)
			if err != nil {
				return err
			}
			if newer {
				e.single[0] = record
			}
			return nil
		}
		e.records = map[[2]int64]sealrollup.Record{{int64(old.Archive), old.Slot}: old}
	}
	old, ok := e.records[slot]
	newer := !ok
	if ok {
		var err error
		newer, err = newerOrigin(partOrigin{Class: LivePart}, record, partOrigin{Class: LivePart}, old)
		if err != nil {
			return err
		}
	}
	if newer {
		e.records[slot] = record
	}
	return nil
}

// foldRollupKeys folds every rollup row of iter into series.
func foldRollupKeys(iter *archiveIter, identities *PolicyCatalog, series map[partsformat.ID]*rollupScanEntry, versions bool) error {
	// Reuse key decoding and temporary slim definitions while folding rows.
	// Catalog identities own their name and suffix before the next row.
	var keys partsformat.KeyDecoder
	names := keyNames{dict: identities.partDict()}
	var definition []byte
	var scratch []sealrollup.Record
	for iter.First(); iter.Valid(); iter.Next() {
		k, err := keys.Decode(iter.Key())
		if err == nil {
			err = names.resolve(&k)
		}
		if err != nil {
			return err
		}
		e := series[k.Metric]
		if e == nil {
			e = &rollupScanEntry{group: k.Group}
			series[k.Metric] = e
		}
		if k.Ordinal != 0 || e.group != k.Group {
			return errors.New("unexpected rollup partition")
		}
		if k.Archive == rollupMetadataArchive {
			err = e.addMetadata(identities, k, iter.Value(), &definition)
			if err != nil {
				return err
			}
			continue
		}
		value := iter.Value()
		if versions {
			all, err := partsformat.DecodeRecordsInto(nil, k.Archive, k.BlockStart, value)
			if err != nil {
				return err
			}
			e.versions = append(e.versions, all...)
		}
		decoded, err := partsformat.DecodeSlotsInto(scratch, k.Archive, k.BlockStart, value)
		if err != nil {
			return err
		}
		scratch = decoded[:0]
		for _, record := range decoded {
			if err = e.addRecord(record); err != nil {
				return err
			}
		}
	}
	return iter.Error()
}

// finish validates the folded name and returns its live records in archive
// and slot order.
func (e *rollupScanEntry) finish(name partsformat.ID) (RollupPolicy, RollupMetadata, []sealrollup.Record, error) {
	if e.identity == nil {
		return RollupPolicy{}, RollupMetadata{}, nil, fmt.Errorf("missing rollup metadata for %s", name)
	}
	policy, meta := e.identity.rollupPolicy(), e.identity.metadata(e.meta)
	if (meta.Version != 1 && meta.Version != 2 && meta.Version != 3) || policy.Group != e.group || policy.Name != name || meta.ID == 0 || meta.Frontier.Order == 0 {
		return policy, meta, nil, fmt.Errorf("missing rollup metadata for %s", name)
	}
	if meta.BaselineOrder > meta.Frontier.Order {
		return policy, meta, nil, errors.New("baseline exceeds rollup frontier")
	}
	if (meta.Version == 1 && (meta.ResetOrder != 0 || meta.Expired)) || meta.ResetOrder > meta.Frontier.Order || (meta.Expired && meta.ResetOrder != meta.Frontier.Order) {
		return policy, meta, nil, errors.New("invalid rollup reset frontier")
	}
	var records []sealrollup.Record
	if e.records == nil {
		records = e.single[:0]
		if e.hasSingle {
			r := e.single[0]
			if r.Archive >= len(policy.Config.Archives) || r.Slot >= policy.Config.Archives[r.Archive].Count || r.Order > meta.Frontier.Order {
				return policy, meta, nil, errors.New("invalid rollup slot/frontier")
			}
			if r.Order > meta.ResetOrder {
				records = append(records, r)
			}
		}
	} else {
		records = make([]sealrollup.Record, 0, len(e.records))
	}
	for slot, record := range e.records {
		if slot[0] >= int64(len(policy.Config.Archives)) || slot[1] >= policy.Config.Archives[slot[0]].Count || record.Order > meta.Frontier.Order {
			return policy, meta, nil, errors.New("invalid rollup slot/frontier")
		}
		if record.Order > meta.ResetOrder {
			records = append(records, record)
		}
	}
	slices.SortFunc(records, func(a, b sealrollup.Record) int {
		if a.Archive != b.Archive {
			return a.Archive - b.Archive
		}
		if a.Slot < b.Slot {
			return -1
		}
		if a.Slot > b.Slot {
			return 1
		}
		return 0
	})
	return policy, meta, records, nil
}

func scanRollups(snapshot *pebble.Snapshot, identities *PolicyCatalog, visit func(RollupPolicy, RollupMetadata, []sealrollup.Record) error) error {
	return scanRollupsWith(snapshot, identities, false, visit)
}

// scanRollupsWith folds every name; open also projects open windows from
// every stored version (no part of a live store is newer than the scan).
func scanRollupsWith(snapshot *pebble.Snapshot, identities *PolicyCatalog, open bool, visit func(RollupPolicy, RollupMetadata, []sealrollup.Record) error) error {
	if copied, err := snapshotHasCopiedParts(snapshot); err != nil {
		return err
	} else if copied {
		return scanCopiedRollups(snapshot, identities, visit)
	}
	series := map[partsformat.ID]*rollupScanEntry{}
	iter, err := newArchiveIter(context.Background(), snapshot, rollupScanBounds())
	if err != nil {
		return err
	}
	defer iter.Close()
	if err = foldRollupKeys(iter, identities, series, open); err != nil {
		return err
	}
	for name, e := range series {
		policy, meta, records, err := e.finish(name)
		if err != nil {
			return err
		}
		if open && meta.Version == 3 && !meta.Expired {
			if records, err = e.projectWindows(policy.Config, compactMetadata(meta), records); err != nil {
				return err
			}
		}
		if err = visit(policy, meta, records); err != nil {
			return err
		}
	}
	return nil
}

func rollupScanBounds() *pebble.IterOptions {
	// GCI2 keys sort just below GCP3 and nothing lies between.
	return &pebble.IterOptions{LowerBound: idKeyMagic, UpperBound: partsformat.PrefixEnd(partKeyMagic)}
}
func scanCopiedRollups(snapshot *pebble.Snapshot, identities *PolicyCatalog, visit func(RollupPolicy, RollupMetadata, []sealrollup.Record) error) error {
	index, err := NewRollupSnapshot(context.Background(), snapshot, nil, identities, nil, 0)
	if err != nil {
		return err
	}
	for name, metric := range index.metrics {
		records, err := index.Records(context.Background(), name)
		if err != nil {
			return err
		}
		slices.SortFunc(records, func(a, b sealrollup.Record) int {
			if a.Archive != b.Archive {
				return a.Archive - b.Archive
			}
			if a.Slot < b.Slot {
				return -1
			}
			if a.Slot > b.Slot {
				return 1
			}
			return 0
		})
		if err = visit(metric.latest.rollupPolicy(), metric.latest.metadata(), records); err != nil {
			return err
		}
	}
	return nil
}

// recoverCoreRollups rebuilds identity and scalars from durable part envelopes.
// Samples remain in parts; unsealed applications are replayed from L0. The bool
// reports copied bindings already checked during recovery, for the merge fence.
func recoverCoreRollups(db *pebble.DB, catalogs ...*PolicyCatalog) (*coreRollups, bool, error) {
	r := newCoreRollups()
	r.db = db
	if len(catalogs) > 0 {
		r.identities = catalogs[0]
	}
	snapshot := db.NewSnapshot()
	defer snapshot.Close()
	copied, err := snapshotHasCopiedParts(snapshot)
	if err != nil {
		return nil, false, err
	}
	if copied {
		index, err := NewRollupSnapshot(context.Background(), snapshot, nil, r.identities, nil, 0)
		if err != nil {
			return nil, false, err
		}
		for _, m := range index.metrics {
			if index.moves[m] != nil && index.moves[m].copied && index.moves[m].authority == nil {
				return nil, false, errors.New("copied-part catalog requires live reducer and numeric identity adoption before writable open")
			}
		}
	}
	if copied {
		err = scanCopiedRollups(snapshot, r.identities, r.installRecovered)
		return r, true, err
	}
	iter, err := newArchiveIter(context.Background(), snapshot, rollupScanBounds())
	if err != nil {
		return nil, false, err
	}
	defer iter.Close()
	var keys partsformat.KeyDecoder
	names := keyNames{dict: r.identities.partDict()}
	var definition []byte
	for iter.First(); iter.Valid(); iter.Next() {
		k, err := keys.Decode(iter.Key())
		if err != nil {
			return nil, false, err
		}
		if k.Archive != rollupMetadataArchive {
			continue
		}
		if err = names.resolve(&k); err != nil {
			return nil, false, err
		}
		entry := rollupScanEntry{group: k.Group}
		if err = entry.addMetadata(r.identities, k, iter.Value(), &definition); err != nil {
			return nil, false, err
		}
		_, meta, _, err := entry.finish(k.Metric)
		if err != nil || k.Ordinal != 0 {
			return nil, false, errors.Join(err, errors.New("invalid recovery envelope"))
		}
		m := r.metrics[meta.ID]
		if m == nil {
			m = newRollupMetric(entry.identity)
		} else if !entry.meta.after(r.meta[r.slotOf(meta.ID, m)]) {
			continue
		}
		if k.ByID {
			m.dictID.Store(k.Local + 1)
		}
		r.install(meta.ID, m, entry.meta)
	}
	err = iter.Error()
	return r, copied, err
}

// installRecovered installs one name rebuilt from parts.
func (r *coreRollups) installRecovered(policy RollupPolicy, meta RollupMetadata, records []sealrollup.Record) error {
	m, err := r.metric(meta.ID, meta.Policy)
	if err != nil {
		return err
	}
	if r.metrics[meta.ID] != nil {
		return errors.New("duplicate recovered rollup ID")
	}
	r.install(meta.ID, m, compactMetadata(meta))
	return nil
}

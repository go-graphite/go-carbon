package v2core

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"
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
	// BaselineOrder certifies the classic baseline part sequence installed before intake.
	// The field name and wire position are retained for legacy metadata/checkpoints.
	BaselineOrder uint64 `json:",omitempty"`

	// ResetOrder is a part sequence in Version 4, masking parts through the expiry
	// seal, including copied history. Versions 1-3 hold a legacy input order.
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

	rankMu    *sync.Mutex // existing owner publication lock; legacy renumber only
	stage     func(string)
	taken     []uint64
	due       []uint32
	planNow   int64
	planClock time.Time
	// names maps installed fingerprints to owner slots without reading name
	// bytes. pendingNames holds this plan's fresh fingerprints and stages.
	names namefp.Table
	// stages is the seal's pointer-free work list; stageMetrics[i] is the
	// metric of stages[i], the only pointer per stage.
	stages       []rollupStage
	stageMetrics []*PolicyIdentity
	pendingNames namefp.Table
	// sweepKeys/sweepSpare are the seal's sorted batch keys and radix spare,
	// seen the expiry pass's bitset of staged IDs. The seal borrows these
	// and pendingNames with the rest of its scratch until Commit/Abort.
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

	// Owner-local slots. Dictionary-backed owners keep all metrics in
	// unranked arrival slots and replace them in place, so no dead slot is
	// created and no lexical compaction is needed. Name-keyed owners use
	// rank order: dense holds every ranked installed
	// metric in canonical (group, vshard, length, name) order, so a seal
	// sorts stages by uint32 rank instead of name bytes; unranked holds those
	// installed since the last renumber, in arrival order. Slot s < len(dense)
	// is dense[s], rank s+1; slot len(dense)+k is unranked[k]. ids[s] is the
	// slot's NumericID (deadSlot once adoption replaced its metric) and
	// meta[s] its committed scalars. install leaves the slot as a hint on the
	// metric's identity; renumber moves ids and meta with their metric and
	// re-stores the hints. ids and meta are pointer-free and sized by this
	// owner's names, not by the engine-wide ID space.
	dense    []*PolicyIdentity
	unranked []*PolicyIdentity
	ids      []uint64
	meta     []metricMetadata
	// generation is this plan's dictionary generation (0: name keys).
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
	return &coreRollups{identities: &PolicyCatalog{}, expiries: calendar{slots: map[int64][]uint32{}}}
}

// install indexes every installed name and files its inactivity expiry, when enabled. meta becomes id's scalars.
func (r *coreRollups) install(id uint64, m *PolicyIdentity, meta metricMetadata) {
	old := r.installedIdentity(id, m)
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
			r.ids[s] = deadSlot
			if r.ttl != nil {
				r.ttl[s] = 0
			}
			s = uint32(len(r.ids))
		}
		if id == deadSlot {
			panic("v2core: rollup NumericID collides with deadSlot")
		}
		r.ids, r.meta, r.unranked = append(r.ids, id), append(r.meta, metricMetadata{}), append(r.unranked, m)
	}
	r.meta[s] = meta
	m.ownerSlot.Store(s + 1)
	if old != m {
		r.names.Put(m.fingerprint(), s)
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
func (r *coreRollups) resolveTTL(slot uint32, m *PolicyIdentity, now time.Time) {
	// Recovery and expiry-off owners have no columns. Activation fills all
	// existing slots; subsequent installs extend them with the slot arrays.
	if n := len(r.ids) - len(r.ttl); n > 0 {
		r.ttl = append(r.ttl, make([]int64, n)...)
		r.expAt = append(r.expAt, make([]int64, n)...)
	}
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
func (r *coreRollups) slotMetric(slot uint32) *PolicyIdentity {
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

// pendingMetadataDue shares row classification with publication. Only fresh
// names can need a first metadata seal; installation is monotonic even across
// expiry and renumber. A background commit may have installed a listed name.
func (r *coreRollups) pendingMetadataDue(h *pendingHead, lookup func(uint64) *ApplicationPolicy) bool {
	r.noteNewNames(h, lookup)
	for _, f := range h.fresh {
		if r.installed(f.id) == nil && r.needsMetadataSeal(&coreBatch{ID: f.id, Registered: lookup(f.id)}) {
			return true
		}
	}
	// Rows without a catalog handle retain the original policy fallback.
	if h.unresolved {
		due := false
		h.rawView(lookup).IDs(h.scanned, h.n, func(_ int, id uint64, cold *coreBatch) bool {
			b := coreBatch{ID: id, Registered: lookup(id)}
			if cold != nil {
				b = *cold
			}
			due = r.installed(id) == nil && r.needsMetadataSeal(&b)
			return !due
		})
		if !due {
			h.scanned = h.n
		}
		return due
	}
	// Cold applications carry their policy inline and are absent from fresh.
	v := h.rawView(lookup)
	for i := range v.segs {
		s := &v.segs[i]
		for j := max(0, h.scanned-s.start); j < len(s.cold); j++ {
			b := &s.cold[j]
			if r.installed(b.ID) == nil && r.needsMetadataSeal(b) {
				return true
			}
		}
	}
	h.scanned = h.n
	return false
}

// noteNewNames classifies only newly appended positions. The existing sorted
// runs recognize IDs classified by earlier publications and keep first
// catalog reads in numeric order. Metadata maintenance shares this result. Names
// retain metadata through expiry; move adoption and renumber preserve membership.
// The caller excludes Commit (the owner loop, or sealPubMu).
func (r *coreRollups) noteNewNames(h *pendingHead, lookup func(uint64) *ApplicationPolicy) {
	if h.freshScanned == h.n {
		return
	}
	visit := func(id uint64) {
		if _, ok := h.freshIDs[id]; ok {
			return
		}
		if r != nil {
			m := r.identities.lookup(id)
			if _, ok := r.hintSlot(m, id); ok {
				return
			}
			if _, ok := r.nameSlot(id, m); ok {
				return
			}
		}
		policy := lookup(id)
		if policy == nil {
			h.unresolved = true
			return
		}
		if h.freshIDs == nil {
			h.freshIDs = map[uint64]struct{}{}
		}
		h.freshIDs[id] = struct{}{}
		h.fresh = append(h.fresh, pendingFresh{hash: policy.NameHash(), id: id})
	}
	v := h.view(lookup)
	// Cursors merge the new IDs against already classified runs. They are
	// iterator positions only; the existing runs remain the sole pending index.
	cursors := make([]int, len(v.runs))
	for ri, run := range v.runs {
		if run.hi <= h.freshScanned {
			continue
		}
		clear(cursors)
		var prev uint64
		have := false
		for _, key := range run.keys {
			pos, id := int(uint32(key)), key>>32
			if have && prev == id {
				continue
			}
			// Positions ascend within an ID. An earlier position was classified
			// already; installed membership cannot disappear during this head.
			if pos < max(h.freshScanned, run.lo) || pos >= run.hi {
				if !h.unresolved {
					prev, have = id, true
				}
				continue
			}
			prev, have = id, true
			known := false
			if !h.unresolved {
				for j := 0; j < ri; j++ {
					keys := v.runs[j].keys
					for cursors[j] < len(keys) && keys[cursors[j]]>>32 < id {
						cursors[j]++
					}
					if cursors[j] < len(keys) && keys[cursors[j]]>>32 == id {
						known = true
						break
					}
				}
			}
			if !known {
				visit(id)
			}
		}
	}
	// Wide IDs have no run key. Cold applications already carry their name.
	for i := range v.segs {
		s := &v.segs[i]
		for _, e := range s.escapes {
			if e.ID >= 1<<32 && s.start+int(e.Row-s.firstRow()) >= h.freshScanned {
				visit(e.ID)
			}
		}
	}
	h.freshScanned = h.n
}

func (r *coreRollups) metric(id uint64, policy []byte) (*PolicyIdentity, error) {
	h, err := r.identities.intern(id, "", policy)
	if err != nil {
		return nil, err
	}
	return h, nil
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

	if dict == nil && len(r.unranked) > 0 && len(r.unranked)*rankSlack >= len(r.dense) {
		r.renumber()
	}
	// One stable sort of the frozen view's applications by ID makes each
	// name's applications one run in arrival order; the sweep stages a name
	// once per run and reads its points in place. The keys and the expiry
	// bitset belong to this seal's borrowed scratch.
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
	seen := r.seen[:0]
	if words := sealSeenWords(maxID, len(keys)); r.expire != nil && words != 0 {
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
		// A valid identity hint reaches either kind of owner slot directly.
		// Missing or overwritten hints use the installed fingerprint table.
		var m *PolicyIdentity
		slot, hinted := r.hintSlot(firstRegistered, id)
		rk := uint32(0)
		if hinted {
			m = r.slotMetric(slot)
			if int(slot) < len(r.dense) {
				rk = slot + 1
			}
		} else if m = r.installed(id); m != nil && !hinted {
			slot = r.slotOf(id, m)
		}
		fresh := m == nil
		meta := freshMetadata
		if !fresh {
			meta = r.meta[slot]
		}
		if fresh {
			if h := firstRegistered; h != nil && h.catalog == r.identities && h.id == id {
				m = h
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
			if (reg == nil || reg != m) && !m.definitionEqual(policy) {
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
	if err := r.expireIdleStages(now, in.Next.NextPart); err != nil {
		return coreSealPlan{}, err
	}
	// Every ID this seal's parts carry is durable before a part is written.
	r.generation = 0
	if dict != nil && len(stages) > 0 {
		if err := r.dictKeys(dict); err != nil {
			return coreSealPlan{}, err
		}
		// Admission synced the names producers drained; this syncs the
		// names dictKeys appended and costs nothing when nothing is new.
		if err := dict.Sync(); err != nil {
			return coreSealPlan{}, err
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
// advance input order; their mask is the containing seal's part sequence.
func (r *coreRollups) expireIdleStages(now time.Time, seq uint64) error {
	if r.expire == nil {
		return nil
	}
	ns := now.UnixNano()
	for i := range r.stages {
		s, m := &r.stages[i], r.stageMetrics[i]
		if s.batches == 0 && r.expiryOverdue(s.slot, ns) && r.expire(m.rollupPolicy(), m.metadata(s.next), now) {
			// Older readers must reject reset authority rather than ignore its
			// fields and resurrect the covered slot history.
			s.next.Version = max(2, s.next.Version)
			s.next.Expired = true
			s.next.ResetOrder = s.next.Frontier.Order
			if seq != 0 {
				s.next.Version = 4
				s.next.ResetOrder = seq
			}
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
// by logical timestamp, class, part sequence and in-part order, before time filters.
// The snapshot must include metadata and all archive SSTs from one publication.
// Every coarse window is computed from the stored versions as reads compute
// it (windowRecords).
func ScanRollups(snapshot *pebble.Snapshot, visit func(RollupPolicy, RollupMetadata, []sealrollup.Record) error) error {
	return scanRollupsWith(snapshot, nil, true, visit)
}

// rollupScanEntry validates one recovered name's durable metadata envelope.
type rollupScanEntry struct {
	group    partsformat.Group
	identity *PolicyIdentity
	meta     metricMetadata
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

// finish validates the recovered identity and scalars without encoding another
// policy definition. addMetadata already interned the envelope's policy.
func (e *rollupScanEntry) finish(name partsformat.ID) (RollupMetadata, error) {
	if e.identity == nil {
		return RollupMetadata{}, fmt.Errorf("missing rollup metadata for %s", name)
	}
	meta := e.identity.metadataScalars(e.meta)
	if (meta.Version < 1 || meta.Version > 4) || e.identity.policy.Group != e.group || !e.identity.matchesName(string(name)) || meta.ID == 0 || meta.Frontier.Order == 0 {
		return meta, fmt.Errorf("missing rollup metadata for %s", name)
	}
	if meta.Version < 4 && meta.BaselineOrder > meta.Frontier.Order || (meta.Version == 1 && (meta.ResetOrder != 0 || meta.Expired)) || meta.Version < 4 && meta.ResetOrder > meta.Frontier.Order || (meta.Expired && meta.Version < 4 && meta.ResetOrder != meta.Frontier.Order) {
		return meta, errors.New("invalid rollup reset frontier")
	}
	return meta, nil
}

func scanRollups(snapshot *pebble.Snapshot, identities *PolicyCatalog, visit func(RollupPolicy, RollupMetadata, []sealrollup.Record) error) error {
	return scanRollupsWith(snapshot, identities, false, visit)
}

// scanRollupsWith folds every name; open also projects open windows from
// every stored version (no part of a live store is newer than the scan).
func scanRollupsWith(snapshot *pebble.Snapshot, identities *PolicyCatalog, open bool, visit func(RollupPolicy, RollupMetadata, []sealrollup.Record) error) error {
	index, err := NewRollupSnapshot(context.Background(), snapshot, nil, identities, nil, 0)
	if err != nil {
		return err
	}
	for name, metric := range index.metrics {
		var records []sealrollup.Record
		if open || index.hasCopied {
			records, err = index.Records(context.Background(), name)
		} else if !metric.latest.meta.Expired {
			for archive, a := range metric.latest.schema().Archives {
				err = index.scanStoredArchive(context.Background(), name, archive, 0, a.Count, func(r sealrollup.Record) error { records = append(records, r); return nil })
				if err != nil {
					break
				}
			}
		}
		if err != nil {
			return err
		}
		if err = visit(metric.latest.rollupPolicy(), metric.latest.metadata(), records); err != nil {
			return err
		}
	}
	return nil
}

func rollupScanBounds() *pebble.IterOptions {
	// GCI2 keys sort just below GCP3 and nothing lies between.
	return &pebble.IterOptions{LowerBound: idKeyMagic, UpperBound: partsformat.PrefixEnd(partKeyMagic)}
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
	copied, err := r.recover()
	return r, copied, err
}

// recover installs the sealed owner state before its first reader or mutation.
// The caller excludes all other accesses to the owner-local planner until done.
func (r *coreRollups) recover() (bool, error) {
	snapshot := r.db.NewSnapshot()
	defer snapshot.Close()
	copied, err := snapshotHasCopiedParts(snapshot)
	if err != nil {
		return false, err
	}
	if copied {
		index, err := NewRollupSnapshot(context.Background(), snapshot, nil, r.identities, nil, 0)
		if err != nil {
			return false, err
		}
		for _, m := range index.metrics {
			if index.moves[m] != nil && index.moves[m].copied && index.moves[m].authority == nil {
				return false, errors.New("copied-part catalog requires live reducer and numeric identity adoption before writable open")
			}
		}
	}
	if copied {
		err = scanRollups(snapshot, r.identities, r.installRecovered)
		return true, err
	}
	iter, err := newArchiveIter(context.Background(), snapshot, rollupScanBounds())
	if err != nil {
		return false, err
	}
	defer iter.Close()
	var keys partsformat.KeyDecoder
	names := keyNames{dict: r.identities.partDict()}
	var definition []byte
	for iter.First(); iter.Valid(); iter.Next() {
		k, err := keys.Decode(iter.Key())
		if err != nil {
			return false, err
		}
		if k.Archive != rollupMetadataArchive {
			continue
		}
		if err = names.resolve(&k); err != nil {
			return false, err
		}
		entry := rollupScanEntry{group: k.Group}
		if err = entry.addMetadata(r.identities, k, iter.Value(), &definition); err != nil {
			return false, err
		}
		meta, err := entry.finish(k.Metric)
		if err != nil || k.Ordinal != 0 {
			return false, errors.Join(err, errors.New("invalid recovery envelope"))
		}
		m := r.installed(meta.ID)
		if m == nil {
			m = entry.identity
		} else if !entry.meta.after(r.meta[r.slotOf(meta.ID, m)]) {
			continue
		}
		if k.ByID {
			m.rememberDictID(k.Local)
		}
		r.install(meta.ID, m, entry.meta)
	}
	err = iter.Error()
	return copied, err
}

// installRecovered installs one name rebuilt from parts.
func (r *coreRollups) installRecovered(policy RollupPolicy, meta RollupMetadata, records []sealrollup.Record) error {
	m, err := r.metric(meta.ID, meta.Policy)
	if err != nil {
		return err
	}
	if r.installed(meta.ID) != nil {
		return errors.New("duplicate recovered rollup ID")
	}
	r.install(meta.ID, m, compactMetadata(meta))
	return nil
}

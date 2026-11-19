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
	"time"

	"github.com/cockroachdb/pebble"
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
	ClosedThrough int64 `json:",omitempty"`
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

// rollupMetric holds what a name needs beyond its scalars: the shared
// identity and, for coarse archives, KeepRecent residency. The committed
// scalars live in coreRollups.meta, by owner slot.
type rollupMetric struct {
	*PolicyIdentity
	slots  sealrollup.Slots
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

	stage   func(string)
	metrics map[uint64]*rollupMetric
	// calendar files each installed slot under the end of every coarse window
	// it holds open (rollupOpenEnd). An idle pass drains only the ends at or
	// before now; single-archive names are never filed. Entries are hints:
	// a drained slot stages only if rollupClosureDue still holds. drained
	// lists the ends this plan drained (deleted at commit); taken is the
	// plan's bitset of slots staged from them, due those slots; planNow is the
	// plan's clock.
	calendar     calendar
	drained      []int64
	taken        []uint64
	due          []uint32
	planNow      int64
	planClock    time.Time
	names        map[partsformat.ID]uint64
	stages       []rollupStage
	pendingNames map[partsformat.ID]uint64
	// sweepKeys/sweepSpare are the seal's sorted batch keys and radix spare,
	// seen the idle pass's bitset of staged IDs: pointer-free, so retaining
	// them between seals adds nothing for the GC to scan.
	sweepKeys   []uint64
	sweepSpare  []uint64
	seen        []uint64
	samples     []sealrollup.Sample
	batches     []sealrollup.Batch
	files       map[string][][]plannedKV
	arena       []byte
	pending     bool
	definitions map[[32]byte][]partsformat.ArchivePolicy
	coverage    map[string]*rollupCoverage

	// dense holds every ranked installed metric in canonical order, so a seal
	// sorts stages by uint32 rank instead of name bytes. unranked metrics were
	// installed since the last renumber; replaced marks IDs whose metric
	// changed (adoption) so the next renumber drops the stale entry.
	dense      []*rollupMetric
	denseSpare []*rollupMetric
	unranked   []*rollupMetric
	replaced   bool
	// Owner-local slots. install gives each installed ID a slot, in arrival
	// order, and leaves it as a hint on the metric's identity. ids[s] is the
	// NumericID in slot s; rank[s] is 1 + its index in dense, in the canonical
	// (group, vshard, length, name) order, 0 when unranked. denseSlots and
	// unrankedSlots run parallel to dense and unranked, so renumber never
	// looks a slot up. All four are pointer-free and sized by this owner's
	// names, not by the engine-wide ID space. Invariant: rank[s] != 0 means
	// dense[rank[s]-1] is the metric installed under ids[s].
	ids             []uint64
	rank            []uint32
	meta            []metricMetadata // committed scalars, parallel to ids
	denseSlots      []uint32
	denseSlotsSpare []uint32
	unrankedSlots   []uint32
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
	return &coreRollups{identities: &PolicyCatalog{}, metrics: map[uint64]*rollupMetric{}, calendar: calendar{slots: map[int64][]uint32{}}, expiries: calendar{slots: map[int64][]uint32{}}, names: map[partsformat.ID]uint64{}}
}

// install is the only writer of metrics and files every open coarse window,
// so the calendar can never miss one. meta becomes id's committed scalars.
func (r *coreRollups) install(id uint64, m *rollupMetric, meta metricMetadata) {
	old := r.metrics[id]
	r.metrics[id] = m
	if old == m {
		r.meta[r.slotOf(id, m)] = meta
	} else {
		s, ok := uint32(0), false
		if old != nil {
			// The stale entry leaves dense at the next renumber; the ID
			// re-enters unranked in its old slot.
			r.replaced = true
			s, ok = r.retire(id, old)
		}
		if !ok {
			s = uint32(len(r.ids))
			r.ids = append(r.ids, id)
			r.rank = append(r.rank, 0)
			r.meta = append(r.meta, metricMetadata{})
			r.ttl = append(r.ttl, 0)
			r.expAt = append(r.expAt, 0)
		}
		r.meta[s] = meta
		m.ownerSlot.Store(s + 1)
		r.unranked = append(r.unranked, m)
		r.unrankedSlots = append(r.unrankedSlots, s)
	}
	slot := r.slotOf(id, m)
	r.file(slot, m, meta, math.MaxUint16)
	if r.expire != nil {
		r.resolveTTL(slot, m, r.clock())
		r.fileExpiry(slot)
	}
}

// setExpiryTTL turns on the expiry calendar, filing every installed name.
func (r *coreRollups) setExpiryTTL(f func(RollupPolicy, time.Time) (time.Duration, time.Time)) {
	r.expiryTTL = f
	now := r.clock()
	for slot := range r.ids {
		if m := r.slotMetric(uint32(slot)); m != nil {
			r.resolveTTL(uint32(slot), m, now)
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
	for slot := range r.ids {
		if m := r.slotMetric(uint32(slot)); m != nil {
			r.resolveTTL(uint32(slot), m, now)
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

// file enters slot under the open window end of each coarse archive whose
// bit (archive index - 1, the last bit also covering higher archives) is set.
func (r *coreRollups) file(slot uint32, m *rollupMetric, meta metricMetadata, mask uint16) {
	for i, a := range m.schema().Archives[1:] {
		if end := rollupOpenEnd(meta, a); mask&(1<<min(i, 15)) != 0 && end != math.MaxInt64 {
			r.calendar.file(end, slot)
		}
	}
}

// refile marks the coarse archives whose open window next files anew: a new
// window end, a reopened window, or an end this plan drained. A window end
// only moves forward per archive, so one entry per open window suffices.
func (r *coreRollups) refile(m *rollupMetric, old, next metricMetadata) uint16 {
	mask := uint16(0)
	for i, a := range m.schema().Archives[1:] {
		end, prior := rollupOpenEnd(next, a), rollupOpenEnd(old, a)
		if end != math.MaxInt64 && (end != prior || prior <= r.planNow) {
			mask |= 1 << min(i, 15)
		}
	}
	return mask
}

// slotMetric is the metric installed in slot.
func (r *coreRollups) slotMetric(slot uint32) *rollupMetric {
	if rk := r.rank[slot]; rk != 0 {
		return r.dense[rk-1]
	}
	return r.metrics[r.ids[slot]]
}

// dueSlots appends, once each, the closure-due slots filed under the drained
// ends, except those whose ID skip reports (already staged by batches).
func (r *coreRollups) dueSlots(skip func(uint64) bool, dst []uint32) []uint32 {
	words := (len(r.ids) + 63) / 64
	taken := slices.Grow(r.taken[:0], words)[:words]
	first := len(dst)
	expiry := r.expire != nil
	for _, end := range r.drained {
		for _, slot := range r.calendar.slots[end] {
			if taken[slot>>6]&(1<<(slot&63)) == 0 && !skip(r.ids[slot]) && (!expiry || !r.meta[slot].Expired) && rollupClosureDue(r.slotMetric(slot), r.meta[slot], r.planNow) {
				taken[slot>>6] |= 1 << (slot & 63)
				dst = append(dst, slot)
			}
		}
	}
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

// closureDue reports whether any calendar end at or before now holds a
// closure-due slot. Ends holding none are stale and dropped here.
func (r *coreRollups) closureDue(now int64) bool {
	for c := &r.calendar; len(c.heap) > 0 && c.heap[0] <= now; c.drop(c.heap[0]) {
		for _, slot := range c.slots[c.heap[0]] {
			if (r.expire == nil || !r.meta[slot].Expired) && rollupClosureDue(r.slotMetric(slot), r.meta[slot], now) {
				return true
			}
		}
	}
	return false
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
func (r *coreRollups) metric(id uint64, policy []byte) (*rollupMetric, error) {
	h, err := r.identities.intern(id, "", policy)
	if err != nil {
		return nil, err
	}
	return newRollupMetric(h), nil
}

func newRollupMetric(h *PolicyIdentity) *rollupMetric {
	return &rollupMetric{PolicyIdentity: h, slots: *h.policy.prepared.NewSlots(), vshard: h.vshard}
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

type rollupStage struct {
	sortKey    uint64
	vshard     uint16
	slot       uint32 // owner slot of an installed metric; unset when fresh
	metric     *rollupMetric
	next       RollupMetadata
	batches    []sealrollup.Batch
	records    []sealrollup.Record
	fresh      bool
	refile     uint16 // coarse archives to file at commit (coreRollups.refile)
	batchCount int
}

type rollupPlannerLane struct {
	windows   rollupWindowCache
	ranked    []rollupRankRef
	spare     []rollupRankRef
	counts    []int
	output    rollupOutput
	workspace sealrollup.Workspace
	records   []sealrollup.Record
	err       error
}

func (r *coreRollups) planParts(in coreSealInput) (coreSealPlan, error) {
	if r.pending {
		return coreSealPlan{}, errors.New("rollup plan still owns seal arenas")
	}
	r.pending = true
	returned := false
	defer func() {
		if !returned {
			r.release()
		}
	}()
	stages := r.stages[:0]
	if len(r.unranked) > 0 && len(r.unranked)*rankSlack >= len(r.dense) {
		r.renumber()
	}
	names := r.pendingNames
	if names == nil {
		names = map[partsformat.ID]uint64{}
		r.pendingNames = names
	}
	// One stable sort of batch indices by ID makes each name's batches one run
	// in arrival order; the sweep stages a name once per run. The keys and the
	// idle bitset are pointer-free scratch retained between seals.
	keys, spare, maxID := sortSealBatches(in.Batches, r.sweepKeys, r.sweepSpare)
	r.sweepKeys, r.sweepSpare = keys, spare
	runs := 0
	for i := range keys {
		if i == 0 || in.Batches[keys[i]&sealBatchIndexMask].ID != in.Batches[keys[i-1]&sealBatchIndexMask].ID {
			runs++
		}
	}
	stages = slices.Grow(stages, runs)
	r.stages = stages
	idle := r.expire != nil || r.db != nil
	seen := r.seen[:0]
	if words := sealSeenWords(maxID, len(keys)); idle && words != 0 {
		seen = slices.Grow(seen, words)[:words]
	}
	r.seen = seen
	// One allocation per sealed part for samples and application descriptors.
	// Every original boundary/order/clock remains distinct.
	sampleCount := 0
	for i := range in.Batches {
		sampleCount += len(in.Batches[i].Points)
	}
	r.samples = slices.Grow(r.samples[:0], sampleCount)[:sampleCount]
	samples := r.samples
	r.batches = slices.Grow(r.batches[:0], len(in.Batches))[:len(in.Batches)]
	batchStorage := r.batches
	offset := 0
	for i := 0; i < len(keys); {
		first := &in.Batches[keys[i]&sealBatchIndexMask]
		id := first.ID
		j := i + 1
		for j < len(keys) && in.Batches[keys[j]&sealBatchIndexMask].ID == id {
			j++
		}
		// A ranked name whose identity carries this planner's slot reaches
		// its metric through dense. Fresh and unranked names, policy-bytes
		// batches and hints another planner overwrote probe the metrics map.
		var m *rollupMetric
		slot, hinted := r.hintSlot(first.Registered, id)
		rk := uint32(0)
		if hinted {
			rk = r.rank[slot]
		}
		if rk != 0 {
			m = r.dense[rk-1]
		} else if m = r.metrics[id]; m != nil && !hinted {
			slot = r.slotOf(id, m)
		}
		fresh := m == nil
		meta := freshMetadata
		if !fresh {
			meta = r.meta[slot]
		}
		if fresh {
			if h := first.Registered; h != nil && h.catalog == r.identities && h.id == id {
				m = newRollupMetric(h)
			} else {
				policy := first.Policy
				if h != nil {
					policy = h.definitionBytes()
				}
				var err error
				m, err = r.metric(id, policy)
				if err != nil {
					return coreSealPlan{}, err
				}
			}
			if prior, ok := r.names[m.policy.Name]; ok && prior != id {
				return coreSealPlan{}, errors.New("canonical name has multiple producer IDs")
			}
			if prior, ok := names[m.policy.Name]; ok && prior != id {
				return coreSealPlan{}, errors.New("canonical name has multiple pending IDs")
			}
			names[m.policy.Name] = id
		}
		stages = append(stages, rollupStage{metric: m, slot: slot, next: m.metadataScalars(meta), fresh: fresh, batchCount: j - i})
		s := &stages[len(stages)-1]
		s.next.Expired = false
		if rk != 0 {
			s.vshard, s.sortKey = m.vshard, uint64(rk)<<1
		} else {
			r.stageKey(s)
		}
		if len(seen) != 0 {
			seen[id>>6] |= 1 << (id & 63)
		}
		s.batches = batchStorage[: 0 : j-i]
		batchStorage = batchStorage[j-i:]
		for ; i < j; i++ {
			b := &in.Batches[keys[i]&sealBatchIndexMask]
			if (b.Registered == nil || b.Registered != m.PolicyIdentity) && !m.definitionEqual(b.Policy) {
				return coreSealPlan{}, errors.New("stored rollup policy changed")
			}
			ps := samples[offset : offset+len(b.Points) : offset+len(b.Points)]
			for k, p := range b.Points {
				ps[k] = sealrollup.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Seq}
			}
			offset += len(b.Points)
			s.batches = append(s.batches, sealrollup.Batch{Order: b.Order, Now: b.Clock, Points: ps})
		}
	}
	named := len(stages)
	// Idle closure uses the existing seal maintenance pass; no separate scheduler.
	now := time.Now()
	if r.now != nil {
		now = r.now()
	}
	r.planNow = now.Unix()
	r.drained = r.calendar.due(r.planNow, 0, r.drained[:0])
	r.planClock = now
	r.reresolve(now) // returns at once without expiry
	r.expDrained = r.expiries.due(r.planNow, 0, r.expDrained[:0])
	if r.db != nil || r.expire != nil {
		r.due = r.dueSlots(func(id uint64) bool { return sealBatchSeen(in.Batches, keys, seen, maxID, id) }, r.due[:0])
		for _, slot := range r.due {
			m := r.slotMetric(slot)
			stages = append(stages, rollupStage{metric: m, slot: slot, next: m.metadata(r.meta[slot])})
			r.stageKey(&stages[len(stages)-1])
		}
	}
	// Expiry callbacks retain their serial owner contract. Idle plans do not
	// advance order, so the persisted reset frontier is already known here.
	for i := range stages {
		s := &stages[i]
		m := s.metric
		if r.expire != nil && len(s.batches) == 0 && r.expiryOverdue(s.slot, now.UnixNano()) && r.expire(m.rollupPolicy(), s.next, now) {
			// Older readers must reject reset authority rather than ignore its
			// fields and resurrect the covered slot history.
			s.next.Version = max(2, s.next.Version)
			s.next.Expired = true
			s.next.ResetOrder = s.next.Frontier.Order
			if s.next.Revision == math.MaxUint64 {
				return coreSealPlan{}, errors.New("metric revision exhausted")
			}
			s.next.Revision++
		}
	}
	var part partsformat.PartID
	if len(stages) > 0 {
		if _, err := rand.Read(part[:]); err != nil {
			return coreSealPlan{}, err
		}
	}
	out, err := r.planStages(stages, part)
	if err != nil {
		return coreSealPlan{}, err
	}
	r.stages = stages
	out.Names = named
	out.Abort = r.release
	out.Commit = func() {
		defer r.release()
		r.commitStages()
		r.calendar.drop(r.planNow)
		expiry := r.expire != nil
		if expiry {
			r.commitExpiries()
		}
		for i := range stages {
			s := &stages[i]
			m := s.metric
			if s.fresh {
				r.install(m.id, m, compactMetadata(s.next))
				r.names[m.policy.Name] = m.id
				continue
			}
			if s.refile != 0 {
				r.file(s.slot, m, r.meta[s.slot], s.refile)
			}
			if expiry {
				r.fileExpiry(s.slot) // a revived name, or one drained this plan
			}
		}
	}
	returned = true
	return out, nil
}

// ScanRollups reads one immutable core snapshot. It resolves each physical slot
// by logical timestamp, then class and source-local order, before time filters.
// The snapshot must include metadata and all archive SSTs from one publication.
func ScanRollups(snapshot *pebble.Snapshot, visit func(RollupPolicy, RollupMetadata, []sealrollup.Record) error) error {
	return scanRollups(snapshot, nil, func(p RollupPolicy, m RollupMetadata, records []sealrollup.Record) error {
		if m.Version == 3 && !m.Expired {
			records = sealrollup.ProjectOpen(p.Config, m.Frontier.MaxTime, m.ClosedThrough, records)
		}
		return visit(p, m, records)
	})
}
func scanRollups(snapshot *pebble.Snapshot, identities *PolicyCatalog, visit func(RollupPolicy, RollupMetadata, []sealrollup.Record) error) error {
	if copied, err := snapshotHasCopiedParts(snapshot); err != nil {
		return err
	} else if copied {
		return scanCopiedRollups(snapshot, visit)
	}
	type entry struct {
		group     partsformat.Group
		identity  *PolicyIdentity
		meta      metricMetadata
		records   map[[2]int64]sealrollup.Record
		single    [1]sealrollup.Record
		hasSingle bool
	}
	series := map[partsformat.ID]*entry{}
	iter, err := newArchiveIter(context.Background(), snapshot, &pebble.IterOptions{LowerBound: []byte{'G', 'C', 'P', 3}, UpperBound: partsformat.PrefixEnd([]byte{'G', 'C', 'P', 3})})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		k, err := partsformat.Decode(iter.Key())
		if err != nil {
			return err
		}
		e := series[k.Metric]
		if e == nil {
			e = &entry{group: k.Group}
			series[k.Metric] = e
		}
		if k.Ordinal != 0 || e.group != k.Group {
			return errors.New("unexpected rollup partition")
		}
		if k.Archive == rollupMetadataArchive {
			m, decodeErr := decodeRollupMetadataView(k.Metric, k.Group, iter.Value())
			if identities == nil {
				m.Policy = bytes.Clone(m.Policy)
			}
			if decodeErr != nil {
				err = decodeErr
				return err
			}
			if compactMetadata(m).after(e.meta) {
				var h *PolicyIdentity
				if identities != nil {
					h, err = identities.intern(m.ID, string(k.Metric), m.Policy)
				} else {
					var policy RollupPolicy
					if err = json.Unmarshal(m.Policy, &policy); err == nil {
						var prepared *sealrollup.Prepared
						prepared, err = sealrollup.Prepare(policy.Config)
						if err == nil {
							h = detachedPolicy(m.ID, policy, m.Policy, prepared)
						}
					}
				}
				if err != nil {
					return err
				}
				if h.policy.prepared.Digest() != m.Frontier.Policy {
					return errors.New("rollup policy/frontier mismatch")
				}
				e.identity, e.meta = h, compactMetadata(m)
			}
		} else {
			// Legacy singleton rows need no temporary decoded slice. Dense runs
			// retain their bounded decoder; slot replacement resolves once below.
			install := func(record sealrollup.Record) error {
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
			value := iter.Value()
			if len(value) == 26 && value[0] == 1 {
				record, err := sealrollup.DecodeRecord(int(k.Archive), k.BlockStart, value)
				if err != nil {
					return err
				}
				if err := install(record); err != nil {
					return err
				}
			} else {
				decoded, err := partsformat.DecodeSlots(k.Archive, k.BlockStart, value)
				if err != nil {
					return err
				}
				for _, record := range decoded {
					if err := install(record); err != nil {
						return err
					}
				}
			}
		}
	}
	if err = iter.Error(); err != nil {
		return err
	}
	for name, e := range series {
		if e.identity == nil {
			return fmt.Errorf("missing rollup metadata for %s", name)
		}
		policy, meta := e.identity.rollupPolicy(), e.identity.metadata(e.meta)
		if (meta.Version != 1 && meta.Version != 2 && meta.Version != 3) || policy.Group != e.group || policy.Name != name || meta.ID == 0 || meta.Frontier.Order == 0 {
			return fmt.Errorf("missing rollup metadata for %s", name)
		}
		if meta.BaselineOrder > meta.Frontier.Order {
			return errors.New("baseline exceeds rollup frontier")
		}
		if (meta.Version == 1 && (meta.ResetOrder != 0 || meta.Expired)) || meta.ResetOrder > meta.Frontier.Order || (meta.Expired && meta.ResetOrder != meta.Frontier.Order) {
			return errors.New("invalid rollup reset frontier")
		}
		var records []sealrollup.Record
		if e.records == nil {
			records = e.single[:0]
			if e.hasSingle {
				r := e.single[0]
				if r.Archive >= len(policy.Config.Archives) || r.Slot >= policy.Config.Archives[r.Archive].Count || r.Order > meta.Frontier.Order {
					return errors.New("invalid rollup slot/frontier")
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
				return errors.New("invalid rollup slot/frontier")
			}
			if record.Order > meta.ResetOrder {
				records = append(records, record)
			}
		}
		slices.SortFunc(records, func(a, b sealrollup.Record) int {
			if a.Archive < b.Archive {
				return -1
			}
			if a.Archive > b.Archive {
				return 1
			}
			if a.Slot < b.Slot {
				return -1
			}
			if a.Slot > b.Slot {
				return 1
			}
			return 0
		})
		if err = visit(policy, meta, records); err != nil {
			return err
		}
	}
	return nil
}
func scanCopiedRollups(snapshot *pebble.Snapshot, visit func(RollupPolicy, RollupMetadata, []sealrollup.Record) error) error {
	index, err := NewRollupSnapshot(context.Background(), snapshot, nil, 0)
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

func recoverCoreRollups(db *pebble.DB, catalogs ...*PolicyCatalog) (*coreRollups, error) {
	r := newCoreRollups()
	r.db = db
	if len(catalogs) > 0 {
		r.identities = catalogs[0]
	}
	snapshot := db.NewSnapshot()
	defer snapshot.Close()
	if found, err := snapshotHasCopiedParts(snapshot); err != nil {
		return nil, err
	} else if found {
		index, err := NewRollupSnapshot(context.Background(), snapshot, nil, 0)
		if err != nil {
			return nil, err
		}
		for _, m := range index.metrics {
			if index.moves[m] != nil && index.moves[m].copied && index.moves[m].authority == nil {
				return nil, errors.New("copied-part catalog requires live reducer and numeric identity adoption before writable open")
			}
		}
	}
	err := scanRollups(snapshot, r.identities, func(policy RollupPolicy, meta RollupMetadata, records []sealrollup.Record) error {
		m, err := r.metric(meta.ID, meta.Policy)
		if err != nil {
			return err
		}
		if r.metrics[meta.ID] != nil {
			return errors.New("duplicate recovered rollup ID")
		}
		m.slots.KeepRecent(meta.Frontier.MaxTime)
		for _, record := range records {
			m.slots.InstallRecent(record.Delta)
		}
		m.slots.KeepRecent(meta.Frontier.MaxTime)
		r.install(meta.ID, m, compactMetadata(meta))
		// Keep the recovered reverse index on the same immutable name as the
		// metric. ScanRollups' decoded policy is temporary construction state.
		r.names[m.policy.Name] = meta.ID
		return nil
	})
	return r, err
}

// Match GCP3 key ordering without retaining a complete key for every name.
func compareRollupIdentity(a, b *rollupMetric) int {
	if len(a.policy.Group) != len(b.policy.Group) {
		return len(a.policy.Group) - len(b.policy.Group)
	}
	if c := bytes.Compare([]byte(a.policy.Group), []byte(b.policy.Group)); c != 0 {
		return c
	}
	if c := int(partsformat.Route(a.policy.Name)) - int(partsformat.Route(b.policy.Name)); c != 0 {
		return c
	}
	if len(a.policy.Name) < len(b.policy.Name) {
		return -1
	}
	if len(a.policy.Name) > len(b.policy.Name) {
		return 1
	}
	return bytes.Compare([]byte(a.policy.Name), []byte(b.policy.Name))
}

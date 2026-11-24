package v2core

import (
	"cmp"
	"math"
	"slices"
	"sort"

	"github.com/go-graphite/go-carbon/internal/v2dict"
)

// stageKey assigns a stage its canonical sort key while the metric is hot.
// A ranked metric keys 2*rank. An unranked one (fresh, or installed since the
// last renumber) keys 2*L+1, where L dense metrics sort before it: it lands
// between dense[L-1] (key 2*L) and dense[L] (key 2*L+2). Unranked stages
// sharing a key tie-break by compareRollupMetrics, so the order is exact. A
// ranked metric whose hint another planner overwrote takes the search key,
// which sorts identically.
//
// With the store dictionary (ID-keyed parts) the key is (vshard, ID), the
// GCI2 key order, so no rank is consulted: dictKeys sets it once the seal's
// new names are appended.
func (r *coreRollups) stageKey(s *rollupStage, m *PolicyIdentity, dict *v2dict.Dict) {
	s.vshard = m.vshard
	if dict != nil {
		return
	}
	if rk := r.metricRank(m); rk != 0 {
		s.sortKey = uint64(rk) << 1
		return
	}
	l, _ := slices.BinarySearchFunc(r.dense, m, compareRollupMetrics)
	s.sortKey = uint64(l)<<1 | 1
}

// dictKeys keys every stage by (vshard, ID). The names the dictionary does
// not hold yet go to it in AppendBatch calls of at most dictChunk names from
// one fixed scratch, each appended under the lock of this owner's own
// dictionary shard, so a first seal of many new names allocates nothing per
// name; the seal syncs the dictionary before it writes a part.
func (r *coreRollups) dictKeys(dict *v2dict.Dict) error {
	var scratch [dictChunk]v2dict.NewName
	var metrics [dictChunk]*PolicyIdentity
	n := 0
	appendNames := func() error {
		err := dict.AppendBatch(scratch[:n])
		for i := range n {
			r.dictDirty = r.dictDirty || scratch[i].Created // every unresolved name on error
			if err == nil {
				metrics[i].rememberDictID(scratch[i].Entry.ID)
			}
		}
		n = 0
		return err
	}
	for _, m := range r.stageMetrics {
		if m.dictID.Load() != 0 {
			continue
		}
		scratch[n], metrics[n] = v2dict.NewName{Group: string(m.policy.Group), VShard: m.vshard, Name: string(m.metricName())}, m
		if n++; n == dictChunk {
			if err := appendNames(); err != nil {
				return err
			}
		}
	}
	if n > 0 {
		if err := appendNames(); err != nil {
			return err
		}
	}
	for i := range r.stages {
		s, m := &r.stages[i], r.stageMetrics[i]
		s.sortKey = uint64(m.vshard)<<32 | uint64(m.dictID.Load()-1)
	}
	return nil
}

// dictChunk is the most names one dictKeys AppendBatch call takes: one
// dictionary shard-lock hold.
const dictChunk = 256

// hintSlot returns the slot identity h names in this planner, when that slot
// holds id. A missing, foreign or stale hint fails the check.
func (r *coreRollups) hintSlot(h *PolicyIdentity, id uint64) (uint32, bool) {
	if h == nil {
		return 0, false
	}
	s := h.ownerSlot.Load() - 1 // no hint wraps past every slot
	if uint64(s) < uint64(len(r.ids)) && r.ids[s] == id {
		return s, true
	}
	return 0, false
}

// hintRank returns the rank of the name installed under id when hint h
// leads to its slot here, 0 otherwise (the caller probes names). A slot
// below len(dense) is the rank minus one.
func (r *coreRollups) hintRank(h *PolicyIdentity, id uint64) uint32 {
	if s, ok := r.hintSlot(h, id); ok && int(s) < len(r.dense) {
		return s + 1
	}
	return 0
}

// metricRank returns m's rank when this planner ranks m itself (not an older
// metric installed under the same ID) and m's hint is its own; 0 otherwise.
func (r *coreRollups) metricRank(m *PolicyIdentity) uint32 {
	if rk := r.hintRank(m, m.id); rk != 0 && r.dense[rk-1] == m {
		return rk
	}
	return 0
}

// deadSlot marks a dense slot whose metric was replaced (adoption). No hint
// passes ids[s] == id there, and the next renumber drops the slot.
const deadSlot = math.MaxUint64

// installed resolves identity before validating its owner-slot hint.
func (r *coreRollups) installed(id uint64) *PolicyIdentity {
	return r.installedIdentity(id, r.identities.lookup(id))
}

// nameSlot validates the owner-local slot independently of shared identity hints.
// A colliding fingerprint must never alias another numeric ID.
func (r *coreRollups) nameSlot(id uint64, m *PolicyIdentity) (uint32, bool) {
	if m == nil {
		return 0, false
	}
	s, ok := r.names.Get(m.fingerprint())
	return s, ok && uint64(s) < uint64(len(r.ids)) && r.ids[s] == id
}

func (r *coreRollups) installedIdentity(id uint64, m *PolicyIdentity) *PolicyIdentity {
	if s, ok := r.hintSlot(m, id); ok {
		return r.slotMetric(s)
	}
	if s, ok := r.nameSlot(id, m); ok {
		return r.slotMetric(s)
	}
	return nil
}

// slotOf uses the owner's fingerprint table when adoption overwrites hints.
func (r *coreRollups) slotOf(id uint64, m *PolicyIdentity) uint32 {
	if s, ok := r.hintSlot(m, id); ok && r.slotMetric(s) == m {
		return s
	}
	s, ok := r.nameSlot(id, m)
	if !ok || r.slotMetric(s) != m {
		panic("v2core: installed rollup metric has no owner slot")
	}
	m.ownerSlot.Store(s + 1)
	return s
}

// rollupTail is the unranked tail of the slot arrays: unranked[k] owns slot
// len(dense)+k. It sorts the columns together; from keeps each entry's old
// slot, so renumber can move the expiry calendar.
type rollupTail struct {
	metrics []*PolicyIdentity
	ids     []uint64
	meta    []metricMetadata
	ttl     []int64
	expAt   []int64
	from    []uint32
}

func (x rollupTail) Len() int { return len(x.metrics) }
func (x rollupTail) Less(i, j int) bool {
	return compareRollupMetrics(x.metrics[i], x.metrics[j]) < 0
}
func (x rollupTail) Swap(i, j int) {
	x.metrics[i], x.metrics[j] = x.metrics[j], x.metrics[i]
	x.ids[i], x.ids[j] = x.ids[j], x.ids[i]
	x.meta[i], x.meta[j] = x.meta[j], x.meta[i]
	if x.ttl != nil {
		x.ttl[i], x.ttl[j] = x.ttl[j], x.ttl[i]
		x.expAt[i], x.expAt[j] = x.expAt[j], x.expAt[i]
	}
	x.from[i], x.from[j] = x.from[j], x.from[i]
}

// renumber merges the unranked tail into dense so every slot is again its
// rank minus one: ids and meta move with their metric, in canonical order.
// It sorts the tail, compacts dense over dead slots in place, then merges
// from the back, so it needs only a tail-sized buffer. Dead (replaced) slots
// drop here; metrics are otherwise never removed from a planner. It re-stores
// every hint, which also restores the fast path after another planner
// overwrote one.
func (r *coreRollups) renumber() {
	if r.rankMu != nil {
		r.rankMu.Lock()
		defer r.rankMu.Unlock()
	}
	d, f := len(r.dense), len(r.unranked)
	if d+f >= math.MaxUint32 {
		return // every new name stays unranked; plans stay exact on the search path
	}
	tail := rollupTail{metrics: r.unranked, ids: r.ids[d:], meta: r.meta[d:], from: make([]uint32, f)}
	if r.ttl != nil {
		tail.ttl, tail.expAt = r.ttl[d:], r.expAt[d:]
	}
	for k := range tail.from {
		tail.from[k] = uint32(d + k)
	}
	sort.Sort(tail)
	fresh := rollupTail{slices.Clone(tail.metrics), slices.Clone(tail.ids), slices.Clone(tail.meta), slices.Clone(tail.ttl), slices.Clone(tail.expAt), tail.from}
	// moved maps each old slot to its new one (gone for a dead slot); only
	// the expiry calendar hold slots across a renumber.
	var moved []uint32
	if len(r.expiries.slots) > 0 {
		moved = make([]uint32, d+f)
	}
	n := 0
	for i, id := range r.ids[:d] {
		if id != deadSlot {
			r.dense[n], r.ids[n], r.meta[n] = r.dense[i], id, r.meta[i]
			if r.ttl != nil {
				r.ttl[n], r.expAt[n] = r.ttl[i], r.expAt[i]
			}
			if moved != nil {
				moved[i] = uint32(n)
			}
			n++
		} else if moved != nil {
			moved[i] = gone
		}
	}
	size := n + f
	if size < d {
		clear(r.dense[size:])
	}
	r.dense = slices.Grow(r.dense, size-len(r.dense))[:size]
	r.ids, r.meta = r.ids[:size], r.meta[:size]
	if r.ttl != nil {
		r.ttl, r.expAt = r.ttl[:size], r.expAt[:size]
	}
	for i, j, w, o := n-1, f-1, size-1, d-1; j >= 0; w-- {
		if i >= 0 && compareRollupMetrics(r.dense[i], fresh.metrics[j]) > 0 {
			r.dense[w], r.ids[w], r.meta[w] = r.dense[i], r.ids[i], r.meta[i]
			if r.ttl != nil {
				r.ttl[w], r.expAt[w] = r.ttl[i], r.expAt[i]
			}
			if moved != nil {
				for moved[o] != uint32(i) { // compacted slot i came from old slot o
					o--
				}
				moved[o] = uint32(w)
				o--
			}
			i--
		} else {
			r.dense[w], r.ids[w], r.meta[w] = fresh.metrics[j], fresh.ids[j], fresh.meta[j]
			if r.ttl != nil {
				r.ttl[w], r.expAt[w] = fresh.ttl[j], fresh.expAt[j]
			}
			if moved != nil {
				moved[fresh.from[j]] = uint32(w)
			}
			j--
		}
	}
	r.expiries.move(moved)
	for s, m := range r.dense {
		r.names.Put(m.fingerprint(), uint32(s))
		if m.ownerSlot.Load() != uint32(s)+1 {
			m.ownerSlot.Store(uint32(s) + 1)
		}
	}
	clear(r.unranked)
	r.unranked = r.unranked[:0]
}

// rollupRankRef is the 16-byte, pointer-free sort record of one stage: its
// key and its index in the planner's stages.
type rollupRankRef struct {
	key   uint64
	stage uint32
}

// sortRollupRanks orders refs by key with an LSD radix sort (11-bit digits,
// only as many passes as the largest key needs), then sorts each run of equal
// odd keys (unranked neighbours) by name. It returns the sorted slice and the
// spare buffer for reuse. metrics[ref.stage] names each ref's metric.
func sortRollupRanks(refs, scratch []rollupRankRef, counts *[]int, metrics []*PolicyIdentity) ([]rollupRankRef, []rollupRankRef) {
	byName := func(a, b rollupRankRef) int { return compareRollupMetrics(metrics[a.stage], metrics[b.stage]) }
	if len(refs) < 256 {
		slices.SortFunc(refs, func(a, b rollupRankRef) int {
			if c := cmp.Compare(a.key, b.key); c != 0 || a.key&1 == 0 {
				return c
			}
			return byName(a, b)
		})
		return refs, scratch
	}
	const bits = 11
	const mask = 1<<bits - 1
	maxKey := uint64(0)
	for i := range refs {
		maxKey = max(maxKey, refs[i].key)
	}
	tmp := slices.Grow(scratch[:0], len(refs))[:len(refs)]
	count := slices.Grow((*counts)[:0], 1<<bits)[:1<<bits]
	*counts = count
	src := refs
	for shift := uint(0); shift < 64 && maxKey>>shift != 0; shift += bits {
		clear(count)
		for i := range src {
			count[(src[i].key>>shift)&mask]++
		}
		sum := 0
		for d, c := range count {
			count[d] = sum
			sum += c
		}
		for i := range src {
			d := (src[i].key >> shift) & mask
			tmp[count[d]] = src[i]
			count[d]++
		}
		src, tmp = tmp, src
	}
	for i := 0; i < len(src); {
		j := i + 1
		for j < len(src) && src[j].key == src[i].key {
			j++
		}
		if j-i > 1 {
			slices.SortFunc(src[i:j], byName)
		}
		i = j
	}
	clear(tmp)
	return src, tmp[:0]
}

// sealSeenWords sizes the idle pass's staged-ID bitset: one bit per ID up to
// maxID, or 0 (binary search instead) when that exceeds two words per application,
// so the bitset never outgrows the sort keys and their spare.
func sealSeenWords(maxID uint64, batches int) int {
	if maxID>>6 >= uint64(2*batches+1024) {
		return 0
	}
	return int(maxID>>6) + 1
}

// gone marks a dead slot in renumber's old-to-new slot map.
const gone = math.MaxUint32

// move rewrites every calendar entry to its slot's new number and drops
// dead slots; moved is renumber's old-to-new map (nil: nothing filed). An
// emptied second stays filed (heap and slots keep one entry per second)
// until a pass drops it.
func (c *calendar) move(moved []uint32) {
	if moved == nil {
		return
	}
	for end, slots := range c.slots {
		kept := slots[:0]
		for _, s := range slots {
			if ns := moved[s]; ns != gone {
				kept = append(kept, ns)
			}
		}
		c.slots[end] = kept
	}
}

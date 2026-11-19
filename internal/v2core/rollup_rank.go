package v2core

import (
	"cmp"
	"math"
	"slices"
	"sort"
)

// stageKey assigns a stage its canonical sort key while the metric is hot.
// A ranked metric keys 2*rank. An unranked one (fresh, or installed since the
// last renumber) keys 2*L+1, where L dense metrics sort before it: it lands
// between dense[L-1] (key 2*L) and dense[L] (key 2*L+2). Unranked stages
// sharing a key tie-break by compareRollupMetrics, so the order is exact. A
// ranked metric whose hint another planner overwrote takes the search key,
// which sorts identically.
func (r *coreRollups) stageKey(s *rollupStage) {
	m := s.metric
	s.vshard = m.vshard
	if rk := r.metricRank(m); rk != 0 {
		s.sortKey = uint64(rk) << 1
		return
	}
	l, _ := slices.BinarySearchFunc(r.dense, m, compareRollupMetrics)
	s.sortKey = uint64(l)<<1 | 1
}

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
// leads to its slot here, 0 otherwise (the caller probes metrics). A slot
// below len(dense) is the rank minus one.
func (r *coreRollups) hintRank(h *PolicyIdentity, id uint64) uint32 {
	if s, ok := r.hintSlot(h, id); ok && int(s) < len(r.dense) {
		return s + 1
	}
	return 0
}

// metricRank returns m's rank when this planner ranks m itself (not an older
// metric installed under the same ID) and m's hint is its own; 0 otherwise.
func (r *coreRollups) metricRank(m *rollupMetric) uint32 {
	if rk := r.hintRank(m.PolicyIdentity, m.id); rk != 0 && r.dense[rk-1] == m {
		return rk
	}
	return 0
}

// deadSlot marks a dense slot whose metric was replaced (adoption). No hint
// passes ids[s] == id there, and the next renumber drops the slot.
const deadSlot = math.MaxUint64

// slotOf returns the slot of m, installed under id. Its identity hint answers
// in O(1). When another planner overwrote the hint, a binary search of dense
// finds a ranked m and a scan of unranked (at most 1/rankSlack of dense) an
// unranked one; the hint is then restored, so each lost hint costs one search.
func (r *coreRollups) slotOf(id uint64, m *rollupMetric) uint32 {
	if s, ok := r.hintSlot(m.PolicyIdentity, id); ok {
		return s
	}
	s, found := uint32(0), false
	for p, _ := slices.BinarySearchFunc(r.dense, m, compareRollupMetrics); p < len(r.dense) && compareRollupMetrics(r.dense[p], m) == 0; p++ {
		if r.dense[p] == m && r.ids[p] == id {
			s, found = uint32(p), true
			break
		}
	}
	for k := len(r.unranked) - 1; !found && k >= 0; k-- {
		if r.unranked[k] == m {
			s, found = uint32(len(r.dense)+k), true
		}
	}
	if !found {
		panic("v2core: installed rollup metric has no owner slot")
	}
	m.ownerSlot.Store(s + 1)
	return s
}

// rollupTail is the unranked tail of the slot arrays: unranked[k] owns slot
// len(dense)+k. It sorts the columns together; from keeps each entry's old
// slot, so renumber can move the closure calendar.
type rollupTail struct {
	metrics []*rollupMetric
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
	x.ttl[i], x.ttl[j] = x.ttl[j], x.ttl[i]
	x.expAt[i], x.expAt[j] = x.expAt[j], x.expAt[i]
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
	d, f := len(r.dense), len(r.unranked)
	if d+f >= math.MaxUint32 {
		return // every new name stays unranked; plans stay exact on the search path
	}
	tail := rollupTail{r.unranked, r.ids[d:], r.meta[d:], r.ttl[d:], r.expAt[d:], make([]uint32, f)}
	for k := range tail.from {
		tail.from[k] = uint32(d + k)
	}
	sort.Sort(tail)
	fresh := rollupTail{slices.Clone(tail.metrics), slices.Clone(tail.ids), slices.Clone(tail.meta), slices.Clone(tail.ttl), slices.Clone(tail.expAt), tail.from}
	// moved maps each old slot to its new one (gone for a dead slot); only
	// the closure and expiry calendars hold slots across a renumber.
	var moved []uint32
	if len(r.calendar.slots) > 0 || len(r.expiries.slots) > 0 {
		moved = make([]uint32, d+f)
	}
	n := 0
	for i, id := range r.ids[:d] {
		if id != deadSlot {
			r.dense[n], r.ids[n], r.meta[n], r.ttl[n], r.expAt[n] = r.dense[i], id, r.meta[i], r.ttl[i], r.expAt[i]
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
	r.ids, r.meta, r.ttl, r.expAt = r.ids[:size], r.meta[:size], r.ttl[:size], r.expAt[:size]
	for i, j, w, o := n-1, f-1, size-1, d-1; j >= 0; w-- {
		if i >= 0 && compareRollupMetrics(r.dense[i], fresh.metrics[j]) > 0 {
			r.dense[w], r.ids[w], r.meta[w], r.ttl[w], r.expAt[w] = r.dense[i], r.ids[i], r.meta[i], r.ttl[i], r.expAt[i]
			if moved != nil {
				for moved[o] != uint32(i) { // compacted slot i came from old slot o
					o--
				}
				moved[o] = uint32(w)
				o--
			}
			i--
		} else {
			r.dense[w], r.ids[w], r.meta[w], r.ttl[w], r.expAt[w] = fresh.metrics[j], fresh.ids[j], fresh.meta[j], fresh.ttl[j], fresh.expAt[j]
			if moved != nil {
				moved[fresh.from[j]] = uint32(w)
			}
			j--
		}
	}
	r.calendar.move(moved)
	r.expiries.move(moved)
	for s, m := range r.dense {
		if m.ownerSlot.Load() != uint32(s)+1 {
			m.ownerSlot.Store(uint32(s) + 1)
		}
	}
	clear(r.unranked)
	r.unranked = r.unranked[:0]
}

// rollupRankRef is the 16-byte sort record of one stage.
type rollupRankRef struct {
	key uint64
	*rollupStage
}

func compareRollupRankRefs(a, b rollupRankRef) int {
	if a.key != b.key {
		if a.key < b.key {
			return -1
		}
		return 1
	}
	if a.key&1 == 0 {
		return 0
	}
	return compareRollupMetrics(a.metric, b.metric)
}

// sortRollupRanks orders refs by key with an LSD radix sort (11-bit digits,
// only as many passes as the largest key needs), then sorts each run of equal
// odd keys (unranked neighbours) by name. It returns the sorted slice and the
// spare buffer for reuse.
func sortRollupRanks(refs, scratch []rollupRankRef, counts *[]int) ([]rollupRankRef, []rollupRankRef) {
	if len(refs) < 256 {
		slices.SortFunc(refs, compareRollupRankRefs)
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
			slices.SortFunc(src[i:j], compareRollupMetricsRef)
		}
		i = j
	}
	clear(tmp)
	return src, tmp[:0]
}

func compareRollupMetricsRef(a, b rollupRankRef) int {
	return compareRollupMetrics(a.metric, b.metric)
}

// sealBatchIndexMask selects the batch index from a sorted seal batch key.
const sealBatchIndexMask = 1<<32 - 1

// sortSealBatches returns the batch indices sorted by ID, stable, so each
// name's batches form one run in arrival order and last-write-wins per slot
// is preserved by construction. A key is ID<<32 | index; LSD radix passes run
// over the ID bits only, as many as the largest ID needs, and the ascending
// index bits keep equal IDs in arrival order. IDs of 32 bits or more take a
// cold comparison-sort path on index-only keys. It also returns the spare
// buffer for reuse and the largest ID.
func sortSealBatches(batches []coreBatch, keys, spare []uint64) (sorted, tmp []uint64, maxID uint64) {
	n := len(batches)
	keys = slices.Grow(keys[:0], n)[:n]
	for i := range batches {
		maxID = max(maxID, batches[i].ID)
	}
	if maxID > sealBatchIndexMask || uint64(n) > sealBatchIndexMask {
		for i := range keys {
			keys[i] = uint64(i)
		}
		slices.SortStableFunc(keys, func(a, b uint64) int { return cmp.Compare(batches[a].ID, batches[b].ID) })
		return keys, spare, maxID
	}
	for i := range batches {
		keys[i] = batches[i].ID<<32 | uint64(i)
	}
	if n < 256 {
		slices.Sort(keys)
		return keys, spare, maxID
	}
	const bits = 11
	const mask = 1<<bits - 1
	var count [1 << bits]int
	tmp = slices.Grow(spare[:0], n)[:n]
	src := keys
	for shift := uint(32); shift < 64 && maxID>>(shift-32) != 0; shift += bits {
		clear(count[:])
		for _, k := range src {
			count[(k>>shift)&mask]++
		}
		sum := 0
		for d, c := range count {
			count[d] = sum
			sum += c
		}
		for _, k := range src {
			d := (k >> shift) & mask
			tmp[count[d]] = k
			count[d]++
		}
		src, tmp = tmp, src
	}
	return src, tmp[:0], maxID
}

// sealSeenWords sizes the idle pass's staged-ID bitset: one bit per ID up to
// maxID, or 0 (binary search instead) when that exceeds two words per batch,
// so the bitset never outgrows the sort keys and their spare.
func sealSeenWords(maxID uint64, batches int) int {
	if maxID>>6 >= uint64(2*batches+1024) {
		return 0
	}
	return int(maxID>>6) + 1
}

// sealBatchSeen reports whether this seal staged id: a bitset probe when the
// IDs were dense enough for one, otherwise a binary search of the sorted keys.
func sealBatchSeen(batches []coreBatch, keys, seen []uint64, maxID, id uint64) bool {
	if id > maxID {
		return false
	}
	if len(seen) != 0 {
		return seen[id>>6]&(1<<(id&63)) != 0
	}
	_, ok := slices.BinarySearchFunc(keys, id, func(k, id uint64) int { return cmp.Compare(batches[k&sealBatchIndexMask].ID, id) })
	return ok
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

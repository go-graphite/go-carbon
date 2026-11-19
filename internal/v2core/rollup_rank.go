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
func (r *coreRollups) stageKey(s *rollupStage, m *rollupMetric) {
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
// leads to its slot here, 0 otherwise (the caller probes metrics). By the
// slot invariant a non-zero rank always names the installed metric.
func (r *coreRollups) hintRank(h *PolicyIdentity, id uint64) uint32 {
	if s, ok := r.hintSlot(h, id); ok {
		return r.rank[s]
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

// retire clears the rank of old, the metric being replaced under id, and
// returns id's slot for reuse. Its identity hint finds a ranked old in O(1).
// When another planner overwrote the hint, a binary search of dense finds a
// ranked old, so no stale rank survives a replacement whatever the hint says.
// An unranked old has no rank to clear; with a lost hint the ID takes a new
// slot and the old one keeps rank 0, which routes lookups to the map.
func (r *coreRollups) retire(id uint64, old *rollupMetric) (uint32, bool) {
	s, ok := r.hintSlot(old.PolicyIdentity, id)
	if ok && r.rank[s] != 0 && r.dense[r.rank[s]-1] == old {
		r.rank[s] = 0
		return s, true
	}
	for p, _ := slices.BinarySearchFunc(r.dense, old, compareRollupMetrics); p < len(r.dense) && compareRollupMetrics(r.dense[p], old) == 0; p++ {
		if d := r.denseSlots[p]; r.dense[p] == old && r.rank[d] == uint32(p+1) {
			r.rank[d] = 0
			return d, true
		}
	}
	return s, ok
}

// rollupSlotSorter sorts unranked metrics with their slots alongside.
type rollupSlotSorter struct {
	metrics []*rollupMetric
	slots   []uint32
}

func (x rollupSlotSorter) Len() int { return len(x.metrics) }
func (x rollupSlotSorter) Less(i, j int) bool {
	return compareRollupMetrics(x.metrics[i], x.metrics[j]) < 0
}
func (x rollupSlotSorter) Swap(i, j int) {
	x.metrics[i], x.metrics[j] = x.metrics[j], x.metrics[i]
	x.slots[i], x.slots[j] = x.slots[j], x.slots[i]
}

// renumber merges the sorted unranked metrics into dense in one sequential
// pass and rewrites every rank from the parallel slot arrays. Replaced IDs
// (adoption) drop their stale metric here; metrics are otherwise never
// removed from a planner. It re-stores each dense metric's hint, which
// restores the fast path after another planner overwrote them.
func (r *coreRollups) renumber() {
	replaced := r.replaced
	live := func(m *rollupMetric) bool { return r.metrics[m.id] == m }
	// A metric this planner ranks is in dense; dead (replaced) ones drop.
	fresh, freshSlots := r.unranked[:0], r.unrankedSlots[:0]
	for k, m := range r.unranked {
		s := r.unrankedSlots[k]
		if rk := r.rank[s]; (rk != 0 && r.dense[rk-1] == m) || (replaced && !live(m)) {
			continue
		}
		fresh, freshSlots = append(fresh, m), append(freshSlots, s)
	}
	sort.Sort(rollupSlotSorter{fresh, freshSlots})
	n := 0
	for k, m := range fresh {
		if n == 0 || m != fresh[n-1] {
			fresh[n], freshSlots[n] = m, freshSlots[k]
			n++
		}
	}
	clear(r.unranked[n:])
	fresh, freshSlots = fresh[:n], freshSlots[:n]
	r.unranked, r.unrankedSlots = fresh, freshSlots
	dense, denseSlots := r.dense, r.denseSlots
	if len(dense)+len(fresh) >= math.MaxUint32 {
		return // every new name stays unranked; plans stay exact on the search path
	}
	out := slices.Grow(r.denseSpare[:0], len(dense)+len(fresh))
	outSlots := slices.Grow(r.denseSlotsSpare[:0], len(dense)+len(fresh))
	i, j := 0, 0
	for i < len(dense) || j < len(fresh) {
		if i < len(dense) && replaced {
			if s := denseSlots[i]; r.rank[s] != uint32(i+1) || !live(dense[i]) {
				if r.rank[s] == uint32(i+1) {
					r.rank[s] = 0
				}
				i++
				continue
			}
		}
		if j == len(fresh) || (i < len(dense) && compareRollupMetrics(dense[i], fresh[j]) < 0) {
			out, outSlots = append(out, dense[i]), append(outSlots, denseSlots[i])
			i++
		} else {
			out, outSlots = append(out, fresh[j]), append(outSlots, freshSlots[j])
			j++
		}
	}
	for k, m := range out {
		s := outSlots[k]
		r.rank[s] = uint32(k + 1)
		if m.ownerSlot.Load() != s+1 {
			m.ownerSlot.Store(s + 1)
		}
	}
	clear(dense)
	r.dense, r.denseSpare = out, dense[:0]
	r.denseSlots, r.denseSlotsSpare = outSlots, denseSlots[:0]
	clear(r.unranked)
	r.unranked, r.unrankedSlots = r.unranked[:0], r.unrankedSlots[:0]
	r.replaced = false
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
func sortRollupRanks(refs, scratch []rollupRankRef, counts *[]int, metrics []*rollupMetric) ([]rollupRankRef, []rollupRankRef) {
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

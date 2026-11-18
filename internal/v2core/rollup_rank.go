package v2core

import (
	"cmp"
	"math"
	"slices"
)

// stageKey assigns a stage its canonical sort key while the metric is hot.
// A ranked metric keys 2*rank. An unranked one (fresh, or installed since the
// last renumber) keys 2*L+1, where L dense metrics sort before it: it lands
// between dense[L-1] (key 2*L) and dense[L] (key 2*L+2). Unranked stages
// sharing a key tie-break by compareRollupMetrics, so the order is exact.
func (r *coreRollups) stageKey(s *rollupStage) {
	m := s.metric
	s.vshard = m.vshard
	if rk := r.rankOf(m.id); rk != 0 && r.dense[rk-1] == m {
		s.sortKey = uint64(rk) << 1
		return
	}
	l, _ := slices.BinarySearchFunc(r.dense, m, compareRollupMetrics)
	s.sortKey = uint64(l)<<1 | 1
}

// renumber merges the sorted unranked metrics into dense in one sequential
// pass and rewrites every rank. Replaced IDs (adoption) drop their stale
// metric here; metrics are otherwise never removed from a planner.
// rankOf returns id's rank in this planner, 0 when unranked. The owner check
// is a range check: IDs past the array were never ranked here.
func (r *coreRollups) rankOf(id uint64) uint32 {
	if id < uint64(len(r.rank)) {
		return r.rank[id]
	}
	return 0
}

// ranks reports whether this planner ranks m itself (not an older metric that
// was installed under the same ID).
func (r *coreRollups) ranks(m *rollupMetric) bool {
	rk := r.rankOf(m.id)
	return rk != 0 && r.dense[rk-1] == m
}

func (r *coreRollups) renumber() {
	replaced := r.replaced
	live := func(m *rollupMetric) bool { return r.metrics[m.id] == m }
	// A metric this planner ranks is in dense; dead (replaced) ones drop.
	fresh := slices.DeleteFunc(r.unranked, func(m *rollupMetric) bool {
		return r.ranks(m) || (replaced && !live(m))
	})
	slices.SortFunc(fresh, compareRollupMetrics)
	fresh = slices.CompactFunc(fresh, func(a, b *rollupMetric) bool { return a == b })
	dense := r.dense
	if len(dense)+len(fresh) >= math.MaxUint32 {
		return // every new name stays unranked; plans stay exact on the search path
	}
	out := slices.Grow(r.denseSpare[:0], len(dense)+len(fresh))
	i, j := 0, 0
	for i < len(dense) || j < len(fresh) {
		if i < len(dense) && replaced {
			if m := dense[i]; r.rankOf(m.id) != uint32(i+1) || !live(m) {
				if r.rankOf(m.id) == uint32(i+1) {
					r.rank[m.id] = 0
				}
				i++
				continue
			}
		}
		if j == len(fresh) || (i < len(dense) && compareRollupMetrics(dense[i], fresh[j]) < 0) {
			out = append(out, dense[i])
			i++
		} else {
			out = append(out, fresh[j])
			j++
		}
	}
	maxID := uint64(0)
	for _, m := range out {
		maxID = max(maxID, m.id)
	}
	if uint64(len(r.rank)) <= maxID {
		r.rank = slices.Grow(r.rank, int(maxID+1)-len(r.rank))[:maxID+1]
	}
	for k, m := range out {
		r.rank[m.id] = uint32(k + 1)
	}
	clear(dense)
	r.dense, r.denseSpare = out, dense[:0]
	clear(r.unranked)
	r.unranked = r.unranked[:0]
	r.replaced = false
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

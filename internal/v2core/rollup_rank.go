package v2core

import (
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
	if m.rankOwner == r && m.rank != 0 {
		s.sortKey = uint64(m.rank) << 1
		return
	}
	l, _ := slices.BinarySearchFunc(r.dense, m, compareRollupMetrics)
	s.sortKey = uint64(l)<<1 | 1
}

// renumber merges the sorted unranked metrics into dense in one sequential
// pass and rewrites every rank. Replaced IDs (adoption) drop their stale
// metric here; metrics are otherwise never removed from a planner.
func (r *coreRollups) renumber() {
	replaced := r.replaced
	live := func(m *rollupMetric) bool { return r.metrics[m.id] == m }
	// A metric this planner ranks is in dense; dead (replaced) ones drop.
	fresh := slices.DeleteFunc(r.unranked, func(m *rollupMetric) bool {
		return (m.rankOwner == r && m.rank != 0) || (replaced && !live(m))
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
			if m := dense[i]; m.rankOwner != r || m.rank != uint32(i+1) || !live(m) {
				if m.rankOwner == r && m.rank == uint32(i+1) {
					m.rank, m.rankOwner = 0, nil
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
	for k, m := range out {
		m.rank, m.rankOwner = uint32(k+1), r
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

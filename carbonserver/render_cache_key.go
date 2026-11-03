package carbonserver

import (
	"cmp"
	"encoding/binary"
	"slices"
)

// canonicalRenderCacheKey treats requests as a multiset of (range, name,
// expression) tuples. It never rewrites Graphite expressions or deduplicates
// targets. Length framing keeps delimiters in names and expressions unambiguous.
// Inputs remain untouched: fetch/stream still uses the caller's request order.
func canonicalRenderCacheKey(targets map[timeRange][]target, format string) string {
	type entry struct {
		timeRange
		target
	}
	count, size := 0, len(format)+32
	for _, ts := range targets {
		count += len(ts)
		for _, t := range ts {
			size += len(t.Name) + 24
			if t.PathExpression != t.Name {
				size += len(t.PathExpression)
			}
		}
	}
	entries := make([]entry, 0, count)
	for tr, ts := range targets {
		for _, t := range ts {
			entries = append(entries, entry{tr, t})
		}
	}
	slices.SortFunc(entries, func(a, b entry) int {
		if c := cmp.Compare(a.from, b.from); c != 0 {
			return c
		}
		if c := cmp.Compare(a.until, b.until); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Name, b.Name); c != 0 {
			return c
		}
		return cmp.Compare(a.PathExpression, b.PathExpression)
	})
	b := make([]byte, 0, size)
	b = append(b, "render-v2:"...)
	b = binary.AppendUvarint(b, uint64(len(format)))
	b = append(b, format...)
	for _, e := range entries {
		b = binary.AppendVarint(b, int64(e.from))
		b = binary.AppendVarint(b, int64(e.until))
		b = binary.AppendUvarint(b, uint64(len(e.Name)))
		b = append(b, e.Name...)
		// Most requests use the name as their expression. Zero represents
		// that exact equality; length+1 frames every other expression.
		if e.PathExpression == e.Name {
			b = binary.AppendUvarint(b, 0)
		} else {
			b = binary.AppendUvarint(b, uint64(len(e.PathExpression))+1)
			b = append(b, e.PathExpression...)
		}
	}
	return string(b)
}

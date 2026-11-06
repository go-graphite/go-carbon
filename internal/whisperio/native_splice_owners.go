package whisperio

import (
	"cmp"
	"context"
	"math"
	"math/bits"
	"slices"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type rankedSplicePart struct {
	part  splicePart
	epoch int64
	order int
}
type spliceOwnerChunk struct {
	single   splicePart
	fragment *spliceFragment
	epoch    int64
	mask     [2]uint64
	parts    []rankedSplicePart
}

func spliceRankAfter(epoch int64, f *spliceFragment, oldEpoch int64, old *spliceFragment) bool {
	if epoch != oldEpoch {
		return epoch > oldEpoch
	}
	if f.priority != old.priority {
		return f.priority > old.priority
	}
	return f.sequence >= old.sequence
}

func (c *spliceOwnerChunk) add(f *spliceFragment, slot, ordinal, count int, epoch int64, r store.Retention) bool {
	mask := spliceMask(slot%128, count)
	intersects := mask[0]&c.mask[0] != 0 || mask[1]&c.mask[1] != 0
	if c.fragment == nil && c.parts == nil {
		c.single = splicePart{*f, slot, ordinal, count}
		c.fragment = f
		c.epoch = epoch
		c.mask = mask
		return false
	}
	if c.parts == nil {
		if mask[0]&c.mask[0] == c.mask[0] && mask[1]&c.mask[1] == c.mask[1] && spliceRankAfter(epoch, f, c.epoch, c.fragment) {
			c.single = splicePart{*f, slot, ordinal, count}
			c.fragment = f
			c.epoch = epoch
			c.mask = mask
			return intersects
		}
		if c.mask[0]&mask[0] == mask[0] && c.mask[1]&mask[1] == mask[1] && !spliceRankAfter(epoch, f, c.epoch, c.fragment) {
			return intersects
		}
		c.parts = append(c.parts, rankedSplicePart{part: c.single, epoch: c.epoch})
		c.fragment = nil
	}
	c.parts = append(c.parts, rankedSplicePart{part: splicePart{*f, slot, ordinal, count}, epoch: epoch, order: len(c.parts)})
	c.mask[0] |= mask[0]
	c.mask[1] |= mask[1]
	return intersects
}

func spliceOrdinalTime(f *spliceFragment, ordinal, step int) int64 {
	if f.irregular {
		return f.payload.points[ordinal].Timestamp
	}
	return f.first + int64(ordinal-f.ordinal)*int64(step)
}

// Dense nonintersecting chunks retain one interval descriptor. Conflicts are
// resolved as ranked masks within each 128-slot chunk, without expanding dense
// intervals into individual owners. Sparse fragments enter as single-slot masks.
func selectSpliceOwners(ctx context.Context, a, b []spliceFragment, r store.Retention) ([]splicePart, int64, bool, error) {
	chunks := make(map[int]*spliceOwnerChunk)
	intersects := false
	for _, fragments := range [][]spliceFragment{a, b} {
		for fi := range fragments {
			if err := ctx.Err(); err != nil {
				return nil, 0, false, err
			}
			f := &fragments[fi]
			add := func(slot, ordinal, count int, epoch int64) {
				chunk := chunks[slot/128]
				if chunk == nil {
					chunk = &spliceOwnerChunk{}
					chunks[slot/128] = chunk
				}
				intersects = chunk.add(f, slot, ordinal, count, epoch, r) || intersects
			}
			if f.irregular {
				for ordinal := f.ordinal; ordinal < f.ordinal+f.count; ordinal++ {
					p := f.payload.points[ordinal]
					if p.Timestamp <= 0 || math.IsNaN(p.Value) {
						continue
					}
					absolute := p.Timestamp / int64(r.Step)
					add(int(absolute%int64(r.Count)), ordinal, 1, absolute/int64(r.Count))
				}
			} else {
				for offset := 0; offset < f.count; {
					timestamp := f.first + int64(offset)*int64(r.Step)
					absolute := timestamp / int64(r.Step)
					slot := int(absolute % int64(r.Count))
					count := min(f.count-offset, 128-slot%128, r.Count-slot)
					add(slot, f.ordinal+offset, count, absolute/int64(r.Count))
					offset += count
				}
			}
		}
	}
	var result []splicePart
	var base int64
	include := func(ts int64) {
		if base == 0 || ts < base {
			base = ts
		}
	}
	for index, chunk := range chunks {
		if chunk.parts == nil {
			result = append(result, chunk.single)
			include(spliceOrdinalTime(chunk.fragment, chunk.single.ordinal, r.Step))
			continue
		}
		slices.SortFunc(chunk.parts, func(a, b rankedSplicePart) int {
			if order := cmp.Compare(b.epoch, a.epoch); order != 0 {
				return order
			}
			if order := cmp.Compare(b.part.fragment.priority, a.part.fragment.priority); order != 0 {
				return order
			}
			if order := cmp.Compare(b.part.fragment.sequence, a.part.fragment.sequence); order != 0 {
				return order
			}
			return b.order - a.order
		})
		var claimed [2]uint64
		for _, ranked := range chunk.parts {
			part := ranked.part
			mask := spliceMask(part.slot%128, part.count)
			for word := range mask {
				remaining := mask[word] &^ claimed[word]
				claimed[word] |= mask[word]
				for remaining != 0 {
					first := bits.TrailingZeros64(remaining)
					count := min(bits.TrailingZeros64(^(remaining >> uint(first))), 64-first)
					offset := word*64 + first - part.slot%128
					selected := part
					selected.slot = index*128 + word*64 + first
					selected.ordinal += offset
					selected.count = count
					result = append(result, selected)
					include(spliceOrdinalTime(&selected.fragment, selected.ordinal, r.Step))
					remaining &^= spliceWordMask(first, count)
				}
			}
		}
	}
	return result, base, intersects, nil
}

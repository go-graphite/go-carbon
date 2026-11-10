package whisperio

import (
	"context"
	"math"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type spliceSlotOwner struct {
	// Eligibility bounds physical timestamps to uint32 and group ordinals to
	// MaxSealedPoints. Keep each winner in 16 bytes instead of 24.
	fragment  *spliceFragment
	ordinal   uint16
	timestamp uint32
}
type spliceOwnerChunk struct {
	single   splicePart
	fragment *spliceFragment
	epoch    int64
	mask     [2]uint64
	owners   *[128]spliceSlotOwner
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
	if c.fragment == nil && c.owners == nil {
		c.single = splicePart{*f, slot, ordinal, count}
		c.fragment = f
		c.epoch = epoch
		c.mask = mask
		return false
	}
	if c.owners == nil {
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
		c.owners = new([128]spliceSlotOwner)
		for i := 0; i < c.single.count; i++ {
			o := c.single.ordinal + i
			c.owners[(c.single.slot+i)%128] = spliceSlotOwner{c.fragment, uint16(o), uint32(spliceOrdinalTime(c.fragment, o, r.Step))}
		}
		c.fragment = nil
	}
	for i := 0; i < count; i++ {
		o := ordinal + i
		ts := uint32(spliceOrdinalTime(f, o, r.Step))
		old := &c.owners[(slot+i)%128]
		if old.fragment == nil || ts > old.timestamp || ts == old.timestamp && spliceRankAfter(epoch, f, epoch, old.fragment) {
			*old = spliceSlotOwner{f, uint16(o), ts}
		}
	}
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

// Dense nonintersecting chunks retain one interval descriptor. Only a chunk
// requiring per-slot comparisons allocates the fixed 128-slot winner table.
// This avoids point-sized fragment lists and sorting in sparse histories.
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
		if chunk.owners == nil {
			result = append(result, chunk.single)
			include(spliceOrdinalTime(chunk.fragment, chunk.single.ordinal, r.Step))
			continue
		}
		for slot := 0; slot < 128; {
			o := chunk.owners[slot]
			if o.fragment == nil {
				slot++
				continue
			}
			count := 1
			for slot+count < 128 {
				next := chunk.owners[slot+count]
				if next.fragment != o.fragment || int(next.ordinal) != int(o.ordinal)+count {
					break
				}
				count++
			}
			result = append(result, splicePart{*o.fragment, index*128 + slot, int(o.ordinal), count})
			for i := 0; i < count; i++ {
				include(int64(chunk.owners[slot+i].timestamp))
			}
			slot += count
		}
	}
	return result, base, intersects, nil
}

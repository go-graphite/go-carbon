package whisperio

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// PrependArchives keeps the target's classic groups verbatim, changing only
// groups receiving older points. Timestamp separation alone is insufficient:
// an older point aliasing an occupied ring slot must lose to the newer target.
// Unsupported or noncanonical images decline before touching dst.
func (c *ContinuationCodec) PrependArchives(ctx context.Context, incoming, target, dst store.ContinuationIO, source store.Snapshot, m store.Metadata, clock func() time.Time) (string, []int, bool, error) {
	// A classic source already has a cheap in-place merge; preserving its
	// incoming history is cheaper than rewriting it into the target's ring.
	d, err := newContinuationFiles(incoming, true).load("main.wsp")
	if err != nil {
		return "", nil, false, err
	}
	if !d.layout.Compressed {
		return "", nil, false, nil
	}
	return c.prependClassicTarget(ctx, target, dst, source, m, clock)
}

func (c *ContinuationCodec) prependClassicTarget(ctx context.Context, target, dst store.ContinuationIO, source store.Snapshot, m store.Metadata, clock func() time.Time) (string, []int, bool, error) {
	decline := func(err error) (string, []int, bool, error) { return "", nil, false, err }
	if c.keepPrependArchives {
		return decline(nil)
	}
	overlay := newContinuationOverlay(target)
	f, _, err := openWindowClassic(overlay, m, clock)
	if err != nil {
		return decline(err)
	}
	if f == nil || f.data.layout.Compressed || f.data.references || len(source.Archives) != len(m.Retentions) {
		return decline(nil)
	}
	f.fs.readOnly = false // all mutations remain private until every archive qualifies
	h := sha256.New()
	counts := make([]int, len(m.Retentions))
	var wire [16]byte
	for ai, r := range m.Retentions {
		if err := ctx.Err(); err != nil {
			return decline(err)
		}
		a := f.data.layout.Archives[ai]
		incoming := source.Archives[ai]
		if incoming.Retention != r {
			return decline(nil)
		}
		step, count := int64(r.Step), int64(r.Count)
		if step <= 0 || count <= 0 {
			return decline(nil)
		}
		var previous int64
		for _, p := range incoming.Points {
			if p.Timestamp <= previous || p.Timestamp > math.MaxUint32 || p.Timestamp%step != 0 || math.IsNaN(p.Value) {
				return decline(nil)
			}
			previous = p.Timestamp
		}
		if len(incoming.Points) > 0 && (previous-incoming.Points[0].Timestamp)/step >= count {
			return decline(nil)
		}
		groups := make([]*store.PointBlock, (r.Count+127)/128)
		var base, low, high int64
		firstSlot := 0
		for gi := range groups {
			if err := ctx.Err(); err != nil {
				return decline(err)
			}
			value, err := overlay.Get(continuationBodyKey(f.data.id, 'p', ai, 0, gi))
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			if err != nil {
				return decline(err)
			}
			block := new(store.PointBlock)
			err = store.DecodePointBlock(value, block)
			if err != nil {
				return decline(err)
			}
			groups[gi] = block
			allocated := f.classicGroupAllocated(a, gi)
			for j := 0; j < min(128, r.Count-gi*128); j++ {
				if block.Present[j/64]&(uint64(1)<<uint(j%64)) == 0 {
					continue
				}
				p := block.Points[j]
				if !allocated || p.Timestamp <= 0 || p.Timestamp > math.MaxUint32 || p.Timestamp%step != 0 || math.IsNaN(p.Value) {
					return decline(nil)
				}
				if gi == 0 && j == 0 {
					base = p.Timestamp
				}
				if low == 0 || p.Timestamp < low {
					low = p.Timestamp
					firstSlot = gi*128 + j
				}
				high = max(high, p.Timestamp)
				counts[ai]++
			}
		}
		if low != 0 && (base == 0 || (high-low)/step >= count || len(incoming.Points) > 0 && previous >= low) {
			return decline(nil)
		}
		if low == 0 && len(incoming.Points) > 0 {
			base = incoming.Points[0].Timestamp
		}
		// Validate the target's addressing before preserving any of its groups.
		for gi, block := range groups {
			if block == nil {
				continue
			}
			for j := 0; j < min(128, r.Count-gi*128); j++ {
				if block.Present[j/64]&(uint64(1)<<uint(j%64)) != 0 {
					slot := ((block.Points[j].Timestamp-base)/step%count + count) % count
					if slot != int64(gi*128+j) {
						return decline(nil)
					}
				}
			}
		}
		kept := make([]store.Point, 0, len(incoming.Points))
		dirty := make([]bool, len(groups))
		for _, p := range incoming.Points {
			slot := int(((p.Timestamp-base)/step%count + count) % count)
			block := groups[slot/128]
			if block == nil {
				block = new(store.PointBlock)
				groups[slot/128] = block
			}
			bit := uint64(1) << uint(slot%64)
			if block.Present[(slot%128)/64]&bit != 0 {
				continue
			}
			block.Present[(slot%128)/64] |= bit
			block.Points[slot%128] = p
			dirty[slot/128] = true
			kept = append(kept, p)
			counts[ai]++
			f.allocateClassicPages(a.Offset+slot*12, a.Offset+slot*12+12)
		}
		binary.BigEndian.PutUint64(wire[:8], uint64(counts[ai]))
		h.Write(wire[:8])
		hashPoint := func(p store.Point) {
			binary.BigEndian.PutUint64(wire[:8], uint64(p.Timestamp))
			binary.BigEndian.PutUint64(wire[8:], math.Float64bits(p.Value))
			h.Write(wire[:])
		}
		for _, p := range kept {
			hashPoint(p)
		}
		// Original target points are a single circular chronological run. Newly
		// prepended points have already been hashed and are skipped here.
		previous = 0
		if low != 0 {
			for n := 0; n < r.Count; n++ {
				slot := (firstSlot + n) % r.Count
				block := groups[slot/128]
				if block == nil {
					continue
				}
				if block.Present[(slot%128)/64]&(uint64(1)<<uint(slot%64)) == 0 {
					continue
				}
				p := block.Points[slot%128]
				if p.Timestamp < low {
					continue
				}
				if p.Timestamp <= previous {
					return decline(nil)
				}
				previous = p.Timestamp
				hashPoint(p)
			}
		}
		for gi, changed := range dirty {
			if changed {
				if err := f.setPointBlock(continuationBodyKey(f.data.id, 'p', ai, 0, gi), groups[gi]); err != nil {
					return decline(err)
				}
			}
		}
	}
	// The optional seek index is invalidated; ordinary writes can rebuild it.
	f.data.windowIndex = nil
	f.data.windowDirty = true
	f.data.windowLoaded = true
	if err := f.fs.flush(); err != nil {
		return decline(err)
	}
	// dst initially contains the incoming staged image. Replace its namespace
	// with the preserved target plus the private overlay's touched groups.
	if err := dst.DeletePrefix(nil); err != nil {
		return decline(err)
	}
	if err := target.Visit(nil, func(k, v []byte) error { return dst.Set(k, v) }); err != nil {
		return decline(err)
	}
	if err := overlay.applyTo(dst); err != nil {
		return decline(err)
	}
	return hex.EncodeToString(h.Sum(nil)), counts, true, nil
}

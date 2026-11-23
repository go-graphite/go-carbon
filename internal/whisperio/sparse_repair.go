package whisperio

import (
	"bytes"
	"context"
	"math"
	"slices"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// PlanSparseRepair shares native window indexing, classicGroup and
// setPointBlock with native window patches. Its contract is ordinary fill:
// target ties win, and distinct circular epochs use PreferArchiveTimestamp.
func (c *ContinuationCodec) PlanSparseRepair(ctx context.Context, kv store.ContinuationIO, m store.Metadata, p store.SparseRepair, clock func() time.Time) (store.SparseRepairPlan, error) {
	unsupported := func(reason string) (store.SparseRepairPlan, error) {
		return store.SparseRepairPlan{}, &store.SparseRepairUnsupported{Reason: reason}
	}
	if err := p.Validate(); err != nil {
		return store.SparseRepairPlan{}, err
	}
	f, ok, err := openWindowClassic(kv, m, clock)
	if err != nil {
		return store.SparseRepairPlan{}, err
	}
	if !ok {
		return unsupported("classic window index, geometry or sidecars")
	}
	if f.data.references {
		return unsupported("referenced aliases")
	}
	// Ordinary import normalizes the ENTIRE target. The revision-bound
	// certificate lets us prove that no unrelated group needs normalization.
	if !f.data.windowIndex.repairSafe {
		return unsupported("target normalization certificate")
	}
	if len(f.data.layout.Archives) != len(m.Retentions) {
		return unsupported("archive geometry")
	}
	for i, a := range f.data.layout.Archives {
		if a.Step != m.Retentions[i].Step || a.Count != m.Retentions[i].Count {
			return unsupported("archive geometry")
		}
	}
	overlay := newContinuationOverlay(kv)
	f.fs.kv, f.fs.readOnly = overlay, false
	changed := false
	for _, archive := range p.Archives {
		ai := archive.Archive
		if ai >= len(m.Retentions) || m.Retentions[ai] != archive.Retention {
			return unsupported("archive geometry")
		}
		a := f.data.layout.Archives[ai]
		base := f.data.windowIndex.archives[ai].base
		groups := make(map[int]*store.PointBlock)
		dirty := make(map[int]bool)
		for _, incoming := range archive.Points {
			if err := ctx.Err(); err != nil {
				return store.SparseRepairPlan{}, err
			}
			value := math.Float64frombits(incoming.Bits)
			if math.IsNaN(value) {
				continue
			} // same normalization as ImportTransfer
			if base == 0 {
				base = incoming.Timestamp
				f.data.windowIndex.archives[ai].base = base
			}
			slot := int(((incoming.Timestamp-base)/int64(a.Step)%int64(a.Count) + int64(a.Count)) % int64(a.Count))
			group, i := slot/128, slot%128
			block := groups[group]
			if block == nil {
				block, err = f.classicGroup(ai, a, group, true)
				if err != nil {
					return store.SparseRepairPlan{}, err
				}
				groups[group] = block
			}
			if block.Present[i/64]&(1<<uint(i%64)) != 0 {
				old := block.Points[i]
				if old.Timestamp == incoming.Timestamp || !store.PreferArchiveTimestamp(incoming.Timestamp, old.Timestamp, archive.Retention, clock().Unix()) {
					continue
				}
			}
			// The certificate originates in a fresh canonical image. Known
			// writers only allocate pages; they never hide occupied cells.
			// Newly allocated pages therefore cannot expose unrelated points.
			block.Present[i/64] |= 1 << uint(i%64)
			block.Points[i] = store.Point{Timestamp: incoming.Timestamp, Value: value}
			dirty[group], changed = true, true
			f.allocateClassicPages(a.Offset+slot*12, a.Offset+slot*12+12)
		}
		for group := range dirty {
			if err := f.setPointBlock(continuationBodyKey(f.data.id, 'p', ai, 0, group), groups[group]); err != nil {
				return store.SparseRepairPlan{}, err
			}
		}
	}
	if !changed {
		return store.SparseRepairPlan{}, nil
	}
	f.data.windowDirty = true
	if err := f.fs.flush(); err != nil {
		return store.SparseRepairPlan{}, err
	}
	if len(overlay.prefixes) != 0 || len(overlay.deleted) != 0 {
		return unsupported("structural rewrite")
	}
	plan := store.SparseRepairPlan{Writes: make([]store.RelayoutKV, 0, len(overlay.values))}
	for k, v := range overlay.values {
		plan.Writes = append(plan.Writes, store.RelayoutKV{Key: []byte(k), Value: v})
	}
	slices.SortFunc(plan.Writes, func(a, b store.RelayoutKV) int { return bytes.Compare(a.Key, b.Key) })
	return plan, nil
}

var _ store.ContinuationSparseRepairer = (*ContinuationCodec)(nil)

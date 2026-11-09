package whisperio

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// PlanArchiveAnchors rotates only inconsistent classic main-file archives.
// Compressed files and OOO sidecars retain their bytes and writer state. No
// retention clipping, timestamp alignment, float conversion or propagation is
// permitted: every present point must fit exactly or the metric is blocked.
func (c *ContinuationCodec) PlanArchiveAnchors(ctx context.Context, kv store.ContinuationIO) (store.AnchorRepairPlan, error) {
	plan := store.AnchorRepairPlan{}
	overlay := newContinuationOverlay(kv)
	fs := newContinuationFiles(overlay, false)
	file, err := fs.OpenFile(fs.main(), os.O_RDWR, 0600)
	if err != nil {
		return plan, err
	}
	defer file.Close()
	f := file.(*continuationFile)
	if f.data.layout.Compressed {
		return plan, nil
	}
	for ai, a := range f.data.layout.Archives {
		if err := ctx.Err(); err != nil {
			return plan, err
		}
		status := store.AnchorArchive{Index: ai}
		old := make(map[int]*store.PointBlock)
		positions := make(map[int]store.Point)
		prefix := continuationBlockPrefix(f.data.id, 'p', ai, 0)
		err := kv.Visit(prefix, func(key, value []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if len(key) != len(prefix)+4 {
				return fmt.Errorf("invalid classic group key")
			}
			index := int(binary.BigEndian.Uint32(key[len(prefix):]))
			var block store.PointBlock
			if err := store.DecodePointBlock(value, &block); err != nil {
				return err
			}
			old[index] = &block
			for offset, p := range block.Points {
				if block.Present[offset/64]&(uint64(1)<<uint(offset%64)) == 0 {
					continue
				}
				slot := index*128 + offset
				if slot >= a.Count {
					return fmt.Errorf("classic point outside archive")
				}
				positions[slot] = p
			}
			return nil
		})
		if err != nil {
			return plan, err
		}
		status.Points = len(positions)
		if len(positions) == 0 {
			plan.Archives = append(plan.Archives, status)
			continue
		}
		base := positions[0].Timestamp
		minimum := int64(math.MaxInt64)
		seen := make(map[int64]bool, len(positions))
		for slot, p := range positions {
			minimum = min(minimum, p.Timestamp)
			if p.Timestamp <= 0 || p.Timestamp > math.MaxUint32 || p.Timestamp%int64(a.Step) != 0 {
				status.Blocked = "off-grid or invalid timestamp"
				continue
			}
			absolute := p.Timestamp / int64(a.Step) % int64(a.Count)
			if seen[absolute] {
				status.Blocked = "distinct physical points alias one ring slot"
			}
			seen[absolute] = true
			relative := ((p.Timestamp-base)/int64(a.Step)%int64(a.Count) + int64(a.Count)) % int64(a.Count)
			if base == 0 || int64(slot) != relative {
				status.Misplaced++
			}
		}
		if status.Blocked != "" || status.Misplaced == 0 {
			plan.Archives = append(plan.Archives, status)
			continue
		}
		fresh := make(map[int]*store.PointBlock)
		for _, p := range positions {
			slot := int((p.Timestamp - minimum) / int64(a.Step) % int64(a.Count))
			block := fresh[slot/128]
			if block == nil {
				block = &store.PointBlock{}
				fresh[slot/128] = block
			}
			offset := slot % 128
			block.Present[offset/64] |= uint64(1) << uint(offset%64)
			block.Points[offset] = p
			f.allocateClassicPages(a.Offset+slot*12, a.Offset+slot*12+12)
		}
		// This compares the complete physical point multiset, including NaN
		// payloads and signed zeros, before any rewrite can reach the caller.
		flatten := func(groups map[int]*store.PointBlock) []store.Point {
			var out []store.Point
			for _, block := range groups {
				for i, p := range block.Points {
					if block.Present[i/64]&(uint64(1)<<uint(i%64)) != 0 {
						out = append(out, p)
					}
				}
			}
			slices.SortFunc(out, func(a, b store.Point) int {
				if a.Timestamp < b.Timestamp {
					return -1
				}
				if a.Timestamp > b.Timestamp {
					return 1
				}
				return 0
			})
			return out
		}
		before, after := flatten(old), flatten(fresh)
		if len(before) != len(after) {
			return plan, fmt.Errorf("anchor rewrite lost physical points")
		}
		for i, p := range before {
			if p.Timestamp != after[i].Timestamp || math.Float64bits(p.Value) != math.Float64bits(after[i].Value) {
				return plan, fmt.Errorf("anchor rewrite changed point bits")
			}
		}
		for index, block := range old {
			replacement := fresh[index]
			if replacement == nil {
				if err := overlay.Delete(continuationBodyKey(f.data.id, 'p', ai, 0, index)); err != nil {
					return plan, err
				}
				continue
			}
			if bytes.Equal(store.EncodePointBlock(block), store.EncodePointBlock(replacement)) {
				delete(fresh, index)
			}
		}
		for index, block := range fresh {
			if err := f.setPointBlock(continuationBodyKey(f.data.id, 'p', ai, 0, index), block); err != nil {
				return plan, err
			}
		}
		status.Changed = true
		plan.Archives = append(plan.Archives, status)
	}
	// An ambiguous archive blocks the entire metric, including already prepared
	// sibling rotations. Dry-run still reports all archive classifications.
	for _, a := range plan.Archives {
		if a.Blocked != "" {
			return plan, nil
		}
	}
	if err := fs.flush(); err != nil {
		return plan, err
	}
	if len(overlay.prefixes) != 0 {
		return plan, fmt.Errorf("unexpected anchor prefix deletion")
	}
	for k, v := range overlay.values {
		old, err := kv.Get([]byte(k))
		if err == nil && bytes.Equal(old, v) {
			continue
		}
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return plan, err
		}
		plan.Writes = append(plan.Writes, store.RelayoutKV{Key: []byte(k), Value: v})
	}
	for k := range overlay.deleted {
		plan.Deletes = append(plan.Deletes, []byte(k))
	}
	slices.SortFunc(plan.Writes, func(a, b store.RelayoutKV) int { return bytes.Compare(a.Key, b.Key) })
	slices.SortFunc(plan.Deletes, bytes.Compare)
	return plan, nil
}

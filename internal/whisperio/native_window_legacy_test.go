package whisperio

import (
	"context"
	"fmt"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"math"
	"time"
)

// Frozen pre-fix oracle from upstream bfe8cd566. Keep independent of the production guard
// so non-aliasing behavior is compared byte-for-byte on identical inputs.
func (c *ContinuationCodec) legacyPlanNativeWindowPatch(ctx context.Context, kv store.ContinuationIO, source store.Snapshot, clock func() time.Time, window store.NativeWindow) ([]store.RelayoutKV, bool, error) {
	f, ok, err := openWindowClassic(kv, source.Metadata, clock)
	if err != nil || !ok {
		return nil, false, err
	}
	if len(f.data.layout.Archives) != len(source.Archives) {
		return nil, false, store.ErrFormat
	}
	overlay := newContinuationOverlay(kv)
	f.fs.kv, f.fs.readOnly = overlay, false
	for ai, archive := range source.Archives {
		a := f.data.layout.Archives[ai]
		if a.Step != archive.Retention.Step || a.Count != archive.Retention.Count {
			return nil, false, store.ErrFormat
		}
		base := f.data.windowIndex.archives[ai].base
		if base == 0 {
			if len(archive.Points) == 0 {
				continue
			}
			base = archive.Points[0].Timestamp
			f.data.windowIndex.archives[ai].base = base
		}
		groups := make(map[int]*store.PointBlock)
		// The reference union removes target NaNs inside the window, including
		// timestamps absent from the donor. Outside NaN payloads remain exact.
		targetPoints, err := classicWindowPoints(ctx, f, ai, window.From, window.Until)
		if err != nil {
			return nil, false, err
		}
		for _, p := range targetPoints {
			if !math.IsNaN(p.Value) {
				continue
			}
			slot := int(((p.Timestamp-base)/int64(a.Step)%int64(a.Count) + int64(a.Count)) % int64(a.Count))
			block := groups[slot/128]
			if block == nil {
				block, err = f.classicGroup(ai, a, slot/128, true)
				if err != nil {
					return nil, false, err
				}
				groups[slot/128] = block
			}
			block.Present[slot%128/64] &^= 1 << uint(slot%64)
			block.Points[slot%128] = store.Point{}
		}
		for _, p := range archive.Points {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
			if !window.Contains(p.Timestamp) || p.Timestamp%int64(a.Step) != 0 || math.IsNaN(p.Value) {
				return nil, false, nil
			}
			slot := int(((p.Timestamp-base)/int64(a.Step)%int64(a.Count) + int64(a.Count)) % int64(a.Count))
			block := groups[slot/128]
			if block == nil {
				block, err = f.classicGroup(ai, a, slot/128, true)
				if err != nil {
					return nil, false, err
				}
				groups[slot/128] = block
			}
			i := slot % 128
			if block.Present[i/64]&(1<<uint(i%64)) != 0 {
				old := block.Points[i]
				if !window.Contains(old.Timestamp) {
					if p.Timestamp > old.Timestamp {
						return nil, false, fmt.Errorf("window would evict out-of-window point: %w", store.ErrConflict)
					}
					continue
				}
				if old.Timestamp > p.Timestamp || old.Timestamp == p.Timestamp && !math.IsNaN(old.Value) {
					continue
				}
			}
			block.Present[i/64] |= 1 << uint(i%64)
			block.Points[i] = p
			f.allocateClassicPages(a.Offset+slot*12, a.Offset+slot*12+12)
		}
		for group, block := range groups {
			if err := f.setPointBlock(continuationBodyKey(f.data.id, 'p', ai, 0, group), block); err != nil {
				return nil, false, err
			}
		}
	}
	f.data.windowDirty = true // Publication advances the owning revision even for an empty patch.
	if err := f.fs.flush(); err != nil {
		return nil, false, err
	}
	if len(overlay.prefixes) != 0 {
		return nil, false, nil
	}
	writes := make([]store.RelayoutKV, 0, len(overlay.values))
	for key, value := range overlay.values {
		writes = append(writes, store.RelayoutKV{Key: []byte(key), Value: value})
	}
	for key := range overlay.deleted {
		writes = append(writes, store.RelayoutKV{Key: []byte(key)})
	}
	return writes, true, nil
}

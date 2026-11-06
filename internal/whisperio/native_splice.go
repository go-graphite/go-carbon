package whisperio

import (
	"context"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type splicePayload struct {
	data   []byte
	sealed bool
	points []store.Point
}
type spliceFragment struct {
	first                              int64
	ordinal, count, priority, sequence int
	payload                            *splicePayload
	value                              float64
	irregular                          bool
}
type splicePart struct {
	fragment             spliceFragment
	slot, ordinal, count int
}

// CombineNative keeps encoded history groups under classic addressing. Coarse
// archives stay independently authoritative, without propagation buffers that
// could reinterpret their union after publication.
func (c *ContinuationCodec) CombineNative(ctx context.Context, donor, survivor store.ContinuationIO, m store.Metadata, clock func() time.Time) ([]store.RelayoutKV, store.NativeCombineClass, bool, error) {
	class := store.NativeCombineClass{Archives: len(m.Retentions)}
	a, ok, err := c.nativeFragments(ctx, donor, m, clock, 0)
	if err != nil || !ok {
		return nil, class, false, err
	}
	b, ok, err := c.nativeFragments(ctx, survivor, m, clock, 2)
	if err != nil || !ok {
		return nil, class, false, err
	}
	selected := make([][]splicePart, len(a))
	bases := make([]int64, len(a))
	for ai, r := range m.Retentions {
		var intersects bool
		selected[ai], bases[ai], intersects, err = selectSpliceOwners(ctx, a[ai], b[ai], r)
		if err != nil {
			return nil, class, false, err
		}
		if intersects {
			class.IntersectingArchives++
		}
	}
	keys, err := c.writeReferencedArchives(ctx, m, selected, bases, &class, clock)
	if err != nil {
		return nil, class, false, err
	}
	return keys, class, class.CopiedBlocks > 0, nil
}

func spliceWordMask(first, count int) uint64 {
	if count == 64 {
		return ^uint64(0)
	}
	return ((uint64(1) << uint(count)) - 1) << uint(first)
}
func spliceMask(first, count int) (result [2]uint64) {
	for count > 0 {
		word := first / 64
		n := min(count, 64-first%64)
		result[word] = spliceWordMask(first%64, n)
		first += n
		count -= n
	}
	return
}

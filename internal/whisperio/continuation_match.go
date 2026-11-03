package whisperio

import (
	"errors"
	"fmt"
	"math"
	"sort"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// decodeStorageGroup validates one physical group. previous validates adjacency
// during a full scan; point probes pass zero and validate the selected group.
func (f *continuationFile) decodeStorageGroup(block whisper.ContinuationBlock, desc blockDescriptor, first int, value []byte, scratch *store.PointBlock, sealed *store.SealedPointBuffer, previous int64) ([]store.Point, error) {
	var decoded []store.Point
	if desc.packed > 0 {
		var err error
		decoded, err = sealed.Decode(value)
		if err != nil {
			return nil, err
		}
		if len(decoded) != min(desc.groupSize(), desc.count-first) {
			return nil, errors.New("sealed group count differs")
		}
	} else {
		if err := store.DecodePointBlock(value, scratch); err != nil {
			return nil, err
		}
		decoded = scratch.Points[:min(128, desc.count-first)]
	}
	for slot, p := range decoded {
		present := desc.packed > 0 || scratch.Present[slot/64]&(uint64(1)<<uint(slot%64)) != 0
		if !present || p.Timestamp <= previous || p.Timestamp > math.MaxUint32 {
			return nil, fmt.Errorf("invalid continuation physical point: file %d archive %d block %d (count %d, step %d): point %d present %v at %d follows %d", f.data.id, block.Archive, block.Index, desc.count, block.Step, first+slot, present, p.Timestamp, previous)
		}
		previous = p.Timestamp
	}
	return decoded, nil
}

// MatchesStoragePoints probes candidate groups and their boundary successors.
// Exact bits in unambiguous archive slots prove a no-op; a persisted maximum
// timestamp never proves that an older correction or hole can be discarded.
func (f *continuationFile) MatchesStoragePoints(block whisper.ContinuationBlock, input []whisper.TimeSeriesPoint) (bool, error) {
	if f.fs.keepDuplicatePlanning || len(input) == 0 {
		return false, nil
	}
	if err := f.validateBlock(block); err != nil {
		return false, err
	}
	if block.First%block.Step != 0 || block.Last%block.Step != 0 {
		return false, nil
	}
	desc, err := f.descriptor(block.Archive, block.Index)
	if err != nil {
		return false, err
	}
	if desc.count != block.Count || desc.count == 0 || desc.count > 4*block.Size {
		return false, nil
	}
	size := desc.groupSize()
	groups := (desc.count + size - 1) / size
	var scratch store.PointBlock
	var sealed *store.SealedPointBuffer
	if desc.packed > 0 {
		sealed = getSealedPointBuffer()
		defer putSealedPointBuffer(sealed)
	}
	var points []store.Point
	cached, checked := -1, -1
	read := func(index int) ([]store.Point, error) {
		if index == cached {
			return points, nil
		}
		value, err := f.fs.kv.Get(desc.groupKey(f.data.id, block.Archive, block.Index, index))
		if err != nil {
			return nil, err
		}
		points, err = f.decodeStorageGroup(block, desc, index*size, value, &scratch, sealed, 0)
		if err != nil {
			return nil, err
		}
		cached = index
		return points, nil
	}
	for i, p := range input {
		if p.Time < block.First || p.Time > block.Last || p.Time%block.Step != 0 || i > 0 && p.Time <= input[i-1].Time {
			return false, nil
		}
		if cached < 0 || int64(p.Time) < points[0].Timestamp || int64(p.Time) > points[len(points)-1].Timestamp {
			lo, hi := 0, groups
			// Dense blocks can address the group without a search. For sparse
			// histories, binary search uses group bounds, not timestamp assumptions.
			if block.Last-block.First == (desc.count-1)*block.Step {
				lo = (p.Time - block.First) / block.Step / size
				hi = lo + 1
			}
			for lo < hi {
				mid := lo + (hi-lo)/2
				decoded, err := read(mid)
				if errors.Is(err, store.ErrNotFound) {
					return false, nil
				}
				if err != nil {
					return false, err
				}
				if decoded[len(decoded)-1].Timestamp < int64(p.Time) {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
			if lo >= groups {
				return false, nil
			}
			if _, err := read(lo); errors.Is(err, store.ErrNotFound) {
				return false, nil
			} else if err != nil {
				return false, err
			}
		}
		at := sort.Search(len(points), func(i int) bool { return points[i].Timestamp >= int64(p.Time) })
		if at == len(points) || points[at].Timestamp != int64(p.Time) || math.Float64bits(points[at].Value) != math.Float64bits(p.Value) {
			return false, nil
		}
		if checked != cached {
			for _, point := range points {
				if point.Timestamp%int64(block.Step) != 0 {
					return false, nil
				}
			}
			checked = cached
		}
		// A converted off-grid successor can occupy the same logical slot.
		// Probe across a group boundary too; raw equality alone is insufficient.
		if block.Step > 1 && at == len(points)-1 && cached+1 < groups {
			if _, err := read(cached + 1); errors.Is(err, store.ErrNotFound) {
				return false, nil
			} else if err != nil {
				return false, err
			}
			if points[0].Timestamp < int64(p.Time+block.Step) {
				return false, nil
			}
		}
	}
	return true, nil
}

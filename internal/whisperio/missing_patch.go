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
	"strconv"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// PlanMissingPatch edits classic physical slots directly, without propagation,
// ring rotation, eviction or anchor repair. Every other physical point remains.
func (c *ContinuationCodec) PlanMissingPatch(ctx context.Context, kv store.ContinuationIO, p store.MissingPatch, now int64) ([]store.RelayoutKV, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	anchors, err := c.PlanArchiveAnchors(ctx, kv)
	if err != nil {
		return nil, err
	}
	for _, a := range anchors.Archives {
		if a.Blocked != "" || a.Changed || a.DuplicatePoints != 0 {
			return nil, fmt.Errorf("MissingOnly requires healthy unambiguous archive anchors")
		}
	}
	if err := kv.Visit([]byte("n/"), func(k, v []byte) error {
		if string(k) != "n/main.wsp" {
			return store.ErrFormat
		}
		return nil
	}); err != nil {
		return nil, err
	}
	overlay := newContinuationOverlay(kv)
	fs := newContinuationFiles(overlay, false)
	file, err := fs.OpenFile(fs.main(), os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	f := file.(*continuationFile)
	if f.data.layout.Compressed || f.data.references {
		return nil, store.ErrFormat
	}
	type archiveState struct {
		positions map[int]store.Point
		groups    map[int]*store.PointBlock
		base      int64
	}
	states := map[int]*archiveState{}
	for _, point := range p.Points {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if point.Archive >= len(f.data.layout.Archives) {
			return nil, store.ErrFormat
		}
		a := f.data.layout.Archives[point.Archive]
		if a.Step != point.Step || a.Count != point.Count || point.Timestamp <= now-int64(a.Step)*int64(a.Count) || point.Timestamp > now {
			return nil, fmt.Errorf("MissingOnly archive geometry/retention differs")
		}
		s := states[point.Archive]
		if s == nil {
			s = &archiveState{positions: map[int]store.Point{}, groups: map[int]*store.PointBlock{}}
			prefix := continuationBlockPrefix(f.data.id, 'p', point.Archive, 0)
			err = overlay.Visit(prefix, func(k, v []byte) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				if len(k) != len(prefix)+4 {
					return store.ErrFormat
				}
				index := int(binary.BigEndian.Uint32(k[len(prefix):]))
				var block store.PointBlock
				if err := store.DecodePointBlock(v, &block); err != nil {
					return err
				}
				s.groups[index] = &block
				for i, x := range block.Points {
					if block.Present[i/64]&(uint64(1)<<uint(i%64)) != 0 {
						s.positions[index*128+i] = x
					}
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
			s.base = s.positions[0].Timestamp
			if len(s.positions) == 0 {
				s.base = point.Timestamp
			}
			if s.base == 0 {
				return nil, store.ErrConflict
			}
			states[point.Archive] = s
		}
		slot := int(((point.Timestamp-s.base)/int64(a.Step)%int64(a.Count) + int64(a.Count)) % int64(a.Count))
		previous, present := s.positions[slot]
		if point.BeforeBits == "" {
			// Even an expired or NaN cell with a different timestamp is another
			// physical point. Never evict it to make room for the requested gap.
			if present {
				return nil, fmt.Errorf("MissingOnly absent slot occupied: %w", store.ErrConflict)
			}
		} else {
			bits, _ := strconv.ParseUint(point.BeforeBits, 16, 64)
			if !present || previous.Timestamp != point.Timestamp || !math.IsNaN(previous.Value) || math.Float64bits(previous.Value) != bits {
				return nil, fmt.Errorf("MissingOnly preimage differs: %w", store.ErrConflict)
			}
		}
		bits, _ := strconv.ParseUint(point.DonorBits, 16, 64)
		block := s.groups[slot/128]
		if block == nil {
			block = &store.PointBlock{}
			s.groups[slot/128] = block
		}
		offset := slot % 128
		value := store.Point{Timestamp: point.Timestamp, Value: math.Float64frombits(bits)}
		block.Points[offset] = value
		block.Present[offset/64] |= uint64(1) << uint(offset%64)
		s.positions[slot] = value
		f.allocateClassicPages(a.Offset+slot*12, a.Offset+slot*12+12)
		if err := f.setPointBlock(continuationBodyKey(f.data.id, 'p', point.Archive, 0, slot/128), block); err != nil {
			return nil, err
		}
	}
	if err := fs.flush(); err != nil {
		return nil, err
	}
	if len(overlay.deleted) != 0 || len(overlay.prefixes) != 0 {
		return nil, store.ErrConflict
	}
	var writes []store.RelayoutKV
	for k, v := range overlay.values {
		old, err := kv.Get([]byte(k))
		if err == nil && bytes.Equal(old, v) {
			continue
		}
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
		writes = append(writes, store.RelayoutKV{Key: []byte(k), Value: v})
	}
	slices.SortFunc(writes, func(a, b store.RelayoutKV) int { return bytes.Compare(a.Key, b.Key) })
	return writes, nil
}

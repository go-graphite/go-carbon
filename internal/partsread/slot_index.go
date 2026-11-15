package partsread

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"sort"
)

// SlotBlock describes a populated immutable block. Slots is a conservative
// physical-slot span, including every retained version, rather than a timestamp
// interval. Key is the complete storage identity: disk sources include placement
// group, routing vshard, canonical series identity and archive in that key.
type SlotBlock struct {
	ID      uint64
	Archive Archive
	Slots   SlotRange
	Key     []byte
}

// ReadSlotBlock reads one exact block identity. It may use ranges to bound disk
// iterators or decoding, but must retain every version in those physical slots.
// Key and ranges are borrowed, read-only, and valid only during the call.
type ReadSlotBlock func(context.Context, []byte, []SlotRange, func(Sample) error) error

// SlotIndex pins a block directory alongside the same generation's immutable
// snapshot. A query searches this in-memory directory, never an on-disk series
// index. Sources must publish a complete directory atomically with their data;
// rebuilding it lazily during a read defeats this contract.
type SlotIndex struct {
	blocks []SlotBlock
	read   ReadSlotBlock
	bytes  int64
}

func NewSlotIndex(blocks []SlotBlock, read ReadSlotBlock) (*SlotIndex, error) {
	if read == nil {
		return nil, errors.New("nil immutable block reader")
	}
	s := &SlotIndex{blocks: slices.Clone(blocks), read: read}
	s.bytes = 64 + int64(cap(s.blocks))*64
	for i := range s.blocks {
		b := &s.blocks[i]
		if b.Archive.Step <= 0 || b.Archive.Points <= 0 || b.Slots.From < 0 || b.Slots.From >= b.Slots.Until || b.Slots.Until > b.Archive.Points || len(b.Key) == 0 {
			return nil, errors.New("invalid immutable block directory entry")
		}
		b.Key = slices.Clone(b.Key)
		s.bytes += int64(cap(b.Key))
	}
	slices.SortFunc(s.blocks, func(a, b SlotBlock) int {
		return cmp.Or(cmp.Compare(a.ID, b.ID), cmp.Compare(a.Archive.Step, b.Archive.Step), cmp.Compare(a.Archive.Points, b.Archive.Points), cmp.Compare(a.Slots.From, b.Slots.From))
	})
	return s, nil
}

func (s *SlotIndex) RetainedBytes() int64 { return s.bytes }

func (s *SlotIndex) first(id uint64, step int64) int {
	return sort.Search(len(s.blocks), func(i int) bool {
		b := &s.blocks[i]
		return b.ID > id || b.ID == id && b.Archive.Step >= step
	})
}

func (s *SlotIndex) HasArchive(ctx context.Context, id uint64, step int64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	i := s.first(id, step)
	return i < len(s.blocks) && s.blocks[i].ID == id && s.blocks[i].Archive.Step == step, nil
}

func (s *SlotIndex) Scan(ctx context.Context, id uint64, a Archive, from, until int64, yield func(Sample) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ranges, n := SlotRanges(a, from, until)
	for i := s.first(id, a.Step); i < len(s.blocks); i++ {
		b := &s.blocks[i]
		if b.ID != id || b.Archive.Step != a.Step {
			break
		}
		if b.Archive != a {
			continue
		}
		var selected [2]SlotRange
		count := 0
		for _, r := range ranges[:n] {
			lo, hi := max(r.From, b.Slots.From), min(r.Until, b.Slots.Until)
			if lo < hi {
				selected[count] = SlotRange{lo, hi}
				count++
			}
		}
		if count == 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.read(ctx, b.Key, selected[:count], func(p Sample) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			slot := (p.Time / a.Step) % a.Points
			if slot < 0 {
				slot += a.Points
			}
			for _, r := range selected[:count] {
				if slot >= r.From && slot < r.Until {
					return yield(p)
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

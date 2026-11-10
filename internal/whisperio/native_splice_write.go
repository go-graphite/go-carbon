package whisperio

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"path/filepath"
	"slices"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func (c *ContinuationCodec) writeReferencedArchives(ctx context.Context, m store.Metadata, archives [][]splicePart, bases []int64, class *store.NativeCombineClass, clock func() time.Time) ([]store.RelayoutKV, error) {
	output := newContinuationOverlay(emptyContinuationIO{})
	empty := store.Snapshot{Metadata: m, Archives: make([]store.Archive, len(archives))}
	for ai, r := range m.Retentions {
		empty.Archives[ai].Retention = r
	}
	if err := c.RestoreArchives(ctx, output, empty, clock); err != nil {
		return nil, err
	}
	fs := newContinuationFiles(output, false).withClock(clock)
	fdata, err := fs.load(filepath.Base(fs.main()))
	if err != nil {
		return nil, err
	}
	fdata.dirty = true
	f := &continuationFile{fs: fs, data: fdata}
	type group struct {
		parts  []splicePart
		scalar bool
	}
	for ai, parts := range archives {
		r := m.Retentions[ai]
		groups := make(map[int]*group)
		baseSlot := int(bases[ai] / int64(r.Step) % int64(r.Count))
		for _, part := range parts {
			for offset := 0; offset < part.count; {
				destination := (part.slot + offset - baseSlot + r.Count) % r.Count
				count := min(part.count-offset, 128-destination%128, r.Count-destination)
				p := part
				p.slot = destination % 128
				p.ordinal += offset
				p.count = count
				g := groups[destination/128]
				if g == nil {
					g = &group{}
					groups[destination/128] = g
				}
				g.parts = append(g.parts, p)
				g.scalar = g.scalar || p.fragment.payload == nil
				offset += count
			}
		}
		ids := make(map[*splicePayload]uint32)
		uses := make(map[uint32]uint64)
		copied := make(map[*splicePayload]bool)
		countCopied := func(payload *splicePayload) {
			if !copied[payload] {
				copied[payload] = true
				class.CopiedBlocks++
				class.CopiedBytes += uint64(len(payload.data))
			}
		}
		for index, g := range groups {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			start := fdata.layout.Archives[ai].Offset + index*128*12
			f.allocateClassicPages(start, fdata.layout.Archives[ai].Offset+min(r.Count, (index+1)*128)*12)
			slices.SortFunc(g.parts, func(a, b splicePart) int { return cmp.Compare(a.slot, b.slot) })
			if !g.scalar && len(g.parts) > 0 && !g.parts[0].fragment.payload.sealed {
				payload := g.parts[0].fragment.payload
				identity := true
				var present [2]uint64
				for _, part := range g.parts {
					if part.fragment.payload != payload || part.ordinal != part.slot {
						identity = false
						break
					}
					mask := spliceMask(part.slot, part.count)
					present[0] |= mask[0]
					present[1] |= mask[1]
				}
				if identity {
					if !payload.presentKnown {
						var original store.PointBlock
						if err := store.DecodePointBlock(payload.data, &original); err != nil {
							return nil, err
						}
						payload.present, payload.presentKnown = original.Present, true
					}
					if present == payload.present {
						if err := output.Set(continuationBodyKey(fdata.id, 'p', ai, 0, index), payload.data); err != nil {
							return nil, err
						}
						countCopied(payload)
						continue
					}
				}
			}
			// Classic Fetch reads slot zero as its time anchor on every query.
			// Keep that bounded group local so recent reads never decode a cold
			// sealed payload merely to obtain the archive anchor.
			if index == 0 {
				g.scalar = true
			}
			if g.scalar {
				var block store.PointBlock
				for _, part := range g.parts {
					var points []store.Point
					if part.fragment.payload != nil {
						points, err = splicePayloadPoints(part.fragment.payload)
						if err != nil {
							return nil, err
						}
						if part.ordinal+part.count > len(points) {
							return nil, store.ErrFormat
						}
					}
					for i := range part.count {
						p := store.Point{Timestamp: part.fragment.first, Value: part.fragment.value}
						if points != nil {
							p = points[part.ordinal+i]
						}
						slot := part.slot + i
						block.Present[slot/64] |= 1 << uint(slot%64)
						block.Points[slot] = p
					}
				}
				value := store.EncodePointBlock(&block)
				if err := output.Set(continuationBodyKey(fdata.id, 'p', ai, 0, index), value); err != nil {
					return nil, err
				}
				class.CombinedBlocks++
				class.CombinedBytes += uint64(len(value))
				continue
			}
			var refs []pointReference
			for _, part := range g.parts {
				payload := part.fragment.payload
				id := ids[payload]
				if id == 0 {
					id = uint32(len(ids) + 1)
					ids[payload] = id
					if err := output.Set(continuationBodyKey(fdata.id, 'r', ai, 0, int(id)), payload.data); err != nil {
						return nil, err
					}
					countCopied(payload)
				}
				ref := pointReference{ID: id, Source: uint16(part.ordinal), Destination: uint16(part.slot), Count: uint16(part.count), Sealed: payload.sealed}
				if len(refs) > 0 {
					last := &refs[len(refs)-1]
					if last.ID == ref.ID && last.Sealed == ref.Sealed && last.Source+last.Count == ref.Source && last.Destination+last.Count == ref.Destination {
						last.Count += ref.Count
						continue
					}
				}
				refs = append(refs, ref)
			}
			fdata.references = true
			if err := output.Set(continuationBodyKey(fdata.id, 'a', ai, 0, index), encodePointReferences(refs)); err != nil {
				return nil, err
			}
			seen := make(map[uint32]bool, len(refs))
			for _, ref := range refs {
				if !seen[ref.ID] {
					uses[ref.ID]++
					seen[ref.ID] = true
				}
			}
		}
		for id, count := range uses {
			if err := output.Set(continuationBodyKey(fdata.id, 'R', ai, 0, int(id)), binary.BigEndian.AppendUint64(nil, count)); err != nil {
				return nil, err
			}
		}
	}
	if err := fs.flush(); err != nil {
		return nil, err
	}
	var keys []store.RelayoutKV
	if err := output.Visit(nil, func(k, v []byte) error {
		keys = append(keys, store.RelayoutKV{Key: bytes.Clone(k), Value: v})
		return nil
	}); err != nil {
		return nil, err
	}
	return keys, nil
}

package whisperio

import (
	"context"
	"encoding/binary"
	"errors"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// thawBlock restores ordinary groups inside the caller's indexed transaction.
// Late corrections, appends and circular reuse then follow the existing writer.
func (f *continuationFile) thawBlock(block whisper.ContinuationBlock, desc *blockDescriptor) error {
	if desc.packed == 0 {
		return nil
	}
	// The incoming block may describe new bounds/count. Read the old image.
	old := block
	old.Count, old.First, old.Last = 0, 0, 0
	points, err := f.ReadStorageBlock(old)
	if err != nil {
		return err
	}
	if err := f.fs.kv.DeletePrefix(desc.groupPrefix(f.data.id, block.Archive, block.Index)); err != nil {
		return err
	}
	desc.packed = 0
	for first := 0; first < len(points); first += 128 {
		var group store.PointBlock
		for i, p := range points[first:min(len(points), first+128)] {
			group.Present[i/64] |= uint64(1) << uint(i%64)
			group.Points[i] = store.Point{Timestamp: int64(p.Time), Value: p.Value}
		}
		if err := f.setPointBlock(desc.groupKey(f.data.id, block.Archive, block.Index, first/128), &group); err != nil {
			return err
		}
	}
	return f.fs.kv.Set(continuationBlockPrefix(f.data.id, 'b', block.Archive, block.Index), desc.encode())
}

// Repack prepares at most one bounded native range. The ordinal cursor bounds
// descriptor work on metrics with years of history; native layout is preserved.
func (c *ContinuationCodec) Repack(ctx context.Context, kv store.ContinuationIO, r store.ColdPackRequest) (store.RepackStats, uint64, bool, error) {
	var stats store.RepackStats
	// Packing needs layout and file identity, never authoritative header pages.
	// Read only the manifest rather than warming every live rollup buffer.
	identity, err := kv.Get(continuationNameKey("main.wsp"))
	if errors.Is(err, store.ErrNotFound) {
		return stats, 0, true, nil
	}
	if err != nil {
		return stats, 0, false, err
	}
	if len(identity) != 8 {
		return stats, 0, false, errors.New("invalid cold pack file identity")
	}
	id := binary.BigEndian.Uint64(identity)
	manifest, err := kv.Get(continuationManifestKey(id))
	if err != nil {
		return stats, 0, false, err
	}
	d, err := decodeContinuationManifest(manifest)
	if err != nil {
		return stats, 0, false, err
	}
	d.id = id
	if !d.layout.Compressed {
		return stats, 0, true, nil
	}
	fs := newContinuationFiles(kv, false)
	f := &continuationFile{fs: fs, data: d}
	pager, ok := kv.(store.ContinuationPageReader)
	if !ok {
		return stats, 0, false, store.ErrFormat
	}
	prefix := append(continuationFilePrefix(id), 'b')
	start := continuationBlockPrefix(id, 'b', int(r.Cursor>>32), int(uint32(r.Cursor)))
	type candidate struct {
		archive, index int
		desc           blockDescriptor
	}
	var candidates []candidate
	exhausted, err := pager.VisitPage(prefix, start, 32, func(key, value []byte) error {
		if len(key) != 16 {
			return errors.New("invalid cold pack descriptor key")
		}
		archive, index := int(binary.BigEndian.Uint16(key[10:12])), int(binary.BigEndian.Uint32(key[12:]))
		if archive >= len(d.layout.Archives) || index >= d.layout.Archives[archive].BlockCount {
			return errors.New("cold pack descriptor outside native layout")
		}
		desc, err := decodeBlockDescriptor(value)
		if err != nil {
			return err
		}
		candidates = append(candidates, candidate{archive, index, desc})
		return nil
	})
	if err != nil {
		return stats, 0, false, err
	}
	next := r.Cursor
	for _, candidate := range candidates {
		archive, index, desc := candidate.archive, candidate.index, candidate.desc
		a := d.layout.Archives[archive]
		next = uint64(archive)<<32 | uint64(index+1)
		if err := ctx.Err(); err != nil {
			return stats, next, false, err
		}
		eligible := desc.bucketed && desc.count > 128 && desc.packed == 0
		if r.Mode == "unpack" {
			eligible = desc.packed != 0
		}
		if eligible && desc.count > store.MaxColdPackRangePoints {
			stats.Skipped++
			eligible = false
		}
		if eligible {
			block := whisper.ContinuationBlock{Archive: archive, Index: index, Offset: a.Offset + index*a.BlockSize, Size: a.BlockSize, Step: a.Step}
			if r.Mode == "unpack" {
				if err := f.thawBlock(block, &desc); err != nil {
					return stats, next, false, err
				}
				stats.Blocks = 1
				stats.Points = uint64(desc.count)
				return stats, next, false, nil
			}
			points, err := f.ReadStorageBlock(block)
			if err != nil {
				return stats, next, false, err
			}
			if len(points) == 0 {
				return stats, next, false, errors.New("empty cold pack range")
			}
			if int64(points[len(points)-1].Time) < r.Cutoff.Unix() {
				var encoded [][]byte
				for first := 0; first < len(points); first += store.ColdPackPoints {
					input := make([]store.Point, min(store.ColdPackPoints, len(points)-first))
					for i, p := range points[first : first+len(input)] {
						input[i] = store.Point{Timestamp: int64(p.Time), Value: p.Value}
					}
					encode := store.EncodeSealedPoints
					if r.Mode == "decimal" {
						encode = store.EncodeSealedPointsDecimal
					}
					value, err := encode(input, a.Step)
					if err != nil {
						return stats, next, false, err
					}
					encoded = append(encoded, value)
				}
				prefix := desc.groupPrefix(d.id, archive, index)
				var oldBytes, newBytes uint64
				if err := kv.Visit(prefix, func(k, v []byte) error { oldBytes += uint64(len(k) + len(v)); return nil }); err != nil {
					return stats, next, false, err
				}
				for i, v := range encoded {
					newBytes += uint64(len(desc.groupKey(d.id, archive, index, i)) + len(v))
				}
				if newBytes+4 < oldBytes {
					if err := kv.DeletePrefix(prefix); err != nil {
						return stats, next, false, err
					}
					for i, v := range encoded {
						if err := kv.Set(desc.groupKey(d.id, archive, index, i), v); err != nil {
							return stats, next, false, err
						}
					}
					desc.packed = store.ColdPackPoints
					if err := kv.Set(continuationBlockPrefix(d.id, 'b', archive, index), desc.encode()); err != nil {
						return stats, next, false, err
					}
					stats.Blocks = 1
					stats.Points = uint64(len(points))
					stats.InputBytes = oldBytes
					stats.OutputBytes = newBytes + 4
					return stats, next, false, nil
				}
			}
		}
	}
	return stats, next, exhausted, nil
}

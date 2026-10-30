package whisperio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync/atomic"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/stagetime"
	whisper "github.com/go-graphite/go-whisper"
)

func (f *continuationFile) archiveAt(offset int) (int, whisper.StorageArchiveLayout, error) {
	for i, a := range f.data.layout.Archives {
		size := a.Count * 12
		if f.data.layout.Compressed {
			size = a.BlockSize * a.BlockCount
		}
		if a.Offset <= offset && offset < a.Offset+size {
			return i, a, nil
		}
	}
	return 0, whisper.StorageArchiveLayout{}, errors.New("continuation offset outside archives")
}

// pointBlock reads a 128-point group. Every update checks the sidecar slots
// its points map to, which mostly hold nothing or late points written long
// ago, so sidecar groups are probed; a main file's are rewritten as it fills.
func (f *continuationFile) pointBlock(key []byte) (*store.PointBlock, error) {
	if buffer, ok := f.fs.kv.(*continuationPointBuffer); ok {
		if block := buffer.blocks[string(key)]; block != nil {
			owned := *block
			return &owned, nil
		}
	}
	var block store.PointBlock
	read := f.fs.kv.Get
	if f.sidecar {
		read = f.fs.probe
	}
	start := f.fs.timer.Nested(stagetime.TailRead)
	value, err := read(key)
	f.fs.timer.Unnest(stagetime.TailRead, start)
	if errors.Is(err, store.ErrNotFound) {
		return &block, nil
	}
	if err != nil {
		return nil, err
	}
	if err := store.DecodePointBlock(value, &block); err != nil {
		return nil, err
	}
	return &block, nil
}

// skipUnallocatedGroups lets classicGroup answer from the page allocation
// instead of reading a group no allocated page overlaps. Tests clear it to
// compare against reading every group.
var skipUnallocatedGroups = true

// unallocatedGroupSkips counts classic group reads answered from the page
// allocation without a storage read.
var unallocatedGroupSkips atomic.Uint64

// UnallocatedGroupSkips returns how many classic group reads were answered
// from the page allocation without a storage read.
func UnallocatedGroupSkips() uint64 { return unallocatedGroupSkips.Load() }

// emptyPointBlock is the result of a skipped read-only group lookup. Callers
// passing mutable=false must not modify it.
var emptyPointBlock store.PointBlock

// classicGroupAllocated reports whether any allocated page overlaps the bytes
// of classic archive a's group groupIndex.
func (f *continuationFile) classicGroupAllocated(a whisper.StorageArchiveLayout, groupIndex int) bool {
	start := a.Offset + groupIndex*128*12
	end := a.Offset + min(a.Count, (groupIndex+1)*128)*12
	for page := start / 4096; page*4096 < end; page++ {
		if f.data.pages[uint32(page)] {
			return true
		}
	}
	return false
}

// classicGroup reads a classic archive's 128-point group. Sidecars are sparse
// and every update and fetch looks up the groups its range maps to; most hold
// nothing, and a storage probe for an absent key still opens a table in every
// level. A group no allocated page overlaps holds no slot (see pages), so it is
// returned empty without a read. A mutable result is the caller's to modify.
func (f *continuationFile) classicGroup(archive int, a whisper.StorageArchiveLayout, groupIndex int, mutable bool) (*store.PointBlock, error) {
	if skipUnallocatedGroups && !f.classicGroupAllocated(a, groupIndex) {
		unallocatedGroupSkips.Add(1)
		if mutable {
			return &store.PointBlock{}, nil
		}
		return &emptyPointBlock, nil
	}
	return f.pointBlock(continuationBodyKey(f.data.id, 'p', archive, 0, groupIndex))
}

func (f *continuationFile) setPointBlock(key []byte, block *store.PointBlock) error {
	if f.sidecar && !f.data.layout.Compressed {
		if err := f.initSidecarIndex(); err != nil {
			return err
		}
		if err := f.indexSidecarGroup(key, block); err != nil {
			return err
		}
		f.fs.sidecarDirty = true
	}
	if block.Present == [2]uint64{} {
		return f.fs.kv.Delete(key)
	}
	if buffer, ok := f.fs.kv.(*continuationPointBuffer); ok {
		return buffer.set(key, block)
	}
	return f.fs.kv.Set(key, store.EncodePointBlock(block))
}

// A compressed block's descriptor holds its point count and, in format v7, the
// time bucket holding its 128-point groups. A v6 descriptor (count only) keeps
// the groups beside the file's other keys. Each block keeps its layout until
// it is rewritten from its first point.
type blockDescriptor struct {
	count    int
	bucket   uint32
	bucketed bool
}

func decodeBlockDescriptor(data []byte) (blockDescriptor, error) {
	switch len(data) {
	case 4:
		return blockDescriptor{count: int(binary.BigEndian.Uint32(data))}, nil
	case 8:
		return blockDescriptor{count: int(binary.BigEndian.Uint32(data)), bucket: binary.BigEndian.Uint32(data[4:]), bucketed: true}, nil
	}
	return blockDescriptor{}, errors.New("invalid continuation block count")
}

func (d blockDescriptor) encode() []byte {
	value := binary.BigEndian.AppendUint32(nil, uint32(d.count))
	if d.bucketed {
		value = binary.BigEndian.AppendUint32(value, d.bucket)
	}
	return value
}

// groupPrefix returns the key prefix of the block's point groups.
func (d blockDescriptor) groupPrefix(id uint64, archive, block int) []byte {
	prefix := continuationBlockPrefix(id, 'c', archive, block)
	if d.bucketed {
		return store.ContinuationBucketKey(d.bucket, prefix)
	}
	return prefix
}

func (d blockDescriptor) groupKey(id uint64, archive, block, group int) []byte {
	return binary.BigEndian.AppendUint32(d.groupPrefix(id, archive, block), uint32(group))
}

// newBlock describes a block written from its first point now.
func (fs *continuationFiles) newBlock() blockDescriptor {
	if !fs.buckets {
		return blockDescriptor{}
	}
	return blockDescriptor{bucketed: true, bucket: uint32(max(0, fs.clock().Unix()/continuationBucketSeconds))}
}

// historyBlock describes an imported block that no longer receives appends.
func (fs *continuationFiles) historyBlock() blockDescriptor {
	if !fs.buckets {
		return blockDescriptor{}
	}
	return blockDescriptor{bucketed: true, bucket: continuationHistoryBucket}
}

// Blocks written in the same hour share a bucket. Writes then concentrate in
// the few buckets whose blocks are still filling: a 1-second archive's 7,200
// point block fills in two hours, so with day buckets the hot range held a
// whole day of dense data. Imported blocks that no longer receive appends go to
// the history bucket instead. Descriptors record each block's bucket, so the
// width can change without affecting existing blocks.
const (
	continuationBucketSeconds = 60 * 60
	continuationHistoryBucket = 0
)

func (f *continuationFile) descriptor(archive, block int) (blockDescriptor, error) {
	start := f.fs.timer.Nested(stagetime.TailRead)
	data, err := f.fs.kv.Get(continuationBlockPrefix(f.data.id, 'b', archive, block))
	f.fs.timer.Unnest(stagetime.TailRead, start)
	if errors.Is(err, store.ErrNotFound) {
		return blockDescriptor{}, nil
	}
	if err != nil {
		return blockDescriptor{}, err
	}
	return decodeBlockDescriptor(data)
}

func (f *continuationFile) validateBlock(block whisper.ContinuationBlock) error {
	if f.closed {
		return os.ErrClosed
	}
	if !f.data.layout.Compressed || block.Archive < 0 || block.Archive >= len(f.data.layout.Archives) {
		return errors.New("invalid continuation compressed archive")
	}
	a := f.data.layout.Archives[block.Archive]
	if block.Index < 0 || block.Index >= a.BlockCount || block.Offset != a.Offset+block.Index*a.BlockSize ||
		block.Size != a.BlockSize || block.Step != a.Step {
		return errors.New("continuation block does not match file layout")
	}
	return nil
}

func (f *continuationFile) ReadStorageBlock(block whisper.ContinuationBlock) ([]whisper.TimeSeriesPoint, error) {
	return f.ReadStorageBlockInto(block, nil)
}

// ReadStorageBlockInto returns a borrowed result in dst. Callers retaining block
// values use ReadStorageBlock, whose nil destination allocates independent data.
// errStopGroupVisit ends a group walk once a block's count is read.
var errStopGroupVisit = errors.New("stop continuation group visit")

// skippedBlockDeletes counts block rewrites that reused their keys instead of
// deleting the old groups.
var skippedBlockDeletes atomic.Uint64

// SkippedBlockDeletes returns how many block rewrites overwrote their old
// groups in place instead of deleting them (ContinuationCodec.ReuseBlockKeys).
func SkippedBlockDeletes() uint64 { return skippedBlockDeletes.Load() }

func (f *continuationFile) ReadStorageBlockInto(block whisper.ContinuationBlock, dst []whisper.TimeSeriesPoint) ([]whisper.TimeSeriesPoint, error) {
	if err := f.validateBlock(block); err != nil {
		return nil, err
	}
	if f.fs.verifiedBlocks != nil {
		if points, ok := f.fs.verifiedBlocks[verifiedBlockKey{file: f.data.id, block: block}]; ok {
			return append(dst[:0], points...), nil
		}
	}
	desc, err := f.descriptor(block.Archive, block.Index)
	if err != nil {
		return nil, err
	}
	count := desc.count
	if count > 4*block.Size || block.Count > 0 && block.Count != count {
		return nil, fmt.Errorf("continuation physical point count differs: file %d archive %d block %d holds %d points, header %d",
			f.data.id, block.Archive, block.Index, count, block.Count)
	}
	points := dst[:0]
	if cap(points) < count {
		points = make([]whisper.TimeSeriesPoint, 0, count)
	}
	previous := int64(0)
	if count > 0 {
		// A physical CWhisper block usually spans many 128-point groups.
		// Walk their contiguous keys once instead of opening a fresh LSM
		// point iterator for every group. Keep decoded scratch on the stack.
		prefix := desc.groupPrefix(f.data.id, block.Archive, block.Index)
		start := f.fs.timer.Nested(stagetime.TailRead)
		defer f.fs.timer.Unnest(stagetime.TailRead, start)
		visit := func(key, value []byte) error {
			first := len(points)
			if first >= count {
				// Groups past the count are left over from an earlier
				// incarnation of this block whose keys the rewrite reuses.
				return errStopGroupVisit
			}
			if len(key) != len(prefix)+4 || int(binary.BigEndian.Uint32(key[len(prefix):])) != first/128 {
				return fmt.Errorf("invalid continuation physical group order: file %d archive %d block %d: group key %x, want group %d",
					f.data.id, block.Archive, block.Index, key[min(len(key), len(prefix)):], first/128)
			}
			var group store.PointBlock
			if err := store.DecodePointBlock(value, &group); err != nil {
				return err
			}
			for slot := 0; slot < min(128, count-first); slot++ {
				point := group.Points[slot]
				if group.Present[slot/64]&(uint64(1)<<uint(slot%64)) == 0 || point.Timestamp <= previous || point.Timestamp > math.MaxUint32 {
					return fmt.Errorf("invalid continuation physical point: file %d archive %d block %d (count %d, step %d): point %d present %v at %d follows %d",
						f.data.id, block.Archive, block.Index, count, block.Step, first+slot,
						group.Present[slot/64]&(uint64(1)<<uint(slot%64)) != 0, point.Timestamp, previous)
				}
				points = append(points, whisper.TimeSeriesPoint{Time: int(point.Timestamp), Value: point.Value})
				previous = point.Timestamp
			}
			return nil
		}
		if count <= 128 {
			// A single group needs no iterator. Get also sees the existing
			// descriptor/tail cache, which cannot answer a prefix walk.
			key := desc.groupKey(f.data.id, block.Archive, block.Index, 0)
			value, readErr := f.fs.kv.Get(key)
			err = readErr
			if errors.Is(err, store.ErrNotFound) {
				err = nil // The count check below reports a missing group.
			} else if err == nil {
				err = visit(key, value)
			}
		} else {
			err = f.fs.kv.Visit(prefix, visit)
		}
		if err != nil && !errors.Is(err, errStopGroupVisit) {
			return nil, err
		}
		if len(points) != count {
			return nil, fmt.Errorf("missing continuation physical group: file %d archive %d block %d: %d of %d points",
				f.data.id, block.Archive, block.Index, len(points), count)
		}
	}
	if count != 0 && (block.First != 0 && points[0].Time != block.First || block.Last != 0 && points[count-1].Time != block.Last) {
		return nil, fmt.Errorf("continuation physical point bounds differ: file %d archive %d block %d: stored %d to %d, header %d to %d",
			f.data.id, block.Archive, block.Index, points[0].Time, points[count-1].Time, block.First, block.Last)
	}
	if f.fs.verifiedBlocks != nil {
		f.fs.verifiedBlocks[verifiedBlockKey{file: f.data.id, block: block}] = append([]whisper.TimeSeriesPoint(nil), points...)
	}
	return points, nil
}

func (f *continuationFile) AppendStorageBlock(block whisper.ContinuationBlock, previous int, added []whisper.TimeSeriesPoint) error {
	if t := f.fs.timer; t != nil {
		start := t.Nested(stagetime.Encode)
		err := f.appendStorageBlock(block, previous, added, f.fs.newBlock())
		t.Unnest(stagetime.Encode, start)
		return err
	}
	return f.appendStorageBlock(block, previous, added, f.fs.newBlock())
}

// appendStorageBlock describes a block rewritten from its first point with
// fresh, which selects its layout and bucket.
func (f *continuationFile) appendStorageBlock(block whisper.ContinuationBlock, previous int, added []whisper.TimeSeriesPoint, fresh blockDescriptor) error {
	if err := f.writable(); err != nil {
		return err
	}
	if err := f.validateBlock(block); err != nil {
		return err
	}
	if previous < 0 || len(added) == 0 || previous+len(added) != block.Count {
		return errors.New("invalid continuation append count")
	}
	desc, err := f.descriptor(block.Archive, block.Index)
	if err != nil {
		return err
	}
	if previous == 0 {
		// A rewritten block drops its old groups wherever they live. When the
		// fresh layout addresses the same keys, its groups overwrite the old
		// ones in place and reads stop at the descriptor's count, so the old
		// groups past the new count need no tombstones.
		old := desc.groupPrefix(f.data.id, block.Archive, block.Index)
		if f.fs.reuseBlockKeys && bytes.Equal(old, fresh.groupPrefix(f.data.id, block.Archive, block.Index)) {
			skippedBlockDeletes.Add(1)
		} else if err := f.fs.kv.DeletePrefix(old); err != nil {
			return err
		}
		desc = fresh
	} else if desc.count != previous {
		return fmt.Errorf("continuation append predecessor differs: %d != %d", desc.count, previous)
	}
	previousTime := int64(0)
	var predecessor *store.PointBlock
	if previous > 0 {
		group, err := f.pointBlock(desc.groupKey(f.data.id, block.Archive, block.Index, (previous-1)/128))
		if err != nil {
			return err
		}
		predecessor = group
		previousTime = group.Points[(previous-1)%128].Timestamp
		if previousTime <= 0 {
			return errors.New("missing continuation append predecessor")
		}
	}
	for first := 0; first < len(added); {
		position := previous + first
		key := desc.groupKey(f.data.id, block.Archive, block.Index, position/128)
		var group *store.PointBlock
		if position%128 != 0 {
			// Only the first iteration can start within an existing group, the
			// one (same descriptor and key) decoded above into an owned block.
			group = predecessor
		} else {
			group = &store.PointBlock{}
		}
		if position != 0 && position%128 != 0 {
			previousTime = group.Points[position%128-1].Timestamp
		}
		end := min(len(added), first+128-position%128)
		for first < end {
			point := added[first]
			// Accept what the compressed format itself accepts. Real sources
			// hold unaligned points (corrupt far-future tails); requiring step
			// alignment rejected files CWhisper reads and writes normally.
			if point.Time <= 0 || int64(point.Time) > math.MaxUint32 || int64(point.Time) <= previousTime {
				return errors.New("invalid continuation appended point")
			}
			slot := (previous + first) % 128
			group.Present[slot/64] |= uint64(1) << uint(slot%64)
			group.Points[slot] = store.Point{Timestamp: int64(point.Time), Value: point.Value}
			previousTime = int64(point.Time)
			first++
		}
		if err := f.setPointBlock(key, group); err != nil {
			return err
		}
	}
	if added[len(added)-1].Time != block.Last {
		return errors.New("continuation append end differs")
	}
	desc.count = block.Count
	return f.fs.kv.Set(continuationBlockPrefix(f.data.id, 'b', block.Archive, block.Index), desc.encode())
}

// inPlaceRollupPatches lets go-whisper correct buffered rollups by replacing
// the stored points' values (ReplaceStoragePoints) instead of rewriting the
// whole file under a new id. Tests clear it to compare against the rewrite.
var inPlaceRollupPatches = true

// CanReplaceStoragePoints implements whisper.StorageBlockPatcher.
func (f *continuationFile) CanReplaceStoragePoints() bool {
	return inPlaceRollupPatches && !f.readOnly && !f.closed
}

// ReplaceStoragePoints replaces the values of the block's points at positions
// with those of points, the block's complete corrected points. Each position
// must already hold the same timestamp. Only the 128-point groups holding a
// position are rewritten; the block keeps its descriptor, bucket and count.
func (f *continuationFile) ReplaceStoragePoints(block whisper.ContinuationBlock, positions []int, points []whisper.TimeSeriesPoint) error {
	if t := f.fs.timer; t != nil {
		start := t.Nested(stagetime.Encode)
		defer t.Unnest(stagetime.Encode, start)
	}
	if err := f.writable(); err != nil {
		return err
	}
	if err := f.validateBlock(block); err != nil {
		return err
	}
	if len(points) != block.Count {
		return errors.New("invalid continuation patch count")
	}
	desc, err := f.descriptor(block.Archive, block.Index)
	if err != nil {
		return err
	}
	if desc.count != block.Count {
		return fmt.Errorf("continuation patched block count differs: %d != %d", desc.count, block.Count)
	}
	var group *store.PointBlock
	current := -1
	flush := func() error {
		if group == nil {
			return nil
		}
		return f.setPointBlock(desc.groupKey(f.data.id, block.Archive, block.Index, current), group)
	}
	for n, position := range positions {
		if position < 0 || position >= block.Count || n > 0 && position <= positions[n-1] {
			return errors.New("invalid continuation patch position")
		}
		if position/128 != current {
			if err := flush(); err != nil {
				return err
			}
			current = position / 128
			if group, err = f.pointBlock(desc.groupKey(f.data.id, block.Archive, block.Index, current)); err != nil {
				return err
			}
		}
		slot, point := position%128, points[position]
		if group.Present[slot/64]&(uint64(1)<<uint(slot%64)) == 0 || group.Points[slot].Timestamp != int64(point.Time) {
			return errors.New("continuation patched point differs from the stored point")
		}
		group.Points[slot].Value = point.Value
	}
	return flush()
}

// RewriteStorageBlock replaces the block's points from position from onward
// with points[from:], points being the block's complete new points and block
// describing them; from equal to their count only shortens the block. The
// points before from must be stored already. Only the groups from the one
// holding from are written; the block keeps its descriptor's layout and
// bucket, unlike an append from the first point.
func (f *continuationFile) RewriteStorageBlock(block whisper.ContinuationBlock, from int, points []whisper.TimeSeriesPoint) error {
	if t := f.fs.timer; t != nil {
		start := t.Nested(stagetime.Encode)
		defer t.Unnest(stagetime.Encode, start)
	}
	if err := f.writable(); err != nil {
		return err
	}
	if err := f.validateBlock(block); err != nil {
		return err
	}
	if from < 0 || from > len(points) || len(points) == 0 || len(points) != block.Count || points[0].Time != block.First || points[len(points)-1].Time != block.Last {
		return errors.New("invalid continuation block rewrite")
	}
	desc, err := f.descriptor(block.Archive, block.Index)
	if err != nil {
		return err
	}
	if desc.count < from {
		return fmt.Errorf("continuation rewrite starts past the stored points: %d > %d", from, desc.count)
	}
	for position := from; position < len(points); {
		key := desc.groupKey(f.data.id, block.Archive, block.Index, position/128)
		group := &store.PointBlock{}
		if position%128 != 0 {
			if group, err = f.pointBlock(key); err != nil {
				return err
			}
			for slot := 0; slot < position%128; slot++ {
				kept := points[position-position%128+slot]
				if group.Present[slot/64]&(uint64(1)<<uint(slot%64)) == 0 || group.Points[slot].Timestamp != int64(kept.Time) {
					return errors.New("continuation rewrite prefix differs from the stored points")
				}
			}
		}
		previous := int64(0)
		if position > 0 {
			previous = int64(points[position-1].Time)
		}
		for end := min(len(points), position-position%128+128); position < end; position++ {
			point := points[position]
			if point.Time <= 0 || int64(point.Time) > math.MaxUint32 || int64(point.Time) <= previous {
				return errors.New("invalid continuation rewritten point")
			}
			slot := position % 128
			group.Present[slot/64] |= uint64(1) << uint(slot%64)
			group.Points[slot] = store.Point{Timestamp: int64(point.Time), Value: point.Value}
			previous = int64(point.Time)
		}
		if err := f.setPointBlock(key, group); err != nil {
			return err
		}
	}
	desc.count = block.Count
	return f.fs.kv.Set(continuationBlockPrefix(f.data.id, 'b', block.Archive, block.Index), desc.encode())
}

func (f *continuationFile) rawBlock(archive, index int, a whisper.StorageArchiveLayout) ([]byte, error) {
	var points []whisper.TimeSeriesPoint
	data, err := f.readRawBlock(archive, index, a, &points)
	if err == nil && len(data) == 0 {
		data = make([]byte, a.BlockSize)
	}
	return data, err
}

// An empty block has no encoded bytes. Readers can clear their destination
// directly; partial writes still materialize a full block through rawBlock.
func (f *continuationFile) readRawBlock(archive, index int, a whisper.StorageArchiveLayout, scratch *[]whisper.TimeSeriesPoint) ([]byte, error) {
	block := whisper.ContinuationBlock{Archive: archive, Index: index, Offset: a.Offset + index*a.BlockSize, Size: a.BlockSize, Step: a.Step}
	points, err := f.ReadStorageBlockInto(block, *scratch)
	if err != nil {
		return nil, err
	}
	*scratch = points
	if len(points) == 0 {
		return nil, nil
	}
	block.First, block.Last, block.Count = points[0].Time, points[len(points)-1].Time, len(points)
	data, err := whisper.EncodeContinuationBlock(block, points)
	if err != nil {
		return nil, fmt.Errorf("continuation file %d: %w", f.data.id, err)
	}
	return data, nil
}

func (f *continuationFile) ReadAt(data []byte, offset int64) (int, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	if offset < 0 {
		return 0, errors.New("negative continuation read")
	}
	if len(data) == 0 {
		return 0, nil
	}
	if offset >= int64(f.data.layout.Size) {
		return 0, io.EOF
	}
	end := min(int64(f.data.layout.Size), offset+int64(len(data)))
	if end < offset {
		return 0, errors.New("continuation read overflow")
	}
	position, written := int(offset), 0
	var points []whisper.TimeSeriesPoint
	for int64(position) < end {
		if position < len(f.data.header) {
			count := min(int(end)-position, len(f.data.header)-position)
			copy(data[written:written+count], f.data.header[position:position+count])
			position, written = position+count, written+count
			continue
		}
		archive, a, err := f.archiveAt(position)
		if err != nil {
			return written, err
		}
		if f.data.layout.Compressed {
			index := (position - a.Offset) / a.BlockSize
			block, err := f.readRawBlock(archive, index, a, &points)
			if err != nil {
				return written, err
			}
			start := (position - a.Offset) % a.BlockSize
			count := min(int(end)-position, a.BlockSize-start)
			if len(block) == 0 {
				clear(data[written : written+count])
			} else {
				copy(data[written:written+count], block[start:start+count])
			}
			position, written = position+count, written+count
		} else {
			groupIndex := (position - a.Offset) / (128 * 12)
			group, err := f.classicGroup(archive, a, groupIndex, false)
			if err != nil {
				return written, err
			}
			limit := min(int(end), a.Offset+min(a.Count, (groupIndex+1)*128)*12)
			for position < limit {
				slot := ((position - a.Offset) / 12) % 128
				within := (position - a.Offset) % 12
				var record [12]byte
				if group.Present[slot/64]&(uint64(1)<<uint(slot%64)) != 0 {
					point := group.Points[slot]
					binary.BigEndian.PutUint32(record[:4], uint32(point.Timestamp))
					binary.BigEndian.PutUint64(record[4:], math.Float64bits(point.Value))
				}
				count := min(limit-position, 12-within)
				copy(data[written:written+count], record[within:within+count])
				position, written = position+count, written+count
			}
		}
	}
	if written != len(data) {
		return written, io.EOF
	}
	return written, nil
}

func (f *continuationFile) allocateClassicPages(start, end int) {
	if end <= start {
		return
	}
	for page := uint32(start / 4096); page <= uint32((end-1)/4096); page++ {
		if !f.data.pages[page] {
			f.data.sortedPages = nil
		}
		f.data.pages[page] = true
	}
	if !f.data.dirty && end > start {
		f.data.dirty = true
	}
}

func (f *continuationFile) WriteAt(data []byte, offset int64) (int, error) {
	if t := f.fs.timer; t != nil {
		start := t.Nested(stagetime.Encode)
		n, err := f.writeAt(data, offset)
		t.Unnest(stagetime.Encode, start)
		return n, err
	}
	return f.writeAt(data, offset)
}

func (f *continuationFile) writeAt(data []byte, offset int64) (int, error) {
	if err := f.writable(); err != nil {
		return 0, err
	}
	if offset < 0 || offset+int64(len(data)) < offset || offset+int64(len(data)) > int64(f.data.layout.Size) {
		return 0, errors.New("continuation write outside configured file")
	}
	position, used := int(offset), 0
	for used < len(data) {
		if position < len(f.data.header) {
			count := min(len(data)-used, len(f.data.header)-position)
			f.data.writeHeader(position, data[used:used+count])
			if !f.data.layout.Compressed {
				f.allocateClassicPages(position, position+count)
			}
			position, used = position+count, used+count
			continue
		}
		archive, a, err := f.archiveAt(position)
		if err != nil {
			return used, err
		}
		if f.data.layout.Compressed {
			index := (position - a.Offset) / a.BlockSize
			start := (position - a.Offset) % a.BlockSize
			count := min(len(data)-used, a.BlockSize-start)
			var raw []byte
			if start == 0 && count == a.BlockSize {
				raw = append([]byte(nil), data[used:used+count]...)
			} else {
				raw, err = f.rawBlock(archive, index, a)
				if err != nil {
					return used, err
				}
				copy(raw[start:start+count], data[used:used+count])
			}
			block := whisper.ContinuationBlock{Archive: archive, Index: index, Offset: a.Offset + index*a.BlockSize, Size: a.BlockSize, Step: a.Step}
			var points []whisper.TimeSeriesPoint
			if !allContinuationZeros(raw) {
				points, err = whisper.DecodeContinuationBlock(block, raw)
			}
			if err != nil {
				return used, err
			}
			if len(points) == 0 {
				desc, err := f.descriptor(archive, index)
				if err != nil {
					return used, err
				}
				if err := f.fs.kv.DeletePrefix(desc.groupPrefix(f.data.id, archive, index)); err != nil {
					return used, err
				}
				if err := f.fs.kv.Delete(continuationBlockPrefix(f.data.id, 'b', archive, index)); err != nil {
					return used, err
				}
			} else {
				block.First, block.Last, block.Count = points[0].Time, points[len(points)-1].Time, len(points)
				if err := f.AppendStorageBlock(block, 0, points); err != nil {
					return used, err
				}
			}
			position, used = position+count, used+count
		} else {
			groupIndex := (position - a.Offset) / (128 * 12)
			key := continuationBodyKey(f.data.id, 'p', archive, 0, groupIndex)
			group, err := f.classicGroup(archive, a, groupIndex, true)
			if err != nil {
				return used, err
			}
			limit := min(position+len(data)-used, a.Offset+min(a.Count, (groupIndex+1)*128)*12)
			from := position
			for position < limit {
				slot := ((position - a.Offset) / 12) % 128
				within := (position - a.Offset) % 12
				var record [12]byte
				if group.Present[slot/64]&(uint64(1)<<uint(slot%64)) != 0 {
					binary.BigEndian.PutUint32(record[:4], uint32(group.Points[slot].Timestamp))
					binary.BigEndian.PutUint64(record[4:], math.Float64bits(group.Points[slot].Value))
				}
				count := min(limit-position, 12-within)
				copy(record[within:within+count], data[used:used+count])
				timestamp := binary.BigEndian.Uint32(record[:4])
				if timestamp == 0 {
					group.Present[slot/64] &^= uint64(1) << uint(slot%64)
					group.Points[slot] = store.Point{}
				} else {
					group.Present[slot/64] |= uint64(1) << uint(slot%64)
					group.Points[slot] = store.Point{Timestamp: int64(timestamp), Value: math.Float64frombits(binary.BigEndian.Uint64(record[4:]))}
				}
				position, used = position+count, used+count
			}
			if err := f.setPointBlock(key, group); err != nil {
				return used, err
			}
			f.allocateClassicPages(from, position)
		}
	}
	return used, nil
}

func allContinuationZeros(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

func (f *continuationFile) NextStorageExtent(start, end, archiveStart int64) (int64, int64, error) {
	if f.closed {
		return 0, 0, os.ErrClosed
	}
	if start < archiveStart || end < start || end > int64(f.data.layout.Size) || f.data.layout.Compressed {
		return 0, 0, errors.New("invalid continuation extent request")
	}
	if f.data.sortedPages == nil {
		f.data.sortedPages = make([]uint32, 0, len(f.data.pages))
		for p := range f.data.pages {
			f.data.sortedPages = append(f.data.sortedPages, p)
		}
		sort.Slice(f.data.sortedPages, func(i, j int) bool { return f.data.sortedPages[i] < f.data.sortedPages[j] })
	}
	pages := f.data.sortedPages
	i := sort.Search(len(pages), func(i int) bool { return int64(pages[i]+1)*4096 > start })
	if i == len(pages) || int64(pages[i])*4096 >= end {
		return end, end, nil
	}
	from := max(archiveStart, int64(pages[i])*4096)
	until := int64(pages[i]+1) * 4096
	for i+1 < len(pages) && pages[i+1] == pages[i]+1 {
		i++
		until = int64(pages[i]+1) * 4096
	}
	from = archiveStart + (from-archiveStart)/12*12
	until = archiveStart + (until-archiveStart+11)/12*12
	return max(start, from), min(end, until), nil
}

var _ whisper.StorageFile = (*continuationFile)(nil)
var _ whisper.StorageLayoutFile = (*continuationFile)(nil)
var _ whisper.StorageBlockReader = (*continuationFile)(nil)
var _ whisper.StorageBlockAppender = (*continuationFile)(nil)

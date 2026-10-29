package whisper

import (
	"fmt"
	"math"
)

// ContinuationBlock identifies a physical compressed block, before circular-slot
// filtering or virtual rollups. Its points and the original header are required
// to preserve the writer's continuation, rather than only its current Fetch view.
type ContinuationBlock struct {
	Archive, Index, Offset, Size, Step int
	First, Last, Count                 int
}

// ContinuationLayout owns the complete header, including propagation buffers and
// encoder watermarks. Blocks are separate so a caller can transcode one at a time.
// The caller must keep the source immutable throughout layout and block reads.
type ContinuationLayout struct {
	Header []byte
	Size   int
	Blocks []ContinuationBlock
}

func (w *Whisper) ContinuationLayout() (ContinuationLayout, error) {
	if !w.compressed || w.aggregationMethod == Mix {
		return ContinuationLayout{}, fmt.Errorf("continuation layout requires non-Mix compressed storage")
	}
	headerSize, size := w.MetadataSize(), w.Size()
	if headerSize <= 0 || size < headerSize {
		return ContinuationLayout{}, fmt.Errorf("invalid continuation size")
	}
	layout := ContinuationLayout{Header: make([]byte, headerSize), Size: size}
	if err := w.fileReadAt(layout.Header, 0); err != nil {
		return ContinuationLayout{}, err
	}
	for ai, archive := range w.archives {
		for _, block := range archive.blockRanges {
			if block.start == 0 {
				continue
			}
			layout.Blocks = append(layout.Blocks, ContinuationBlock{
				Archive: ai, Index: block.index, Offset: archive.blockOffset(block.index),
				Size: archive.blockSize, Step: archive.secondsPerPoint,
				First: block.start, Last: block.end, Count: block.count + 1,
			})
		}
	}
	return layout, nil
}

// ContinuationBlockPoints returns all physical points, including old owners that
// the current Fetch view hides. It neither merges the sidecar nor materializes
// virtual rollups. Returned points own their storage.
func (w *Whisper) ContinuationBlockPoints(block ContinuationBlock) ([]TimeSeriesPoint, error) {
	var scratch ContinuationBlockBuffer
	return w.ContinuationBlockPointsBuffered(block, &scratch)
}

// ContinuationBlockBuffer is local scratch for sequential block reads. It must
// not be shared by concurrent readers. The ordinary API still returns owned data.
type ContinuationBlockBuffer struct {
	data   []byte
	points []dataPoint
	result []TimeSeriesPoint
}

// ContinuationBlockPointsBuffered preserves the same complete physical point
// view. Its result is borrowed until the next read using scratch; import and
// verification must consume it before reading another block.
func (w *Whisper) ContinuationBlockPointsBuffered(block ContinuationBlock, scratch *ContinuationBlockBuffer) ([]TimeSeriesPoint, error) {
	if scratch == nil {
		return nil, fmt.Errorf("continuation block scratch is nil")
	}
	if !w.compressed || block.Archive < 0 || block.Archive >= len(w.archives) {
		return nil, fmt.Errorf("invalid continuation archive")
	}
	archive := w.archives[block.Archive]
	if block.Index < 0 || block.Index >= len(archive.blockRanges) {
		return nil, fmt.Errorf("invalid continuation block")
	}
	r := archive.blockRanges[block.Index]
	if r.start == 0 || block.Offset != archive.blockOffset(block.Index) || block.Size != archive.blockSize ||
		block.Step != archive.secondsPerPoint || block.First != r.start || block.Last != r.end || block.Count != r.count+1 {
		return nil, fmt.Errorf("continuation layout changed")
	}
	if block.Size < PointSize || block.Size > maxInt/4 || block.Count <= 0 || block.Count > 4*block.Size {
		return nil, fmt.Errorf("invalid continuation point count")
	}
	if input, ok := w.file.(interface {
		ReadStorageBlockInto(ContinuationBlock, []TimeSeriesPoint) ([]TimeSeriesPoint, error)
	}); ok {
		result, err := input.ReadStorageBlockInto(block, scratch.result[:0])
		scratch.result = result
		return result, err
	}
	if input, ok := w.file.(StorageBlockReader); ok {
		return input.ReadStorageBlock(block)
	}
	if cap(scratch.data) < block.Size {
		scratch.data = make([]byte, block.Size)
	}
	data := scratch.data[:block.Size]
	if err := w.fileReadAt(data, int64(block.Offset)); err != nil {
		return nil, err
	}
	if cap(scratch.points) < block.Count {
		scratch.points = make([]dataPoint, 0, block.Count)
	}
	points, _, err := archive.ReadFromBlock(data, scratch.points[:0], 0, maxInt)
	scratch.points = points
	if err != nil {
		return nil, err
	}
	if len(points) != block.Count {
		return nil, fmt.Errorf("continuation block count differs: %d != %d", len(points), block.Count)
	}
	if cap(scratch.result) < len(points) {
		scratch.result = make([]TimeSeriesPoint, len(points))
	}
	result := scratch.result[:len(points)]
	scratch.result = result
	for i, p := range points {
		result[i] = TimeSeriesPoint{Time: p.interval, Value: p.value}
	}
	return result, nil
}

// EncodeContinuationBlock reconstructs a compressed block from exact typed
// points without changing source state. The ordinary codec supplies the same
// prefix bits for every append batch partition. Unused bytes after EOB are zero.
//
// Points must be positive and strictly increasing, which is what the format
// holds; they need not lie on the archive's grid. Converted sources contain
// off-grid points (corrupt CWhisper blocks), and the store keeps them exactly,
// so every rewrite must be able to read such a block back. The encoder stores
// an off-grid change of interval in full; a block holding one is decoded again
// here to prove it reads back unchanged.
func EncodeContinuationBlock(block ContinuationBlock, points []TimeSeriesPoint) ([]byte, error) {
	if block.Size < PointSize+endOfBlockSize || block.Step <= 0 || block.Offset < 0 ||
		block.Count <= 0 || len(points) != block.Count || block.Size > maxInt-2*MaxCompressedPointSize-2*endOfBlockSize {
		return nil, fmt.Errorf("invalid continuation block encoding: %s, size %d, %d points", block.describe(), block.Size, len(points))
	}
	input := make([]dataPoint, len(points))
	previous, offGrid := 0, false
	for i, p := range points {
		if p.Time <= previous {
			return nil, fmt.Errorf("continuation points must increase: %s: point %d at %d follows %d", block.describe(), i, p.Time, previous)
		}
		offGrid = offGrid || p.Time%block.Step != 0
		input[i] = dataPoint{interval: p.Time, value: p.Value}
		previous = p.Time
	}
	if points[0].Time != block.First || previous != block.Last {
		return nil, fmt.Errorf("continuation point bounds differ: %s: points span %d to %d", block.describe(), points[0].Time, previous)
	}
	// A single local block avoids allocating a source-sized block-range table.
	archive := archiveInfo{
		Retention: NewRetention(block.Step, block.Count), offset: block.Offset,
		blockSize: block.Size, blockRanges: make([]blockRange, 1),
		cblock: blockInfo{lastByteOffset: block.Offset, lastByteBitPos: 7},
	}
	data := make([]byte, block.Size+2*MaxCompressedPointSize+2*endOfBlockSize)
	_, left, _ := archive.AppendPointsToBlock(data, input)
	if len(left) != 0 {
		return nil, fmt.Errorf("continuation points exceed their original block: %s: %d of %d points fit in %d bytes",
			block.describe(), len(points)-len(left), len(points), block.Size)
	}
	if offGrid {
		decoded, _, err := archive.ReadFromBlock(data[:block.Size], make([]dataPoint, 0, len(input)), 0, maxInt)
		if err == nil && len(decoded) != len(input) {
			err = fmt.Errorf("%d points read back", len(decoded))
		}
		for i := 0; err == nil && i < len(decoded); i++ {
			if decoded[i].interval != input[i].interval || math.Float64bits(decoded[i].value) != math.Float64bits(input[i].value) {
				err = fmt.Errorf("point %d reads back as %d %v, written %d %v", i, decoded[i].interval, decoded[i].value, input[i].interval, input[i].value)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("continuation block does not read back: %s: %w", block.describe(), err)
		}
	}
	return data[:block.Size], nil
}

// describe names a block and its descriptor for errors.
func (block ContinuationBlock) describe() string {
	return fmt.Sprintf("archive %d block %d (first %d, last %d, count %d, step %d)",
		block.Archive, block.Index, block.First, block.Last, block.Count, block.Step)
}

func DecodeContinuationBlock(block ContinuationBlock, data []byte) ([]TimeSeriesPoint, error) {
	if block.Step <= 0 || block.Size < PointSize+endOfBlockSize || len(data) != block.Size {
		return nil, fmt.Errorf("invalid continuation block decoding")
	}
	count := block.Count
	if count < 1 {
		count = 1
	}
	archive := archiveInfo{Retention: NewRetention(block.Step, count), blockSize: block.Size}
	points, _, err := archive.ReadFromBlock(data, nil, 0, maxInt)
	if err != nil {
		return nil, err
	}
	if block.Count > 0 && len(points) != block.Count {
		return nil, fmt.Errorf("continuation block count differs: %s: %d points decoded", block.describe(), len(points))
	}
	result := make([]TimeSeriesPoint, len(points))
	for i, p := range points {
		result[i] = TimeSeriesPoint{Time: p.interval, Value: p.value}
	}
	if len(result) != 0 && ((block.First != 0 && result[0].Time != block.First) ||
		(block.Last != 0 && result[len(result)-1].Time != block.Last)) {
		return nil, fmt.Errorf("continuation block bounds differ: %s: decoded %d to %d", block.describe(), result[0].Time, result[len(result)-1].Time)
	}
	return result, nil
}

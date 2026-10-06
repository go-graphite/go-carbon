package whisper

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
	"unsafe"
)

var (
	CompressedMetadataSize     = 28 + FreeCompressedMetadataSize
	FreeCompressedMetadataSize = 16

	VersionSize = 1

	CompressedArchiveInfoSize     = 92 + FreeCompressedArchiveInfoSize
	FreeCompressedArchiveInfoSize = 36

	compressedHeaderAggregationOffset = len(compressedMagicString) + VersionSize
	compressedHeaderXFFOffset         = compressedHeaderAggregationOffset + IntSize*2

	BlockRangeSize = 16
	endOfBlockSize = 5

	// One can see that blocks that extend longer than two
	// hours provide diminishing returns for compressed size. A
	// two-hour block allows us to achieve a compression ratio of
	// 1.37 bytes per data point.
	//                                      4.1.2 Compressing values
	//     Gorilla: A Fast, Scalable, In-Memory Time Series Database
	DefaultPointsPerBlock = 7200 // recommended by the gorilla paper algorithm

	// using 2 buffer here to mitigate data points arriving at
	// random orders causing early propagation
	bufferCount = 2

	compressedMagicString = []byte("whisper_compressed") // len = 18

	debugCompress  bool
	debugBitsWrite bool
	debugExtend    bool

	avgCompressedPointSize float32 = 2
)

// In worst case scenario all data points would required 2 bytes more space
// after compression, this buffer size make sure that it's always big enough
// to contain the compressed result
const MaxCompressedPointSize = PointSize + 2

const sizeEstimationBuffer = 0.618

func Debug(compress, bitsWrite bool) {
	debugCompress = compress
	debugBitsWrite = bitsWrite
}

func (whisper *Whisper) WriteHeaderCompressed() (err error) {
	scratch := compressedHeaderScratch.Get().(*[]byte)
	b := *scratch
	size := whisper.MetadataSize()
	if cap(b) < size {
		b = make([]byte, size)
	}
	b = b[:size]
	// Reserved fields and the CRC placeholder must start at zero when
	// reusing storage that previously held a different file's header.
	for i := range b {
		b[i] = 0
	}
	defer func() {
		if cap(b) > 64*1024 {
			b = nil
		}
		*scratch = b[:0]
		compressedHeaderScratch.Put(scratch)
	}()
	i := 0

	// magic string
	i += len(compressedMagicString)
	copy(b, compressedMagicString)

	// version
	b[i] = whisper.compVersion
	i += VersionSize

	i += packInt(b, int(whisper.aggregationMethod), i)
	i += packInt(b, whisper.maxRetention, i)
	i += packFloat32(b, whisper.xFilesFactor, i)
	i += packInt(b, whisper.pointsPerBlock, i)
	i += packInt(b, len(whisper.archives), i)
	i += packFloat32(b, whisper.avgCompressedPointSize, i)
	i += packInt(b, 0, i) // crc32 always write at the end of whisper meta info header and before archive header
	i += FreeCompressedMetadataSize

	for _, archive := range whisper.archives {
		i += packInt(b, archive.offset, i)
		i += packInt(b, archive.secondsPerPoint, i)
		i += packInt(b, archive.numberOfPoints, i)
		i += packInt(b, archive.blockSize, i)
		i += packInt(b, archive.blockCount, i)
		i += packFloat32(b, archive.avgCompressedPointSize, i)

		var mixSpecSize int
		if archive.aggregationSpec != nil {
			b[i] = byte(archive.aggregationSpec.Method)
			i += ByteSize
			i += packFloat32(b, archive.aggregationSpec.Percentile, i)
			mixSpecSize = ByteSize + FloatSize
		}

		i += packInt(b, archive.cblock.index, i)
		i += packInt(b, archive.cblock.p0.interval, i)
		i += packFloat64(b, archive.cblock.p0.value, i)
		i += packInt(b, archive.cblock.pn1.interval, i)
		i += packFloat64(b, archive.cblock.pn1.value, i)
		i += packInt(b, archive.cblock.pn2.interval, i)
		i += packFloat64(b, archive.cblock.pn2.value, i)
		i += packInt(b, int(archive.cblock.lastByte), i)
		i += packInt(b, archive.cblock.lastByteOffset, i)
		i += packInt(b, archive.cblock.lastByteBitPos, i)
		i += packInt(b, archive.cblock.count, i)
		i += packInt(b, int(archive.cblock.crc32), i)

		i += packInt(b, int(archive.stats.discard.oldInterval), i)
		i += packInt(b, int(archive.stats.extended), i)

		i += FreeCompressedArchiveInfoSize - mixSpecSize

		if FreeCompressedArchiveInfoSize < mixSpecSize {
			panic("out of FreeCompressedArchiveInfoSize") // a panic that should never happens
		}
	}

	// write block_range_info and buffer
	for _, archive := range whisper.archives {
		for _, bran := range archive.blockRanges {
			i += packInt(b, bran.start, i)
			i += packInt(b, bran.end, i)
			i += packInt(b, bran.count, i)
			i += packInt(b, int(bran.crc32), i)
		}

		if archive.hasBuffer() {
			i += copy(b[i:], archive.buffer)
		}
	}

	whisper.crc32 = crc32(b, 0)
	packInt(b, int(whisper.crc32), whisper.crc32Offset())

	if err := whisper.fileWriteAt(b, 0); err != nil {
		return err
	}
	if _, err := whisper.file.Seek(int64(len(b)), 0); err != nil {
		return err
	}

	return nil
}

// Only scratch bytes are pooled: archive metadata and propagation buffers must
// remain owned by the handle, including across Close during a rewrite.
var compressedHeaderScratch = sync.Pool{New: func() interface{} { return new([]byte) }}

func (whisper *Whisper) readHeaderCompressed() (err error) {
	if _, err := whisper.file.Seek(int64(len(compressedMagicString)), 0); err != nil {
		return err
	}

	offset := 0
	hlen := whisper.MetadataSize() - len(compressedMagicString)
	scratch := compressedHeaderScratch.Get().(*[]byte)
	b := *scratch
	defer func() {
		// An unusually large header must not inflate the pool indefinitely.
		if cap(b) > 64*1024 {
			b = nil
		}
		*scratch = b[:0]
		compressedHeaderScratch.Put(scratch)
	}()
	if cap(b) < hlen {
		b = make([]byte, hlen)
	}
	b = b[:hlen]
	readed, err := whisper.file.Read(b)
	if err != nil {
		err = fmt.Errorf("unable to read header: %s", err)
		return
	}
	if readed != hlen {
		err = fmt.Errorf("unable to read header: EOF")
		return
	}

	whisper.compVersion = b[offset]
	offset++

	whisper.aggregationMethod = AggregationMethod(unpackInt(b[offset : offset+IntSize]))
	offset += IntSize
	whisper.maxRetention = unpackInt(b[offset : offset+IntSize])
	offset += IntSize
	whisper.xFilesFactor = unpackFloat32(b[offset : offset+FloatSize])
	offset += FloatSize
	whisper.pointsPerBlock = unpackInt(b[offset : offset+IntSize])
	offset += IntSize
	archiveCount := unpackInt(b[offset : offset+IntSize])
	offset += IntSize
	whisper.avgCompressedPointSize = unpackFloat32(b[offset : offset+FloatSize])
	offset += FloatSize
	whisper.crc32 = uint32(unpackInt(b[offset : offset+IntSize]))
	offset += IntSize
	offset += FreeCompressedMetadataSize

	whisper.archives = make([]*archiveInfo, archiveCount)
	for i := 0; i < archiveCount; i++ {
		if cap(b) < CompressedArchiveInfoSize {
			b = make([]byte, CompressedArchiveInfoSize)
		}
		b = b[:CompressedArchiveInfoSize]
		readed, err = whisper.file.Read(b)
		if err != nil || readed != CompressedArchiveInfoSize {
			err = fmt.Errorf("unable to read compressed archive %d metadata: %s", i, err)
			return
		}
		var offset int
		var arc archiveInfo

		arc.offset = unpackInt(b[offset : offset+IntSize])
		offset += IntSize
		arc.secondsPerPoint = unpackInt(b[offset : offset+IntSize])
		offset += IntSize
		arc.numberOfPoints = unpackInt(b[offset : offset+IntSize])
		offset += IntSize
		arc.blockSize = unpackInt(b[offset : offset+IntSize])
		offset += IntSize
		arc.blockCount = unpackInt(b[offset : offset+IntSize])
		offset += IntSize
		arc.avgCompressedPointSize = unpackFloat32(b[offset : offset+FloatSize])
		offset += FloatSize

		if whisper.aggregationMethod == Mix && i > 0 {
			arc.aggregationSpec = &MixAggregationSpec{}
			arc.aggregationSpec.Method = AggregationMethod(b[offset])
			offset += ByteSize
			arc.aggregationSpec.Percentile = unpackFloat32(b[offset : offset+FloatSize])
			offset += FloatSize
		}

		arc.cblock.index = unpackInt(b[offset : offset+IntSize])
		offset += IntSize
		arc.cblock.p0.interval = unpackInt(b[offset : offset+IntSize])
		offset += IntSize
		arc.cblock.p0.value = unpackFloat64(b[offset : offset+Float64Size])
		offset += Float64Size
		arc.cblock.pn1.interval = unpackInt(b[offset : offset+IntSize])
		offset += IntSize
		arc.cblock.pn1.value = unpackFloat64(b[offset : offset+Float64Size])
		offset += Float64Size
		arc.cblock.pn2.interval = unpackInt(b[offset : offset+IntSize])
		offset += IntSize
		arc.cblock.pn2.value = unpackFloat64(b[offset : offset+Float64Size])
		offset += Float64Size
		arc.cblock.lastByte = byte(unpackInt(b[offset : offset+IntSize]))
		offset += IntSize
		arc.cblock.lastByteOffset = unpackInt(b[offset : offset+IntSize])
		offset += IntSize
		arc.cblock.lastByteBitPos = unpackInt(b[offset : offset+IntSize])
		offset += IntSize
		arc.cblock.count = unpackInt(b[offset : offset+IntSize])
		offset += IntSize
		arc.cblock.crc32 = uint32(unpackInt(b[offset : offset+IntSize]))
		offset += IntSize

		arc.stats.discard.oldInterval = uint32(unpackInt(b[offset : offset+IntSize]))
		whisper.discardedPointsAtOpen += arc.stats.discard.oldInterval
		offset += IntSize
		arc.stats.extended = uint32(unpackInt(b[offset : offset+IntSize]))
		offset += IntSize

		whisper.archives[i] = &arc
	}

	whisper.initMetaInfo()

	for i, arc := range whisper.archives {
		size := BlockRangeSize * arc.blockCount
		if cap(b) < size {
			b = make([]byte, size)
		}
		b = b[:size]
		readed, err = whisper.file.Read(b)
		if err != nil || readed != BlockRangeSize*arc.blockCount {
			err = fmt.Errorf("unable to read archive %d block ranges: %s", i, err)
			return
		}
		offset := 0

		arc.blockRanges = make([]blockRange, arc.blockCount)
		for i := range arc.blockRanges {
			arc.blockRanges[i].index = i
			arc.blockRanges[i].start = unpackInt(b[offset : offset+IntSize])
			offset += IntSize
			arc.blockRanges[i].end = unpackInt(b[offset : offset+IntSize])
			offset += IntSize
			arc.blockRanges[i].count = unpackInt(b[offset : offset+IntSize])
			offset += IntSize
			arc.blockRanges[i].crc32 = uint32(unpackInt(b[offset : offset+IntSize]))
			offset += IntSize
		}

		// arc.initBlockRanges()

		if !arc.hasBuffer() {
			continue
		}
		arc.buffer = make([]byte, arc.bufferSize)

		readed, err = whisper.file.Read(arc.buffer)
		if err != nil {
			return fmt.Errorf("unable to read archive %d buffer: %s", i, err)
		} else if readed != arc.bufferSize {
			return fmt.Errorf("unable to read archive %d buffer: readed = %d want = %d", i, readed, arc.bufferSize)
		}
	}

	return nil
}

func (a *archiveInfo) blockOffset(blockIndex int) int {
	return a.offset + blockIndex*a.blockSize
}

const maxInt = 1<<uint(strconv.IntSize-1) - 1

func (archive *archiveInfo) getSortedBlockRanges() []blockRange {
	brs := make([]blockRange, len(archive.blockRanges))
	copy(brs, archive.blockRanges)

	sort.SliceStable(brs, func(i, j int) bool {
		istart := brs[i].start
		if brs[i].start == 0 {
			istart = maxInt
		}
		jstart := brs[j].start
		if brs[j].start == 0 {
			jstart = maxInt
		}

		return istart < jstart
	})
	return brs
}

// Match the prefix of getSortedBlockRanges before the current block without
// allocating or sorting. Empty starts sort last; ties preserve slice order.
func (archive *archiveInfo) blockStatsBeforeCurrent() (points, blocks int) {
	current, currentStart := len(archive.blockRanges), maxInt
	for i, b := range archive.blockRanges {
		if b.index == archive.cblock.index {
			start := b.start
			if start == 0 {
				start = maxInt
			}
			if current == len(archive.blockRanges) || start < currentStart {
				current, currentStart = i, start
			}
		}
	}
	for i, b := range archive.blockRanges {
		start := b.start
		if start == 0 {
			start = maxInt
		}
		if current == len(archive.blockRanges) || start < currentStart || (start == currentStart && i < current) {
			points += b.count
			blocks++
		}
	}
	return points, blocks
}

func (archive *archiveInfo) getRange() (from, until int) {
	for _, b := range archive.blockRanges {
		if from == 0 || from > b.start {
			from = b.start
		}
		if until == 0 || b.end > until {
			until = b.end
		}
	}
	return
}

func (archive *archiveInfo) hasBuffer() bool { return archive.bufferSize > 0 }

func (whisper *Whisper) fetchCompressed(start, end int64, archive *archiveInfo) ([]dataPoint, error) {
	dst, err := whisper.storedPoints(archive, int(start), int(end))
	if err != nil {
		return nil, err
	}

	base := whisper.archives[0]
	if base == archive {
		return whisper.filterCompressedSlots(archive, dst)
	}

	// Start live aggregation. This probably has a read peformance hit.
	if whisper.aggregationMethod == Mix {
		// Mix aggregation is triggered when block in base archive is rotated and also
		// depends on the sufficiency of data points. This could results to a over
		// long gap when fetching data from higer archives, depending on different
		// retention policy. Therefore cwhisper needs to do live aggregation.

		var dps []dataPoint
		var inBase bool
		var baseLookupNeeded = len(dst) == 0 || dst[len(dst)-1].interval < int(end)
		if baseLookupNeeded {
			bstart, bend := base.getRange()
			inBase = int64(bstart) <= end || end <= int64(bend)
		}

		if inBase {
			nstart := start
			if len(dst) > 0 {
				// TODO: invest why shifting the last data point interval is wrong
				nstart = int64(archive.Interval(dst[len(dst)-1].interval)) // + archive.secondsPerPoint
			}
			var err error
			dps, err = whisper.fetchCompressed(nstart, end, base)
			if err != nil {
				return dst, err
			}
		}

		if base.hasBuffer() {
			for _, p := range unpackDataPoints(base.buffer) {
				if p.interval != 0 && int(start) <= p.interval && p.interval <= int(end) {
					dps = append(dps, p)
				}
			}
		}

		adps := whisper.aggregateByArchives(dps)
		dst = append(dst, adps[archive]...)
	} else {
		// Buffers are circular and can contain an unfinished final window.
		// Aggregate distinct slots in time order, applying XFF at every level.
		var pending []dataPoint
		for _, arc := range whisper.archives {
			if arc == archive {
				break
			}
			buffered := unpackDataPointsStrict(arc.buffer)
			from, until, present := spanOf(buffered, pending)
			var stored []dataPoint
			if present {
				from = arc.AggregateInterval(from)
				until = arc.AggregateInterval(until) + arc.next.secondsPerPoint - arc.secondsPerPoint
				var err error
				stored, err = whisper.storedPoints(arc, from, until)
				if err != nil {
					return nil, err
				}
				stored, err = whisper.filterCompressedSlots(arc, stored)
				if err != nil {
					return nil, err
				}
			}
			values := make(map[int]float64, len(stored)+len(pending))
			for _, p := range stored {
				values[p.interval] = p.value
			}
			for _, p := range pending {
				values[p.interval] = p.value
			}
			points := make([]dataPoint, 0, len(values))
			for interval, value := range values {
				points = append(points, dataPoint{interval, value})
			}
			sort.Slice(points, func(i, j int) bool { return points[i].interval < points[j].interval })
			pending = nil
			known := make([]float64, 0, arc.next.secondsPerPoint/arc.secondsPerPoint)
			for i := 0; i < len(points); {
				window := arc.AggregateInterval(points[i].interval)
				known = known[:0]
				for i < len(points) && arc.AggregateInterval(points[i].interval) == window {
					known = append(known, points[i].value)
					i++
				}
				if float32(len(known))/float32(arc.next.secondsPerPoint/arc.secondsPerPoint) >= whisper.xFilesFactor {
					pending = append(pending, dataPoint{window, aggregate(whisper.aggregationMethod, known)})
				}
			}
		}
		// Future buffered rollups can overwrite a requested circular slot.
		// Include them while resolving slot ownership, then clip the result.
		dst = append(dst, pending...)
	}

	if whisper.aggregationMethod == Mix {
		return dst, nil
	}
	dst, err = whisper.filterCompressedSlots(archive, dst)
	if err != nil {
		return nil, err
	}
	kept := dst[:0]
	for _, point := range dst {
		if int(start) <= point.interval && point.interval <= int(end) {
			kept = append(kept, point)
		}
	}
	return kept, nil
}

// Blocks can retain more than the logical circular archive. A newer sample
// occupying the same slot invalidates the older one, including future writes.
func (whisper *Whisper) filterCompressedSlots(archive *archiveInfo, points []dataPoint) ([]dataPoint, error) {
	if whisper.aggregationMethod == Mix {
		return points, nil
	}
	from, until, ok := spanOf(points)
	if !ok {
		return points, nil
	}
	newerFrom := from + archive.MaxRetention()
	_, latestInterval := archive.getRange()
	if until > latestInterval {
		latestInterval = until
	}
	for offset := 0; offset < len(archive.buffer); offset += PointSize {
		if interval := unpackInt(archive.buffer[offset:]); interval > latestInterval {
			latestInterval = interval
		}
	}
	// Include both stored and virtual buffered points in the bound. A span
	// shorter than one ring cannot contain aliases of different timestamps.
	if latestInterval < newerFrom {
		return points, nil
	}
	newer, err := whisper.storedPoints(archive, newerFrom, maxInt)
	if err != nil {
		return nil, err
	}
	latest := make(map[int]int, len(points)+len(newer))
	for _, ps := range [][]dataPoint{points, newer} {
		for _, p := range ps {
			slot := mod(p.interval/archive.secondsPerPoint, archive.numberOfPoints)
			if p.interval > latest[slot] {
				latest[slot] = p.interval
			}
		}
	}
	kept := points[:0]
	for _, p := range points {
		if latest[mod(p.interval/archive.secondsPerPoint, archive.numberOfPoints)] == p.interval {
			kept = append(kept, p)
		}
	}
	return kept, nil
}

// splitOutOfOrder separates out the points that the block encoder cannot
// accept: those whose interval is not strictly newer than the current block
// watermark (cblock.pn1). Compressed blocks are delta-of-delta encoded and
// addressable only per block, so such points can never be written in place.
//
// Points with a zero interval stay in kept and are skipped by the encoder, as
// before. Rejected points are counted in stats.discard.oldInterval whether or
// not the caller goes on to divert them to the out-of-order sidecar, so that
// counter keeps its existing meaning: rejected by the compressed encoder.
//
// The watermark advances as points are accepted, mirroring what the encoder
// itself does, so kept comes back strictly ascending whatever order it was given
// and AppendPointsToBlock's own check never has to fire. That keeps
// stats.discard.oldInterval and the diverted count in agreement.
//
// kept aliases the caller's backing array and is compacted in place; every
// caller passes a freshly built slice.
func (archive *archiveInfo) splitOutOfOrder(dps []dataPoint) (kept, dropped []dataPoint) {
	watermark := archive.cblock.pn1.interval

	n := 0
	for _, p := range dps {
		if p.interval != 0 && p.interval <= watermark {
			archive.stats.discard.oldInterval++
			dropped = append(dropped, p)
			continue
		}
		if p.interval > watermark {
			watermark = p.interval
		}
		dps[n] = p
		n++
	}

	return dps[:n], dropped
}

// NOTE: this method assumes data saved in higer archives are fixed. If
// we mvoe to allowing data/intervals coming in non-monotonic order, we
// need to rethink the implementation here as well.
//
// Points rejected because they are not newer than what is already written are
// returned as dropped, so the caller can divert them to the out-of-order
// sidecar (see ooo.go). Without Options.OutOfOrder the caller ignores them,
// which is the historical behaviour.
func (whisper *Whisper) archiveUpdateManyCompressed(archive *archiveInfo, points []*TimeSeriesPoint) (dropped []oooPoint, err error) {
	alignedPoints := alignPoints(archive, points)
	// Classic writes the entire batch into its circular slots before doing
	// any propagation. Do not roll up an earlier sample overwritten later
	// in this same batch, for example by a future timestamp in the same slot.
	lastSlot := make(map[int]int, len(alignedPoints))
	for i, point := range alignedPoints {
		lastSlot[mod(point.interval/archive.secondsPerPoint, archive.numberOfPoints)] = i
	}
	kept := alignedPoints[:0]
	for i, point := range alignedPoints {
		if lastSlot[mod(point.interval/archive.secondsPerPoint, archive.numberOfPoints)] == i {
			kept = append(kept, point)
		}
	}
	alignedPoints = kept

	// Note: in the current design, mix aggregation doesn't have any buffer in
	// higer archives
	if !archive.hasBuffer() {
		var rejected []dataPoint
		alignedPoints, rejected = archive.splitOutOfOrder(alignedPoints)
		dropped = whisper.appendOOO(dropped, archive, rejected)

		rotated, err := archive.appendToBlockAndRotate(alignedPoints)
		if err != nil {
			return dropped, err
		}

		if !(whisper.aggregationMethod == Mix && rotated) {
			return dropped, nil
		}

		return dropped, whisper.propagateToMixedArchivesCompressed()
	}

	baseIntervalsPerUnit, currentUnit, minInterval := archive.getBufferInfo()
	bufferUnitPointsCount := archive.next.secondsPerPoint / archive.secondsPerPoint
	for aindex := 0; aindex < len(alignedPoints); {
		dp := alignedPoints[aindex]
		dpBaseInterval := archive.AggregateInterval(dp.interval)

		// NOTE: current implementation expects data points to be monotonically
		// increasing in time
		if minInterval != 0 && dpBaseInterval < minInterval { // TODO: check against cblock pn1.interval?
			archive.stats.discard.oldInterval++
			dropped = whisper.appendOOO(dropped, archive, []dataPoint{dp})
			aindex++
			continue
		}

		// Tolerate out of order data handling within the current buffer.
		targetUnit := -1
		for i, unitInterval := range baseIntervalsPerUnit {
			if dpBaseInterval == unitInterval {
				targetUnit = i
				break
			}
		}
		if targetUnit != -1 {
			aindex++

			// TODO: not efficient if many data points are being written in one call
			offset := targetUnit*bufferUnitPointsCount + (dp.interval-dpBaseInterval)/archive.secondsPerPoint
			copy(archive.buffer[offset*PointSize:], dp.Bytes())

			continue
		}

		// check if buffer is full
		if baseIntervalsPerUnit[currentUnit] == 0 || baseIntervalsPerUnit[currentUnit] == dpBaseInterval {
			aindex++
			baseIntervalsPerUnit[currentUnit] = dpBaseInterval

			// TODO: not efficient if many data points are being written in one call
			offset := currentUnit*bufferUnitPointsCount + (dp.interval-dpBaseInterval)/archive.secondsPerPoint
			copy(archive.buffer[offset*PointSize:], dp.Bytes())

			continue
		}

		currentUnit = (currentUnit + 1) % len(baseIntervalsPerUnit)
		baseIntervalsPerUnit[currentUnit] = 0

		// flush buffer
		buffer := archive.getBufferByUnit(currentUnit)
		dps := unpackDataPointsStrict(buffer)

		// reset buffer
		for i := range buffer {
			buffer[i] = 0
		}

		dps, flushRejected := archive.splitOutOfOrder(dps)
		dropped = whisper.appendOOO(dropped, archive, flushRejected)

		// nothing left to write or propagate: the unit was empty, or every
		// point in it was older than the block watermark and got diverted
		if len(dps) <= 0 {
			continue
		}

		previousEnd := archive.cblock.pn1.interval
		if _, err := archive.appendToBlockAndRotate(dps); err != nil {
			// TODO: record and continue?
			return dropped, err
		}

		// propagate
		lower := archive.next
		lowerIntervalStart := archive.AggregateInterval(dps[0].interval)
		// A rewrite can encode the beginning of an unfinished window. When
		// its remaining buffer is flushed, aggregate the whole stored window.
		if previousEnd >= lowerIntervalStart {
			dps, err = whisper.storedPoints(archive, lowerIntervalStart, lowerIntervalStart+lower.secondsPerPoint-archive.secondsPerPoint)
			if err != nil {
				return dropped, err
			}
			dps, err = whisper.filterCompressedSlots(archive, dps)
			if err != nil {
				return dropped, err
			}
		}

		var knownValues []float64
		for _, dPoint := range dps {
			knownValues = append(knownValues, dPoint.value)
		}

		knownPercent := float32(len(knownValues)) / float32(lower.secondsPerPoint/archive.secondsPerPoint)
		// check we have enough data points to propagate a value
		if knownPercent >= whisper.xFilesFactor {
			aggregateValue := aggregate(whisper.aggregationMethod, knownValues)
			point := &TimeSeriesPoint{lowerIntervalStart, aggregateValue}

			// TODO: consider migrating to a non-recursive propagation implementation like mix policy
			lowerDropped, err := whisper.archiveUpdateManyCompressed(lower, []*TimeSeriesPoint{point})
			dropped = append(dropped, lowerDropped...)
			if err != nil {
				return dropped, err
			}
		}
	}

	return dropped, nil
}

func (archive *archiveInfo) getBufferInfo() (units []int, index, min int) {
	var max int
	for i := 0; i < archive.bufferUnitCount(); i++ {
		v := getFirstDataPointStrict(archive.getBufferByUnit(i)).interval
		if v > 0 {
			v = archive.AggregateInterval(v)
		}
		units = append(units, v)

		if max < v {
			max = v
			index = i
		}
		if min == 0 || min > v {
			min = v
		}
	}
	return
}

func (archive *archiveInfo) bufferUnitCount() int {
	return len(archive.buffer) / PointSize / (archive.next.secondsPerPoint / archive.secondsPerPoint)
}

func (archive *archiveInfo) getBufferByUnit(unit int) []byte {
	count := archive.next.secondsPerPoint / archive.secondsPerPoint
	lb := unit * PointSize * count
	ub := (unit + 1) * PointSize * count
	return archive.buffer[lb:ub]
}

func (archive *archiveInfo) appendToBlockAndRotate(dps []dataPoint) (rotated bool, err error) {
	rotated, _, err = archive.appendToBlockAndRotateWithBuffer(dps, nil)
	return rotated, err
}

func (archive *archiveInfo) appendToBlockAndRotateWithBuffer(dps []dataPoint, blockBuffer []byte) (bool, []byte, error) {
	var rotated bool
	whisper := archive.whisper // TODO: optimize away?

	// Why MaxCompressedPointSize+1 and endOfBlockSize*2:
	//
	// MaxCompressedPointSize is set to 14, but in reality, a maximum compressed
	// data point has a bit length of 14.125(113 bits). And there might be times
	// when the current block is almost full, and more data needs to be set 0. So
	// here go-whisper should prefer to have a large buffer just to be on the safe
	// side.
	//
	// An edge case that can be fixed by this allocation strategy is that: the
	// current block has less than 14 bytes plus endOfBlockSize (5) bytes
	// available, and a data point that can't be well compressed comes in, then the
	// buffer isn't large enough to fill the generated binary data.
	//
	// This allocation strategy makes sure that there is enough space in the block
	// buffer for compression output.
	bufferSize := len(dps)*(MaxCompressedPointSize+1) + endOfBlockSize*2
	if cap(blockBuffer) < bufferSize {
		blockBuffer = make([]byte, bufferSize)
	} else {
		blockBuffer = blockBuffer[:bufferSize]
		for i := range blockBuffer {
			blockBuffer[i] = 0
		}
	}

	for {
		offset := archive.cblock.lastByteOffset // lastByteOffset is updated in AppendPointsToBlock
		size, left, rotate := archive.AppendPointsToBlock(blockBuffer, dps)

		// flush block
		if size >= len(blockBuffer) {
			// TODO: panic?
			size = len(blockBuffer)
		}
		if err := whisper.fileWriteAt(blockBuffer[:size], int64(offset)); err != nil {
			return rotated, blockBuffer, err
		}

		if len(left) == 0 {
			break
		}

		// reset block
		for i := 0; i < len(blockBuffer); i++ {
			blockBuffer[i] = 0
		}

		dps = left
		if !rotate {
			continue
		}

		var nblock blockInfo
		nblock.index = (archive.cblock.index + 1) % len(archive.blockRanges)
		// A block's byte capacity is only an estimate. Grow before reusing
		// a block that still contains retained samples, including within one
		// large UpdateMany call. Extending after the call cannot recover it.
		if next := archive.blockRanges[nblock.index]; next.start != 0 && next.end >= dps[0].interval-archive.MaxRetention()+archive.secondsPerPoint {
			rets := make([]*Retention, len(whisper.archives))
			for i, arc := range whisper.archives {
				ret := arc.Retention
				if arc == archive {
					ret.blockCount *= 2
				}
				rets[i] = &ret
			}
			if err := whisper.rewrite(rets, "grow", nil); err != nil {
				return rotated, blockBuffer, err
			}
			whisper.Extended = true
			continue
		}
		nblock.lastByteBitPos = 7
		nblock.lastByteOffset = archive.blockOffset(nblock.index)
		archive.cblock = nblock
		archive.blockRanges[nblock.index].start = 0
		archive.blockRanges[nblock.index].end = 0

		rotated = true
	}

	return rotated, blockBuffer, nil
}

func (whisper *Whisper) extendIfNeeded() error {
	rets, extend, msg := whisper.computeExtendedRetentions()
	if !extend {
		return nil
	}

	if debugExtend {
		fmt.Println("extend:", whisper.file.Name(), msg)
	}

	if err := whisper.rewrite(rets, "extend", nil); err != nil {
		return err
	}
	whisper.Extended = true

	return nil
}

// computeExtendedRetentions reports the retentions the file should be rewritten
// with, and whether any archive has outgrown its estimated compressed point
// size and therefore needs the rewrite at all.
func (whisper *Whisper) computeExtendedRetentions() (rets []*Retention, extend bool, msg string) {
	for _, arc := range whisper.archives {
		ret := &Retention{
			secondsPerPoint:        arc.secondsPerPoint,
			numberOfPoints:         arc.numberOfPoints,
			avgCompressedPointSize: arc.avgCompressedPointSize,
			blockCount:             arc.blockCount,
		}

		totalPoints, totalBlocks := arc.blockStatsBeforeCurrent()
		if totalPoints > 0 {
			avgPointSize := float32(totalBlocks*arc.blockSize) / float32(totalPoints)
			if avgPointSize > arc.avgCompressedPointSize {
				extend = true
				if avgPointSize-arc.avgCompressedPointSize < sizeEstimationBuffer {
					avgPointSize += sizeEstimationBuffer
				}
				if debugExtend {
					msg += fmt.Sprintf("%s:%v->%v ", ret, ret.avgCompressedPointSize, avgPointSize)
				}
				ret.avgCompressedPointSize = avgPointSize
				arc.stats.extended++
			}
		}

		rets = append(rets, ret)
	}

	return rets, extend, msg
}

// rewrite rebuilds the whole compressed file under the given retentions by
// streaming populated blocks into a sibling temp file, which is then renamed
// into place. Compaction can copy unchanged leading blocks without decoding.
// op names the operation and its temp suffix
// ("extend", "compact").
//
// extra optionally supplies additional points to merge into archive i as it is
// rewritten. They must be sorted ascending by interval. Where an interval is
// already present in the file, the file's value wins unless the extra point is
// marked replace. This is how the out-of-order sidecar is folded back in; pass
// nil for a plain rewrite.
func (whisper *Whisper) rewrite(rets []*Retention, op string, extra func(archiveIndex int) []extraPoint) error {
	oldArchives := whisper.archives
	var mixSpecs []MixAggregationSpec
	var mixSizes = make(map[int][]float32)
	var nferrs []error

	filename := whisper.file.Name()
	tmpname := auxiliaryPath(filename, "."+op)
	if err := os.Remove(tmpname); err != nil && !os.IsNotExist(err) {
		nferrs = append(nferrs, err)
	}

	if whisper.aggregationMethod == Mix && len(rets) > 1 {
		rets, mixSpecs, mixSizes = extractMixSpecs(rets, whisper.archives)
	}

	pointsPerBlock := DefaultPointsPerBlock
	if extra != nil {
		// Preserve compaction's block geometry when no extension is needed.
		pointsPerBlock = whisper.pointsPerBlock
	}
	nwhisper, err := CreateWithOptions(
		tmpname, rets,
		whisper.aggregationMethod, whisper.xFilesFactor,
		&Options{
			Compressed:                 true,
			PointsPerBlock:             pointsPerBlock,
			InMemory:                   whisper.opts.InMemory,
			MixAggregationSpecs:        mixSpecs,
			MixAvgCompressedPointSizes: mixSizes,
		},
	)
	if err != nil {
		return fmt.Errorf("%s: %s", op, err)
	}

	// Every error return between here and the explicit close below abandons
	// nwhisper, so without this its descriptor and temp file are leaked. A
	// sustained disk error storm rewriting many metrics would otherwise walk the
	// process into EMFILE. The explicit close sets nwhisperClosed, so the
	// handed-off handle is never closed twice, and by the time nwhisper is
	// reused for the reopen below this is already a no-op.
	nwhisperClosed := false
	defer func() {
		if nwhisperClosed {
			return
		}
		_ = nwhisper.file.Close()
		if whisper.opts.InMemory {
			releaseMemFile(tmpname)
		} else {
			_ = os.Remove(tmpname)
		}
	}()

	for i := len(whisper.archives) - 1; i >= 0; i-- {
		archive := whisper.archives[i]
		if op != "batch" {
			copy(nwhisper.archives[i].buffer, archive.buffer)
		}
		nwhisper.archives[i].stats = archive.stats

		var pending []extraPoint
		if extra != nil {
			pending = extra(i)
			// Buffered values are served after encoded blocks. Apply the same
			// replacement precedence there or an old buffer would hide the
			// corrected value just written into a block during compaction.
			buffer := nwhisper.archives[i].buffer
			for _, point := range pending {
				if !point.replace {
					continue
				}
				for offset := 0; offset < len(buffer); offset += PointSize {
					if unpackDataPoint(buffer[offset:offset+PointSize]).interval == point.interval {
						if op == "rollup" && point.interval <= archive.cblock.pn1.interval {
							// This replacement and its downstream rollups are now
							// encoded. A later flush must not divert it as a gap fill.
							for j := offset; j < offset+PointSize; j++ {
								buffer[j] = 0
							}
						} else {
							copy(buffer[offset:offset+PointSize], point.dataPoint.Bytes())
						}
					}
				}
			}
		}

		// Blocks come back ascending by start and each block's points are
		// ascending, so the whole stream is ascending and pending can be drained
		// against it in order. appendToBlockAndRotate requires that.
		buf := make([]byte, archive.blockSize)
		var dst []dataPoint
		var mergeBuffer []dataPoint
		var blockBuffer []byte
		target := nwhisper.archives[i]
		copyPrefix := (op == "compact" || op == "rollup") && whisper.compVersion == nwhisper.compVersion &&
			archive.blockSize == target.blockSize && archive.blockCount <= target.blockCount
		for _, block := range archive.getSortedBlockRanges() {
			if op == "batch" {
				break
			}
			// Empty blocks have no points, even if their unused bytes contain
			// old data. Decoding zero-filled capacity also builds a large stream
			// of zero timestamps that the encoder would only discard again.
			if block.start == 0 {
				continue
			}
			if err := whisper.fileReadAt(buf, int64(archive.blockOffset(block.index))); err != nil {
				return fmt.Errorf("archives[%d].blocks[%d].file.read: %s", i, block.index, err)
			}
			if copyPrefix && (len(pending) == 0 || pending[0].interval > block.end) {
				if err := target.copyCompressedBlock(archive, block, buf); err != nil {
					return fmt.Errorf("archives[%d].blocks[%d].copy: %w", i, block.index, err)
				}
				continue
			}
			// Extra points can shift all later block boundaries. Only copy the
			// unchanged prefix; stream the remainder through the usual encoder.
			copyPrefix = false
			// count is the last point's index, not its length. Treat disk
			// metadata as a hint bounded by the two-bit minimum encoding.
			if block.count >= 0 && block.count < maxInt && block.count/4 < len(buf) && cap(dst) < block.count+1 {
				dst = make([]dataPoint, 0, block.count+1)
			}
			dst, _, err = archive.ReadFromBlock(buf, dst[:0], 0, maxInt)
			if err != nil {
				return fmt.Errorf("archives[%d].blocks[%d].read: %s", i, block.index, err)
			}

			var before []extraPoint
			before, pending = splitBefore(pending, block.start)
			if len(before) > 0 {
				if _, blockBuffer, err = nwhisper.archives[i].appendToBlockAndRotateWithBuffer(dataPointsOf(before), blockBuffer); err != nil {
					return fmt.Errorf("archives[%d].extra.write: %s", i, err)
				}
			}
			var merged []dataPoint
			merged, pending = mergeExtraWithBuffer(dst, pending, block.end, &mergeBuffer)

			if _, blockBuffer, err = nwhisper.archives[i].appendToBlockAndRotateWithBuffer(merged, blockBuffer); err != nil {
				return fmt.Errorf("archives[%d].blocks[%d].write: %s", i, block.index, err)
			}
		}

		if len(pending) > 0 {
			if _, _, err := nwhisper.archives[i].appendToBlockAndRotateWithBuffer(dataPointsOf(pending), blockBuffer); err != nil {
				return fmt.Errorf("archives[%d].extra.write: %s", i, err)
			}
		}

	}
	if err := nwhisper.WriteHeaderCompressed(); err != nil {
		return fmt.Errorf("%s: failed to write header: %w", op, err)
	}

	lockFile := whisper.lockFile
	whisper.lockFile = nil
	lockTransferred := false
	defer func() {
		if lockFile != nil && !lockTransferred {
			_ = lockFile.Close()
		}
	}()

	if err := whisper.Close(); err != nil {
		nferrs = append(nferrs, err)
	}
	if err := nwhisper.file.Close(); err != nil {
		nferrs = append(nferrs, err)
	}
	nwhisperClosed = true

	if whisper.opts.InMemory {
		whisper.file.(*memFile).data = nwhisper.file.(*memFile).data
		releaseMemFile(tmpname)
	} else if err = os.Rename(tmpname, filename); err != nil {
		return fmt.Errorf("%s/rename: %s", op, err)
	}

	// whisper.Close() above released the cached sidecar handle, and the copy
	// below replaces every field. OpenWithOptions re-derives oooPath via
	// detectOOO, so only what a stat cannot tell us has to be carried over.
	oooPoints, oooBroken := whisper.OutOfOrderPoints, whisper.oooBroken

	nwhisper, err = openWithOptions(filename, whisper.opts, lockFile)
	if err != nil {
		return fmt.Errorf("%s/reopen: %w", op, err)
	}
	lockTransferred = true
	*whisper = *nwhisper
	for _, arc := range whisper.archives {
		arc.whisper = whisper
	}
	// Growth may run while a writer holds an archive pointer. Keep those
	// pointers valid and attach them to the reopened file and buffers.
	if op == "grow" {
		for i, arc := range whisper.archives {
			*oldArchives[i] = *arc
			oldArchives[i].whisper = whisper
		}
		for i := range oldArchives {
			if i+1 < len(oldArchives) {
				oldArchives[i].next = oldArchives[i+1]
			}
		}
		whisper.archives = oldArchives
	}
	whisper.OutOfOrderPoints, whisper.oooBroken = oooPoints, oooBroken
	if oooBroken {
		whisper.oooPath = ""
	}
	whisper.NonFatalErrors = append(whisper.NonFatalErrors, nferrs...)

	return err
}

// copyCompressedBlock relocates an unchanged leading block into the next output
// slot. Sealed blocks need no decoder state. The active tail needs its encoder
// state relocated too, so appending after compaction resumes at the right bit.
// The caller checks the format version, block size and destination capacity.
func (target *archiveInfo) copyCompressedBlock(source *archiveInfo, block blockRange, data []byte) error {
	index := target.cblock.index
	if err := target.whisper.fileWriteAt(data, int64(target.blockOffset(index))); err != nil {
		return err
	}
	sourceIndex := block.index
	block.index = index
	target.blockRanges[index] = block
	if sourceIndex == source.cblock.index {
		target.cblock = source.cblock
		target.cblock.index = index
		target.cblock.lastByteOffset += target.blockOffset(index) - source.blockOffset(sourceIndex)
	} else {
		next := (index + 1) % len(target.blockRanges)
		target.cblock = blockInfo{index: next, lastByteBitPos: 7, lastByteOffset: target.blockOffset(next)}
	}
	return nil
}

// splitBefore peels off the leading run of points strictly older than interval.
// A zero interval marks an empty block and matches nothing.
func splitBefore(points []extraPoint, interval int) (before, rest []extraPoint) {
	if interval <= 0 {
		return nil, points
	}

	n := 0
	for n < len(points) && points[n].interval < interval {
		n++
	}

	return points[:n], points[n:]
}

// dataPointsOf strips the merge flag, for the encoder.
func dataPointsOf(ps []extraPoint) []dataPoint {
	dps := make([]dataPoint, len(ps))
	for i, p := range ps {
		dps[i] = p.dataPoint
	}

	return dps
}

// mergeExtra merges the leading run of extra points at or before end into the
// ascending block points. Where both hold the same interval the block's value
// wins - the compressed file stays authoritative - unless the extra point is
// marked replace, which recomputed aggregates are (see recomputeAggregates).
func mergeExtra(block []dataPoint, extra []extraPoint, end int) (merged []dataPoint, rest []extraPoint) {
	var buffer []dataPoint
	return mergeExtraWithBuffer(block, extra, end, &buffer)
}

// buffer must not alias block; the decoder and merger retain separate scratch
// space so a merge does not replace or overwrite the next decode's buffer.
func mergeExtraWithBuffer(block []dataPoint, extra []extraPoint, end int, buffer *[]dataPoint) (merged []dataPoint, rest []extraPoint) {
	if end <= 0 {
		return block, extra
	}

	n := 0
	for n < len(extra) && extra[n].interval <= end {
		n++
	}
	head, rest := extra[:n], extra[n:]
	if len(head) == 0 {
		return block, rest
	}

	if cap(*buffer) < len(block)+len(head) {
		*buffer = make([]dataPoint, 0, len(block)+len(head))
	}
	merged = (*buffer)[:0]
	i, j := 0, 0
	for i < len(block) && j < len(head) {
		switch {
		case block[i].interval < head[j].interval:
			merged = append(merged, block[i])
			i++
		case block[i].interval > head[j].interval:
			merged = append(merged, head[j].dataPoint)
			j++
		case head[j].replace:
			merged = append(merged, head[j].dataPoint)
			i++
			j++
		default:
			merged = append(merged, block[i])
			i++
			j++
		}
	}
	merged = append(merged, block[i:]...)
	for _, point := range head[j:] {
		merged = append(merged, point.dataPoint)
	}
	*buffer = merged

	return merged, rest
}

func extractMixSpecs(orets Retentions, arcs []*archiveInfo) (Retentions, []MixAggregationSpec, map[int][]float32) {
	var nrets Retentions
	var specs []MixAggregationSpec
	var sizes = make(map[int][]float32)
	var specsCont bool

	for i, ret := range orets {
		sizes[ret.secondsPerPoint] = append(sizes[ret.secondsPerPoint], ret.avgCompressedPointSize)

		if len(nrets) == 0 {
			nrets = append(nrets, ret)
			continue
		}

		if ret.secondsPerPoint != nrets[len(nrets)-1].secondsPerPoint {
			nrets = append(nrets, ret)

			if len(specs) == 0 {
				specs = append(specs, *arcs[i].aggregationSpec)
				specsCont = true
			} else {
				specsCont = false
			}
		} else if specsCont {
			specs = append(specs, *arcs[i].aggregationSpec)
		}
	}

	return nrets, specs, sizes
}

func (arc *archiveInfo) avgPointsPerBlockReal() float32 {
	var totalPoints int
	var totalBlocks int
	for _, b := range arc.getSortedBlockRanges() {
		if b.index == arc.cblock.index {
			break
		}

		totalBlocks++
		totalPoints += b.count
	}
	if totalPoints > 0 {
		return float32(totalBlocks*arc.blockSize) / float32(totalPoints)
	}
	return 0
}

// Timestamp:
// 1. The block header stores the starting time stamp, t−1,
// which is aligned to a two hour window; the first time
// stamp, t0, in the block is stored as a delta from t−1 in
// 14 bits. 1
// 2. For subsequent time stamps, tn:
// (a) Calculate the delta of delta:
// D = (tn − tn−1) − (tn−1 − tn−2)
// (b) If D is zero, then store a single ‘0’ bit
// (c) If D is between [-63, 64], store ‘10’ followed by
// the value (7 bits)
// (d) If D is between [-255, 256], store ‘110’ followed by
// the value (9 bits)
// (e) if D is between [-2047, 2048], store ‘1110’ followed
// by the value (12 bits)
// (f) Otherwise store ‘1111’ followed by D using 32 bits
//
// Value:
// 1. The first value is stored with no compression
// 2. If XOR with the previous is zero (same value), store
// single ‘0’ bit
// 3. When XOR is non-zero, calculate the number of leading
// and trailing zeros in the XOR, store bit ‘1’ followed
// by either a) or b):
// 	(a) (Control bit ‘0’) If the block of meaningful bits
// 	    falls within the block of previous meaningful bits,
// 	    i.e., there are at least as many leading zeros and
// 	    as many trailing zeros as with the previous value,
// 	    use that information for the block position and
// 	    just store the meaningful XORed value.
// 	(b) (Control bit ‘1’) Store the length of the number
// 	    of leading zeros in the next 5 bits, then store the
// 	    length of the meaningful XORed value in the next
// 	    6 bits. Finally store the meaningful bits of the
// 	    XORed value.

func (a *archiveInfo) AppendPointsToBlock(buf []byte, ps []dataPoint) (written int, left []dataPoint, rotate bool) {
	var bw bitsWriter
	bw.buf = buf
	bw.bitPos = a.cblock.lastByteBitPos

	// set and clean possible end-of-block maker
	bw.buf[0] = a.cblock.lastByte
	bw.buf[0] &= 0xFF ^ (1<<uint(a.cblock.lastByteBitPos+1) - 1)
	bw.buf[1] = 0

	defer func() {
		a.cblock.lastByte = bw.buf[bw.index]
		a.cblock.lastByteBitPos = int(bw.bitPos)
		a.cblock.lastByteOffset += bw.index
		written = bw.index // size not including eob

		// write end-of-block marker if there is enough space
		bw.Write(4, 0x0f)
		bw.Write(32, 0)

		// exclude last byte from crc32 unless block is full
		if rotate {
			blockEnd := a.blockOffset(a.cblock.index) + a.blockSize - 1
			if left := blockEnd - a.cblock.lastByteOffset - (bw.index - written); left > 0 {
				bw.index += left
			}

			a.cblock.crc32 = crc32(buf[:bw.index+1], a.cblock.crc32)
			a.cblock.lastByteOffset = blockEnd
		} else if written > 0 {
			// exclude eob for crc32 when block isn't full
			a.cblock.crc32 = crc32(buf[:written], a.cblock.crc32)
		}

		written = bw.index + 1

		a.blockRanges[a.cblock.index].start = a.cblock.p0.interval
		a.blockRanges[a.cblock.index].end = a.cblock.pn1.interval
		a.blockRanges[a.cblock.index].count = a.cblock.count
		a.blockRanges[a.cblock.index].crc32 = a.cblock.crc32
	}()

	if debugCompress {
		fmt.Printf("AppendPointsToBlock(%s): cblock.index=%d bw.index = %d lastByteOffset = %d blockSize = %d\n", a.Retention, a.cblock.index, bw.index, a.cblock.lastByteOffset, a.blockSize)
	}

	// TODO: return error if interval is not monotonically increasing?

	for i, p := range ps {
		if p.interval == 0 {
			continue
		} else if p.interval <= a.cblock.pn1.interval {
			a.stats.discard.oldInterval++
			continue
		}

		oldBwIndex := bw.index
		oldBwBitPos := bw.bitPos
		oldBwLastByte := bw.buf[bw.index]

		var delta1, delta2 int
		if a.cblock.p0.interval == 0 {
			a.cblock.p0 = p
			a.cblock.pn1 = p
			a.cblock.pn2 = p

			copy(buf, p.Bytes())
			bw.index += PointSize

			if debugCompress {
				fmt.Printf("begin\n")
				fmt.Printf("%d: %v\n", p.interval, p.value)
			}

			continue
		}

		delta1 = p.interval - a.cblock.pn1.interval
		delta2 = a.cblock.pn1.interval - a.cblock.pn2.interval
		delta := (delta1 - delta2) / a.secondsPerPoint

		if debugCompress {
			fmt.Printf("%d %d: %v\n", i, p.interval, p.value)
		}

		// TODO: use two's complement instead to extend delta range?
		if delta == 0 {
			if debugCompress {
				fmt.Printf("\tbuf.index = %d/%d delta = %d: %0s\n", bw.bitPos, bw.index, delta, dumpBits(1, 0))
			}

			bw.Write(1, 0)
			a.stats.interval.len1++
		} else if -63 < delta && delta < 64 {
			if delta < 0 {
				delta *= -1
				delta |= 64
			}

			if debugCompress {
				fmt.Printf("\tbuf.index = %d/%d delta = %d: %0s\n", bw.bitPos, bw.index, delta, dumpBits(2, 2, 7, uint64(delta)))
			}

			bw.Write(2, 2)
			bw.Write(7, uint64(delta))
			a.stats.interval.len9++
		} else if -255 < delta && delta < 256 {
			if delta < 0 {
				delta *= -1
				delta |= 256
			}
			if debugCompress {
				fmt.Printf("\tbuf.index = %d/%d delta = %d: %0s\n", bw.bitPos, bw.index, delta, dumpBits(3, 6, 9, uint64(delta)))
			}

			bw.Write(3, 6)
			bw.Write(9, uint64(delta))
			a.stats.interval.len12++
		} else if -2047 < delta && delta < 2048 {
			if delta < 0 {
				delta *= -1
				delta |= 2048
			}

			if debugCompress {
				fmt.Printf("\tbuf.index = %d/%d delta = %d: %0s\n", bw.bitPos, bw.index, delta, dumpBits(4, 14, 12, uint64(delta)))
			}

			bw.Write(4, 14)
			bw.Write(12, uint64(delta))
			a.stats.interval.len16++
		} else {
			if debugCompress {
				fmt.Printf("\tbuf.index = %d/%d delta = %d: %0s\n", bw.bitPos, bw.index, delta, dumpBits(4, 15, 32, uint64(delta)))
			}

			bw.Write(4, 15)
			bw.Write(32, uint64(p.interval))
			a.stats.interval.len36++
		}

		pn1val := math.Float64bits(a.cblock.pn1.value)
		pn2val := math.Float64bits(a.cblock.pn2.value)
		val := math.Float64bits(p.value)
		pxor := pn1val ^ pn2val
		xor := pn1val ^ val

		if debugCompress {
			fmt.Printf("  %v %016x\n", a.cblock.pn2.value, pn2val)
			fmt.Printf("  %v %016x\n", a.cblock.pn1.value, pn1val)
			fmt.Printf("  %v %016x\n", p.value, val)
			fmt.Printf("  pxor: %016x (%064b)\n  xor:  %016x (%064b)\n", pxor, pxor, xor, xor)
		}

		if xor == 0 {
			bw.Write(1, 0)
			if debugCompress {
				fmt.Printf("\tsame, write 0\n")
			}

			a.stats.value.same++
		} else {
			plz := bits.LeadingZeros64(pxor)
			lz := bits.LeadingZeros64(xor)
			ptz := bits.TrailingZeros64(pxor)
			tz := bits.TrailingZeros64(xor)
			if plz <= lz && ptz <= tz {
				mlen := 64 - plz - ptz // meaningful block size
				bw.Write(2, 2)
				bw.Write(mlen, xor>>uint64(ptz))
				if debugCompress {
					fmt.Printf("\tsame-length meaningful block: %0s\n", dumpBits(2, 2, uint64(mlen), xor>>uint(ptz)))
				}

				a.stats.value.sameLen++
			} else {
				if lz >= 1<<5 {
					lz = 31 // 11111
				}
				mlen := 64 - lz - tz // meaningful block size
				wmlen := mlen

				if mlen == 64 {
					mlen = 63
				} else if mlen == 63 {
					wmlen = 64
				} else {
					xor >>= uint64(tz)
				}

				if debugCompress {
					fmt.Printf("lz = %+v\n", lz)
					fmt.Printf("mlen = %+v\n", mlen)
					fmt.Printf("xor mblock = %08b\n", xor)
				}

				bw.Write(2, 3)
				bw.Write(5, uint64(lz))
				bw.Write(6, uint64(mlen))
				bw.Write(wmlen, xor)
				if debugCompress {
					fmt.Printf("\tvaried-length meaningful block: %0s\n", dumpBits(2, 3, 5, uint64(lz), 6, uint64(mlen), uint64(wmlen), xor))
				}

				a.stats.value.variedLen++
			}
		}

		if bw.isFull() || bw.index+a.cblock.lastByteOffset+endOfBlockSize >= a.blockOffset(a.cblock.index)+a.blockSize {
			rotate = bw.index+a.cblock.lastByteOffset+endOfBlockSize >= a.blockOffset(a.cblock.index)+a.blockSize

			// reset dirty buffer tail
			bw.buf[oldBwIndex] = oldBwLastByte
			for i := oldBwIndex + 1; i <= bw.index; i++ {
				bw.buf[i] = 0
			}

			bw.index = oldBwIndex
			bw.bitPos = oldBwBitPos
			left = ps[i:]

			if debugCompress {
				fmt.Printf("buffer is full, write aborted: oldBwIndex = %d oldBwLastByte = %08x\n", oldBwIndex, oldBwLastByte)
			}

			break
		}

		a.cblock.pn2 = a.cblock.pn1
		a.cblock.pn1 = p
		a.cblock.count++

		if debugCompress {
			start := oldBwIndex
			end := bw.index + 2
			if end > len(bw.buf) {
				end = len(bw.buf) - 1
			}
			eob := bw.index + a.cblock.lastByteOffset
			size := eob - a.blockOffset(a.cblock.index)
			fmt.Printf("buf[%d-%d](index=%d len=%d eob=%d size=%d/%d): %08b\n", start, end, bw.index, len(bw.buf), eob, size, a.blockSize, bw.buf[start:end])
		}
	}

	return
}

type bitsWriter struct {
	buf    []byte
	index  int // index
	bitPos int // 0 indexed
}

func (bw *bitsWriter) isFull() bool {
	return bw.index+1 >= len(bw.buf)
}

func mask(l int) uint {
	return (1 << uint(l)) - 1
}

func (bw *bitsWriter) Write(lenb int, data uint64) {
	buf := make([]byte, 8)
	switch {
	case lenb <= 8:
		buf[0] = byte(data)
	case lenb <= 16:
		binary.LittleEndian.PutUint16(buf, uint16(data))
	case lenb <= 32:
		binary.LittleEndian.PutUint32(buf, uint32(data))
	case lenb <= 64:
		binary.LittleEndian.PutUint64(buf, data)
	default:
		panic(fmt.Sprintf("write size = %d > 64", lenb))
	}

	index := bw.index
	end := bw.index + 5
	if debugBitsWrite {
		if end >= len(bw.buf) {
			end = len(bw.buf) - 1
		}
		fmt.Printf("bw.bitPos = %+v\n", bw.bitPos)
		fmt.Printf("bw.buf = %08b\n", bw.buf[bw.index:end])
	}

	for _, b := range buf {
		if lenb <= 0 || bw.isFull() {
			break
		}

		if bw.bitPos+1 > lenb {
			bw.buf[bw.index] |= b << uint(bw.bitPos+1-lenb)
			bw.bitPos -= lenb
			lenb = 0
		} else {
			var left int
			if lenb < 8 {
				left = lenb - 1 - bw.bitPos
				lenb = 0
			} else {
				left = 7 - bw.bitPos
				lenb -= 8
			}
			bw.buf[bw.index] |= b >> uint(left)

			if bw.index == len(bw.buf)-1 {
				break
			}
			bw.index++
			bw.buf[bw.index] |= (b & byte(mask(left))) << uint(8-left)
			bw.bitPos = 7 - left
		}
	}
	if debugBitsWrite {
		fmt.Printf("bw.buf = %08b\n", bw.buf[index:end])
	}
}

func (a *archiveInfo) ReadFromBlock(buf []byte, dst []dataPoint, start, end int) ([]dataPoint, int, error) {
	var br bitsReader
	br.buf = buf
	br.bitPos = 7
	br.current = PointSize

	// the first data point is not compressed
	p := unpackDataPoint(buf)
	if start <= p.interval && p.interval <= end {
		dst = append(dst, p)
	}

	pn1, pn2 := p, p
	var exitByEOB bool

readloop:
	for {
		if br.current >= len(br.buf) {
			break
		}

		var p dataPoint

		if debugCompress {
			endd := br.current + 8
			if endd >= len(br.buf) {
				endd = len(br.buf) - 1
			}
			fmt.Printf("new point %d:\n  br.index = %d/%d br.bitPos = %d byte = %08b peek(1) = %08b peek(2) = %08b peek(3) = %08b peek(4) = %08b buf[%d:%d] = %08b\n", len(dst), br.current, len(br.buf), br.bitPos, br.buf[br.current], br.Peek(1), br.Peek(2), br.Peek(3), br.Peek(4), br.current, endd, br.buf[br.current:endd])
		}

		var skip, toRead int
		switch {
		case br.Peek(1) == 0: //  0xxx
			skip = 0
			toRead = 1
		case br.Peek(2) == 2: //  10xx
			skip = 2
			toRead = 7
		case br.Peek(3) == 6: //  110x
			skip = 3
			toRead = 9
		case br.Peek(4) == 14: // 1110
			skip = 4
			toRead = 12
		case br.Peek(4) == 15: // 1111
			skip = 4
			toRead = 32
		default:
			if br.current >= len(buf)-1 {
				break readloop
			}
			start, endd, data := br.trailingDebug()
			return dst, br.current, fmt.Errorf("unknown timestamp prefix (archive[%d]): %04b at %d@%d, context[%d-%d] = %08b len(dst) = %d", a.secondsPerPoint, br.Peek(4), br.current, br.bitPos, start, endd, data, len(dst))
		}

		br.Read(skip)
		delta := int(br.Read(toRead))

		if debugCompress {
			fmt.Printf("\tskip = %d toRead = %d delta = %d\n", skip, toRead, delta)
		}

		switch toRead {
		case 0:
			if debugCompress {
				fmt.Println("\tended by 0 bits to read")
			}
			break readloop
		case 32:
			if delta == 0 {
				if debugCompress {
					fmt.Println("\tended by EOB")
				}

				exitByEOB = true
				break readloop
			}
			p.interval = delta

			if debugCompress {
				fmt.Printf("\tfull interval read: %d\n", delta)
			}
		default:
			// TODO: incorrect?
			if skip > 0 && delta&(1<<uint(toRead-1)) > 0 { // POC: toRead-1
				delta &= (1 << uint(toRead-1)) - 1
				delta *= -1
			}
			delta *= a.secondsPerPoint
			p.interval = 2*pn1.interval + delta - pn2.interval

			if debugCompress {
				fmt.Printf("\tp.interval = 2*%d + %d - %d = %d\n", pn1.interval, delta, pn2.interval, p.interval)
			}
		}

		if debugCompress {
			fmt.Printf("  br.index = %d/%d br.bitPos = %d byte = %08b peek(1) = %08b peek(2) = %08b\n", br.current, len(br.buf), br.bitPos, br.buf[br.current], br.Peek(1), br.Peek(2))
		}

		switch {
		case br.Peek(1) == 0: // 0x
			br.Read(1)
			p.value = pn1.value

			if debugCompress {
				fmt.Printf("\tsame as previous value %016x (%v)\n", math.Float64bits(pn1.value), p.value)
			}
		case br.Peek(2) == 2: // 10
			br.Read(2)
			xor := math.Float64bits(pn1.value) ^ math.Float64bits(pn2.value)
			lz := bits.LeadingZeros64(xor)
			tz := bits.TrailingZeros64(xor)
			val := br.Read(64 - lz - tz)
			p.value = math.Float64frombits(math.Float64bits(pn1.value) ^ (val << uint(tz)))

			if debugCompress {
				fmt.Printf("\tsame-length meaningful block\n")
				fmt.Printf("\txor: %016x val: %016x (%v)\n", val<<uint(tz), math.Float64bits(p.value), p.value)
			}
		case br.Peek(2) == 3: // 11
			br.Read(2)
			lz := br.Read(5)
			mlen := br.Read(6)
			rmlen := mlen
			if mlen == 63 {
				rmlen = 64
			}
			xor := br.Read(int(rmlen))
			if mlen < 63 {
				xor <<= uint(64 - lz - mlen)
			}
			p.value = math.Float64frombits(math.Float64bits(pn1.value) ^ xor)

			if debugCompress {
				fmt.Printf("\tvaried-length meaningful block\n")
				fmt.Printf("\txor: %016x mlen: %d val: %016x (%v)\n", xor, mlen, math.Float64bits(p.value), p.value)
			}
		}

		if br.badRead {
			if debugCompress {
				fmt.Printf("ended by badRead\n")
			}
			break
		}

		pn2 = pn1
		pn1 = p

		if start <= p.interval && p.interval <= end {
			dst = append(dst, p)
		}
		if p.interval >= end {
			if debugCompress {
				fmt.Printf("ended by hitting end interval\n")
			}
			break
		}
	}

	endOffset := br.current
	if exitByEOB && endOffset > endOfBlockSize {
		endOffset -= endOfBlockSize - 1
	}

	return dst, endOffset, nil
}

type bitsReader struct {
	buf     []byte
	current int
	bitPos  int // 0 indexed
	badRead bool
}

func (br *bitsReader) trailingDebug() (start, end int, data []byte) {
	start = br.current - 1
	if br.current == 0 {
		start = 0
	}
	end = br.current + 1
	if end >= len(br.buf) {
		end = len(br.buf) - 1
	}
	data = br.buf[start : end+1]
	return
}

func (br *bitsReader) Peek(c int) byte {
	if br.current >= len(br.buf) {
		return 0
	}
	if br.bitPos+1 >= c {
		return (br.buf[br.current] & (1<<uint(br.bitPos+1) - 1)) >> uint(br.bitPos+1-c)
	}
	if br.current+1 >= len(br.buf) {
		return 0
	}
	var b byte
	left := c - br.bitPos - 1
	b = (br.buf[br.current] & (1<<uint(br.bitPos+1) - 1)) << uint(left)
	b |= br.buf[br.current+1] >> uint(8-left)
	return b
}

func (br *bitsReader) Read(c int) uint64 {
	if c > 64 {
		panic("bitsReader can't read more than 64 bits")
	}

	var data uint64
	oldc := c
	for {
		if br.badRead = br.current >= len(br.buf); br.badRead || c <= 0 {
			// TODO: should reset data?
			// data = 0
			break
		}

		if c < br.bitPos+1 {
			data <<= uint(c)
			data |= (uint64(br.buf[br.current]>>uint(br.bitPos+1-c)) & ((1 << uint(c)) - 1))
			br.bitPos -= c
			break
		}

		data <<= uint(br.bitPos + 1)
		data |= (uint64(br.buf[br.current] & ((1 << uint(br.bitPos+1)) - 1)))
		c -= br.bitPos + 1
		br.current++
		br.bitPos = 7
		continue
	}

	var result uint64
	for i := 8; i <= 64; i += 8 {
		if oldc-i < 0 {
			result |= (data & (1<<uint(oldc%8) - 1)) << uint(i-8)
			break
		}
		result |= ((data >> uint(oldc-i)) & 0xFF) << uint(i-8)
	}
	return result
}

func dumpBits(data ...uint64) string {
	var bw bitsWriter
	bw.buf = make([]byte, 16)
	bw.bitPos = 7
	var l uint64
	for i := 0; i < len(data); i += 2 {
		bw.Write(int(data[i]), data[i+1])
		l += data[i]
	}
	return fmt.Sprintf("%08b len(%d) end_bit_pos(%d)", bw.buf[:bw.index+1], l, bw.bitPos)
}

// For archive.Buffer handling, CompressTo assumes a simple archive layout that
// higher archive will propagate to lower archive. [wrong]
//
// CompressTo should stop compression/return errors when runs into any issues (if feasible).
func (whisper *Whisper) CompressTo(dstPath string) error {
	// Note: doesn't support mix-aggregation.
	if whisper.aggregationMethod == Mix {
		return errors.New("mix aggregation policy isn't supported")
	}

	var rets []*Retention
	for _, arc := range whisper.archives {
		rets = append(rets, &Retention{secondsPerPoint: arc.secondsPerPoint, numberOfPoints: arc.numberOfPoints})
	}

	var pointsByArchives = make([][]dataPoint, len(whisper.archives))
	for i := len(whisper.archives) - 1; i >= 0; i-- {
		archive := whisper.archives[i]

		b := make([]byte, archive.Size())
		err := whisper.fileReadAt(b, archive.Offset())
		if err != nil {
			return err
		}
		points := unpackDataPointsStrict(b)
		sort.Slice(points, func(i, j int) bool {
			return points[i].interval < points[j].interval
		})

		// filter null data points
		var bound = int(time.Now().Unix()) - archive.MaxRetention()
		for i := 0; i < len(points); i++ {
			if points[i].interval >= bound {
				points = points[i:]
				break
			}
		}

		pointsByArchives[i] = points
		rets[i].avgCompressedPointSize = estimatePointSize(points, rets[i], DefaultPointsPerBlock)
	}

	dst, err := CreateWithOptions(
		dstPath, rets,
		whisper.aggregationMethod, whisper.xFilesFactor,
		&Options{FLock: true, Compressed: true, PointsPerBlock: DefaultPointsPerBlock},
	)
	if err != nil {
		return err
	}
	defer dst.Close()

	// TODO: consider support moving the last data points to buffer
	for i := len(whisper.archives) - 1; i >= 0; i-- {
		points := pointsByArchives[i]
		if _, err := dst.archives[i].appendToBlockAndRotate(points); err != nil {
			return err
		}
	}

	if err := dst.WriteHeaderCompressed(); err != nil {
		return err
	}

	// TODO: check if compression is done correctly

	return err
}

// estimatePointSize calculates point size estimation by doing an on-the-fly
// compression without changing archiveInfo state.
func estimatePointSize(ps []dataPoint, ret *Retention, pointsPerBlock int) float32 {
	// Certain number of datapoints is needed in order to  calculate a good size.
	// Because when there is not enough data point for calculation, it would make a
	// inaccurately big size.
	//
	// 30 is semi-ramdonly chosen based on a simple test.
	if len(ps) < 30 {
		return avgCompressedPointSize
	}

	var sum int
	for i := 0; i < len(ps); {
		end := i + pointsPerBlock
		if end > len(ps) {
			end = len(ps)
		}

		buf := make([]byte, pointsPerBlock*(MaxCompressedPointSize)+endOfBlockSize)
		na := archiveInfo{
			Retention:   *ret,
			offset:      0,
			blockRanges: make([]blockRange, 1),
			blockSize:   len(buf),
			cblock: blockInfo{
				index:          0,
				lastByteBitPos: 7,
				lastByteOffset: 0,
			},
		}

		size, left, _ := na.AppendPointsToBlock(buf, ps[i:end])
		if len(left) > 0 {
			i += len(ps) - len(left)
		} else {
			i += pointsPerBlock
		}
		sum += size
	}
	size := float32(sum) / float32(len(ps))
	if math.IsNaN(float64(size)) || size <= 0 {
		size = avgCompressedPointSize
	} else {
		size += sizeEstimationBuffer
	}
	return size
}

func (whisper *Whisper) IsCompressed() bool { return whisper.compressed }

// memFile is simple implementation of in-memory file system.
// Close doesn't release the file from memory, need to call releaseMemFile.
type memFile struct {
	name   string
	data   []byte
	offset int64
}

var memFiles sync.Map

func newMemFile(name string) *memFile {
	val, ok := memFiles.Load(name)
	if ok {
		val.(*memFile).offset = 0
		return val.(*memFile)
	}
	var mf memFile
	mf.name = name
	memFiles.Store(name, &mf)
	return &mf
}

func releaseMemFile(name string) { memFiles.Delete(name) }

func (mf *memFile) Fd() uintptr  { return uintptr(unsafe.Pointer(mf)) } // skipcq: GSC-G103
func (mf *memFile) Name() string { return mf.name }
func (mf *memFile) Close() error { return nil } // skipcq: RVV-B0013

func (mf *memFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		mf.offset = offset
	case 1:
		mf.offset += offset
	case 2:
		mf.offset = int64(len(mf.data)) + offset
	}
	return mf.offset, nil
}

func (mf *memFile) ReadAt(b []byte, off int64) (n int, err error) {
	n = copy(b, mf.data[off:])
	if n < len(b) {
		err = io.EOF
	}
	return
}
func (mf *memFile) WriteAt(b []byte, off int64) (n int, err error) {
	if l := int64(len(mf.data)); l <= off {
		mf.data = append(mf.data, make([]byte, off-l+1)...)
	}
	for l, i := len(mf.data[off:]), 0; i < len(b)-l; i++ {
		mf.data = append(mf.data, 0)
	}
	n = copy(mf.data[off:], b)
	if n < len(b) {
		err = io.EOF
	}
	return
}

func (mf *memFile) Read(b []byte) (n int, err error) {
	n = copy(b, mf.data[mf.offset:])
	if n < len(b) {
		err = io.EOF
	}
	mf.offset += int64(n)
	return
}
func (mf *memFile) Write(b []byte) (n int, err error) {
	n, err = mf.WriteAt(b, mf.offset)
	mf.offset += int64(n)
	return
}

func (mf *memFile) Truncate(size int64) error {
	if int64(len(mf.data)) >= size {
		mf.data = mf.data[:size]
	} else {
		mf.data = append(mf.data, make([]byte, size-int64(len(mf.data)))...)
	}
	return nil
}

func (mf *memFile) dumpOnDisk(fpath string) error { return os.WriteFile(fpath, mf.data, 0644) }

// FillCompressed backfill cwhisper files from srcw.
// The old and new whisper should have the same retention policies.
func (dstw *Whisper) FillCompressed(srcw *Whisper) error {
	defer dstw.Close()

	pointsByArchives, err := dstw.retrieveAndMerge(srcw)
	if err != nil {
		return err
	}

	var rets []*Retention
	for i, arc := range dstw.archives {
		ret := &Retention{secondsPerPoint: arc.secondsPerPoint, numberOfPoints: arc.numberOfPoints}
		points := pointsByArchives[i]

		ret.avgCompressedPointSize = estimatePointSize(points, ret, ret.calculateSuitablePointsPerBlock(dstw.pointsPerBlock))

		rets = append(rets, ret)
	}

	var mixSpecs []MixAggregationSpec
	var mixSizes = make(map[int][]float32)
	if dstw.aggregationMethod == Mix && len(rets) > 1 {
		rets, mixSpecs, mixSizes = extractMixSpecs(rets, srcw.archives)
	}

	newDst, err := CreateWithOptions(
		dstw.file.Name()+".fill", rets,
		dstw.aggregationMethod, dstw.xFilesFactor,
		&Options{
			FLock: true, Compressed: true,
			PointsPerBlock:             DefaultPointsPerBlock,
			InMemory:                   true, // need to close file if switch to non in-memory
			MixAggregationSpecs:        mixSpecs,
			MixAvgCompressedPointSizes: mixSizes,
		},
	)
	if err != nil {
		return err
	}
	defer releaseMemFile(newDst.file.Name())

	for i := len(dstw.archives) - 1; i >= 0; i-- {
		points := pointsByArchives[i]
		if _, err := newDst.archives[i].appendToBlockAndRotate(points); err != nil {
			return err
		}
		copy(newDst.archives[i].buffer, dstw.archives[i].buffer)
	}
	if err := newDst.WriteHeaderCompressed(); err != nil {
		return err
	}

	data := newDst.file.(*memFile).data
	if err := dstw.file.Truncate(int64(len(data))); err != nil {
		fmt.Printf("convert: failed to truncate %s: %s", dstw.file.Name(), err)
	}
	if err := dstw.fileWriteAt(data, 0); err != nil {
		return err
	}

	f := dstw.file
	*dstw = *newDst
	dstw.file = f

	return nil
}

func (whisper *Whisper) propagateToMixedArchivesCompressed() error {
	var lastArchive = whisper.archives[len(whisper.archives)-1]
	var largestSPP = lastArchive.secondsPerPoint
	if largestSPP == 0 {
		return nil
	}

	var firstArchive = whisper.archives[0]
	var firstStart, firstEnd = firstArchive.getRange()
	var _, lastEnd = lastArchive.getRange()

	var from int
	if lastEnd > 0 {
		from = lastEnd
	} else {
		from = firstStart
	}

	// 1s:1d,1m:30d,1h:1y,1d:10y
	// 86400,43200,8760,3650
	//
	// 7200 -> 2h
	//
	// [0    - 7200)
	// [7200 - 14400)

	// Why "- 1": always exclude the last data point to make sure it's not
	// a pre-mature propagation. propagation aggregation is "mod down", check
	// archiveInfo.AggregateInterval.
	var until = lastArchive.Interval(firstEnd) - largestSPP*5 - 1

	if until-from <= 0 {
		return nil
	}

	dps, err := whisper.fetchCompressed(int64(from), int64(until), firstArchive)
	if err != nil {
		return fmt.Errorf("mix: failed to firstArchive.fetchCompressed(%d, %d): %s", from, until, err)
	}

	adps := whisper.aggregateByArchives(dps)
	for _, arc := range whisper.archives[1:] {
		if dps := adps[arc]; len(dps) > 0 {
			if _, err := arc.appendToBlockAndRotate(dps); err != nil {
				return fmt.Errorf("mix: failed to propagate archive %s: %s", arc.Retention, err)
			}
		}
	}

	return nil
}

// NOTE: this method could be called from both read and write paths.
func (whisper *Whisper) aggregateByArchives(dps []dataPoint) (adps map[*archiveInfo][]dataPoint) {
	adps = map[*archiveInfo][]dataPoint{}

	if len(dps) == 0 {
		return // TODO: should be an error?
	}

	var spps []int
	for _, arc := range whisper.archives[1:] {
		var knownSPP bool
		for _, spp := range spps {
			knownSPP = knownSPP || (arc.secondsPerPoint == spp)
		}
		if !knownSPP {
			spps = append(spps, arc.secondsPerPoint)
		}
	}

	sort.SliceStable(dps, func(i, j int) bool { return dps[i].interval < dps[j].interval })

	type groupedDataPoint struct {
		interval int
		values   []float64
	}
	var dpsBySPP = map[int][]groupedDataPoint{}

	for i, dp := range dps {
		if i < len(dps)-1 && dps[i+1].interval == dp.interval {
			continue
		}

		for _, spp := range spps {
			interval := dp.interval - mod(dp.interval, spp) // same as archiveInfo.AggregateInterval

			if len(dpsBySPP[spp]) == 0 {
				gdp := groupedDataPoint{
					interval: interval,
					values:   []float64{dp.value},
				}

				dpsBySPP[spp] = append(dpsBySPP[spp], gdp)
				continue
			}

			gdp := &dpsBySPP[spp][len(dpsBySPP[spp])-1]
			if gdp.interval == interval {
				gdp.values = append(gdp.values, dp.value)
				continue
			}

			// check we have enough data points to propagate a value
			baseArchive := whisper.archives[0]
			knownPercent := float32(len(gdp.values)) / float32(spp/baseArchive.secondsPerPoint)
			if knownPercent < whisper.xFilesFactor {
				// clean up the last data point
				gdp.interval = interval
				gdp.values = []float64{dp.value}
				continue
			}

			gdp = &groupedDataPoint{
				interval: interval,
				values:   []float64{dp.value},
			}

			dpsBySPP[spp] = append(dpsBySPP[spp], *gdp)
			continue
		}
	}

	for _, arc := range whisper.archives[1:] {
		gdps := dpsBySPP[arc.secondsPerPoint]
		dps := make([]dataPoint, 0, len(gdps))
		_, limit := arc.getRange() // NOTE: not supporting propagation rewrite/out of order
		for _, gdp := range gdps {
			if gdp.interval <= limit {
				continue
			}

			dps = append(dps, dataPoint{})
			dp := &dps[len(dps)-1]
			dp.interval = gdp.interval

			if arc.aggregationSpec == nil {
				values := gdp.values
				dp.value = aggregate(whisper.aggregationMethod, values)
			} else if arc.aggregationSpec.Method == Percentile {
				// sorted for percentiles
				sort.Float64s(gdp.values)
				dp.value = aggregatePercentile(arc.aggregationSpec.Percentile, gdp.values)
			} else {
				dp.value = aggregate(arc.aggregationSpec.Method, gdp.values)
			}
		}
		if len(dps) == 0 {
			continue
		}

		adps[arc] = dps
	}

	return
}

// Same implementation copied from carbonapi, without using quickselect for
// keeping zero dependency.
// percentile values: 0 - 100
func aggregatePercentile(p float32, vals []float64) float64 {
	if len(vals) == 0 || p < 0 || p > 100 {
		return math.NaN()
	}

	k := (float64(len(vals)-1) * float64(p)) / 100
	index := int(math.Ceil(k))
	remainder := k - float64(int(k))
	if remainder == 0 {
		return vals[index]
	}
	return (vals[index] * remainder) + (vals[index-1] * (1 - remainder))
}

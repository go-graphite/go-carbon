package whisper

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/bits"
	"os"
	"sort"
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

func Debug(compress, bitsWrite bool) {
	debugCompress = compress
	debugBitsWrite = bitsWrite
}

func (whisper *Whisper) WriteHeaderCompressed() (err error) {
	b := make([]byte, whisper.MetadataSize())
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

		i += FreeCompressedArchiveInfoSize
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

func (whisper *Whisper) readHeaderCompressed() (err error) {
	if _, err := whisper.file.Seek(int64(len(compressedMagicString)), 0); err != nil {
		return err
	}

	offset := 0
	hlen := whisper.MetadataSize() - len(compressedMagicString)
	b := make([]byte, hlen)
	readed, err := whisper.file.Read(b)
	if err != nil {
		err = fmt.Errorf("Unable to read header: %s", err.Error())
		return
	}
	if readed != hlen {
		err = fmt.Errorf("Unable to read header: EOF")
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
		b := make([]byte, CompressedArchiveInfoSize)
		readed, err = whisper.file.Read(b)
		if err != nil || readed != CompressedArchiveInfoSize {
			err = fmt.Errorf("Unable to read compressed archive %d metadata: %s", i, err)
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
		offset += IntSize
		arc.stats.extended = uint32(unpackInt(b[offset : offset+IntSize]))
		offset += IntSize

		whisper.archives[i] = &arc
	}

	whisper.initMetaInfo()

	for i, arc := range whisper.archives {
		b := make([]byte, BlockRangeSize*arc.blockCount)
		readed, err = whisper.file.Read(b)
		if err != nil || readed != BlockRangeSize*arc.blockCount {
			err = fmt.Errorf("Unable to read archive %d block ranges: %s", i, err)
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
			return fmt.Errorf("Unable to read archive %d buffer: %s", i, err)
		} else if readed != arc.bufferSize {
			return fmt.Errorf("Unable to read archive %d buffer: readed = %d want = %d", i, readed, arc.bufferSize)
		}
	}

	return nil
}

type blockInfo struct {
	index          int
	crc32          uint32
	p0, pn1, pn2   dataPoint // pn1/pn2: points at len(block_points) - 1/2
	lastByte       byte
	lastByteOffset int
	lastByteBitPos int
	count          int
}

type blockRange struct {
	index      int
	start, end int // start and end timestamps
	count      int
	crc32      uint32
}

func (a *archiveInfo) blockOffset(blockIndex int) int {
	return a.offset + blockIndex*a.blockSize
}

func (archive *archiveInfo) getSortedBlockRanges() []blockRange {
	brs := make([]blockRange, len(archive.blockRanges))
	copy(brs, archive.blockRanges)

	sort.SliceStable(brs, func(i, j int) bool {
		istart := brs[i].start
		if brs[i].start == 0 {
			istart = math.MaxInt64
		}
		jstart := brs[j].start
		if brs[j].start == 0 {
			jstart = math.MaxInt64
		}

		return istart < jstart
	})
	return brs
}

func (archive *archiveInfo) hasBuffer() bool {
	return archive.bufferSize > 0
}

func (whisper *Whisper) fetchCompressed(start, end int64, archive *archiveInfo) ([]dataPoint, error) {
	var dst []dataPoint
	for _, block := range archive.getSortedBlockRanges() {
		if block.end >= int(start) && int(end) >= block.start {
			buf := make([]byte, archive.blockSize)
			if err := whisper.fileReadAt(buf, int64(archive.blockOffset(block.index))); err != nil {
				return nil, fmt.Errorf("fetchCompressed: %s", err)
			}

			var err error
			dst, _, err = archive.ReadFromBlock(buf, dst, int(start), int(end))
			if err != nil {
				return dst, err
			}
		}
	}
	if archive.hasBuffer() {
		dps := unpackDataPoints(archive.buffer)
		for _, p := range dps {
			if p.interval != 0 && int(start) <= p.interval && p.interval <= int(end) {
				dst = append(dst, p)
			}
		}
	}
	return dst, nil
}

func (whisper *Whisper) archiveUpdateManyCompressed(archive *archiveInfo, points []*TimeSeriesPoint) error {
	alignedPoints := alignPoints(archive, points)

	if !archive.hasBuffer() {
		return archive.appendToBlockAndRotate(alignedPoints)
	}

	baseIntervalsPerUnit, currentUnit, minInterval := archive.getBufferInfo()
	bufferUnitPointsCount := archive.next.secondsPerPoint / archive.secondsPerPoint
	for aindex := 0; aindex < len(alignedPoints); {
		dp := alignedPoints[aindex]
		bpBaseInterval := archive.AggregateInterval(dp.interval)

		// NOTE: current implementation expects data points to be monotonically
		// increasing in time
		if minInterval != 0 && bpBaseInterval < minInterval {
			archive.stats.discard.oldInterval++
			continue
		}

		// check if buffer is full
		if baseIntervalsPerUnit[currentUnit] == 0 || baseIntervalsPerUnit[currentUnit] == bpBaseInterval {
			aindex++
			baseIntervalsPerUnit[currentUnit] = bpBaseInterval

			offset := currentUnit*bufferUnitPointsCount + (dp.interval-bpBaseInterval)/archive.secondsPerPoint
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

		if len(dps) <= 0 {
			continue
		}

		if err := archive.appendToBlockAndRotate(dps); err != nil {
			// TODO: record and continue?
			return err
		}

		// propagate
		lower := archive.next
		lowerIntervalStart := archive.AggregateInterval(dps[0].interval)

		var knownValues []float64
		for _, dPoint := range dps {
			knownValues = append(knownValues, dPoint.value)
		}

		knownPercent := float32(len(knownValues)) / float32(lower.secondsPerPoint/archive.secondsPerPoint)
		// check we have enough data points to propagate a value
		if knownPercent >= whisper.xFilesFactor {
			aggregateValue := aggregate(whisper.aggregationMethod, knownValues)
			point := &TimeSeriesPoint{lowerIntervalStart, aggregateValue}

			if err := whisper.archiveUpdateManyCompressed(lower, []*TimeSeriesPoint{point}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (archive *archiveInfo) getBufferInfo() (units []int, index, min int) {
	unitCount := len(archive.buffer) / PointSize / (archive.next.secondsPerPoint / archive.secondsPerPoint)
	var max int
	for i := 0; i < unitCount; i++ {
		v := getFirstDataPointStrict(archive.getBufferByUnit(i)).interval
		if v > 0 {
			v = archive.AggregateInterval(v)
		}
		units = append(units, v)

		if max < v {
			max = v
			index = i
		}
		if min > v {
			min = v
		}
	}
	return
}

func (archive *archiveInfo) getBufferByUnit(unit int) []byte {
	count := archive.next.secondsPerPoint / archive.secondsPerPoint
	lb := unit * PointSize * count
	ub := (unit + 1) * PointSize * count
	return archive.buffer[lb:ub]
}

func (archive *archiveInfo) appendToBlockAndRotate(dps []dataPoint) error {
	whisper := archive.whisper // TODO: optimize away?

	blockBuffer := make([]byte, len(dps)*(MaxCompressedPointSize)+endOfBlockSize)

	for {
		offset := archive.cblock.lastByteOffset // lastByteOffset is updated in AppendPointsToBlock
		size, left, rotate := archive.AppendPointsToBlock(blockBuffer, dps)

		// flush block
		if size >= len(blockBuffer) {
			// TODO: panic?
			size = len(blockBuffer)
		}
		if err := whisper.fileWriteAt(blockBuffer[:size], int64(offset)); err != nil {
			return err
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
		nblock.lastByteBitPos = 7
		nblock.lastByteOffset = archive.blockOffset(nblock.index)
		archive.cblock = nblock
		archive.blockRanges[nblock.index].start = 0
		archive.blockRanges[nblock.index].end = 0
	}

	return nil
}

func (whisper *Whisper) extendIfNeeded() error {
	var rets []*Retention
	var extend bool
	var msg string
	for _, arc := range whisper.archives {
		ret := &Retention{
			secondsPerPoint:        arc.secondsPerPoint,
			numberOfPoints:         arc.numberOfPoints,
			avgCompressedPointSize: arc.avgCompressedPointSize,
			blockCount:             arc.blockCount,
		}

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
			avgPointSize := float32(totalBlocks*arc.blockSize) / float32(totalPoints)
			if avgPointSize > arc.avgCompressedPointSize {
				extend = true
				if avgPointSize-arc.avgCompressedPointSize < 0.618 {
					avgPointSize += 0.618
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

	if !extend {
		return nil
	}

	if debugExtend {
		fmt.Println("extend:", whisper.file.Name(), msg)
	}

	filename := whisper.file.Name()
	os.Remove(whisper.file.Name() + ".extend")

	nwhisper, err := CreateWithOptions(
		whisper.file.Name()+".extend", rets,
		whisper.aggregationMethod, whisper.xFilesFactor,
		&Options{Compressed: true, PointsPerBlock: DefaultPointsPerBlock, InMemory: whisper.opts.InMemory},
	)
	if err != nil {
		return fmt.Errorf("extend: %s", err)
	}

	for i := len(whisper.archives) - 1; i >= 0; i-- {
		archive := whisper.archives[i]
		copy(nwhisper.archives[i].buffer, archive.buffer)
		nwhisper.archives[i].stats = archive.stats

		for _, block := range archive.getSortedBlockRanges() {
			buf := make([]byte, archive.blockSize)
			if err := whisper.fileReadAt(buf, int64(archive.blockOffset(block.index))); err != nil {
				return fmt.Errorf("archives[%d].blocks[%d].file.read: %s", i, block.index, err)
			}
			dst, _, err := archive.ReadFromBlock(buf, []dataPoint{}, block.start, block.end)
			if err != nil {
				return fmt.Errorf("archives[%d].blocks[%d].read: %s", i, block.index, err)
			}
			if err := nwhisper.archives[i].appendToBlockAndRotate(dst); err != nil {
				return fmt.Errorf("archives[%d].blocks[%d].write: %s", i, block.index, err)
			}
		}

		nwhisper.archives[i].buffer = archive.buffer
	}
	if err := nwhisper.WriteHeaderCompressed(); err != nil {
		return fmt.Errorf("extend: failed to writer header: %s", err)
	}

	// TODO: better error handling
	whisper.Close()
	nwhisper.file.Close()

	if whisper.opts.InMemory {
		whisper.file.(*memFile).data = nwhisper.file.(*memFile).data
		releaseMemFile(filename + ".extend")
	} else {
		if err = os.Rename(filename+".extend", filename); err != nil {
			return fmt.Errorf("extend/rename: %s", err)
		}
	}

	nwhisper, err = OpenWithOptions(filename, whisper.opts)
	*whisper = *nwhisper
	whisper.Extended = true

	return err
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

	p := unpackDataPoint(buf)
	if start <= p.interval && p.interval <= end {
		dst = append(dst, p)
	}

	var pn1, pn2 *dataPoint = &p, &p
	var exitByEOB bool

readloop:
	for {
		if br.current >= len(br.buf) {
			break
		}

		var p dataPoint

		if debugCompress {
			end := br.current + 8
			if end >= len(br.buf) {
				end = len(br.buf) - 1
			}
			fmt.Printf("new point %d:\n  br.index = %d/%d br.bitPos = %d byte = %08b peek(1) = %08b peek(2) = %08b peek(3) = %08b peek(4) = %08b buf[%d:%d] = %08b\n", len(dst), br.current, len(br.buf), br.bitPos, br.buf[br.current], br.Peek(1), br.Peek(2), br.Peek(3), br.Peek(4), br.current, end, br.buf[br.current:end])
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
			start, end, data := br.trailingDebug()
			return dst, br.current, fmt.Errorf("unknown timestamp prefix (archive[%d]): %04b at %d@%d, context[%d-%d] = %08b len(dst) = %d", a.secondsPerPoint, br.Peek(4), br.current, br.bitPos, start, end, data, len(dst))
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
		pn1 = &p

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
		if err := dst.archives[i].appendToBlockAndRotate(points); err != nil {
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

func (mf *memFile) Fd() uintptr  { return uintptr(unsafe.Pointer(mf)) }
func (mf *memFile) Name() string { return mf.name }
func (mf *memFile) Close() error { return nil }

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

func (mf *memFile) dumpOnDisk(fpath string) error { return ioutil.WriteFile(fpath, mf.data, 0644) }

func (dstw *Whisper) FillCompressed(srcw *Whisper) error {
	defer dstw.Close()

	var rets []*Retention
	for _, arc := range dstw.archives {
		rets = append(rets, &Retention{secondsPerPoint: arc.secondsPerPoint, numberOfPoints: arc.numberOfPoints})
	}

	var pointsByArchives = make([][]dataPoint, len(dstw.archives))
	for i, srcArc := range srcw.archives {
		until := int(Now().Unix())
		from := until - srcArc.MaxRetention()

		srcPoints, err := srcw.Fetch(from, until)
		if err != nil {
			return err
		}
		dstPoints, err := dstw.Fetch(from, until)
		if err != nil {
			return err
		}

		points := make([]dataPoint, len(dstPoints.values))
		var lenp int
		for i, val := range dstPoints.values {
			if !math.IsNaN(val) {
			} else if !math.IsNaN(srcPoints.values[i]) {
				val = srcPoints.values[i]
			} else {
				continue
			}

			points[lenp].interval = srcPoints.fromTime + i*srcArc.secondsPerPoint
			points[lenp].value = val
			lenp++
		}
		points = points[:lenp]

		pointsByArchives[i] = points
		rets[i].avgCompressedPointSize = estimatePointSize(points, rets[i], rets[i].calculateSuitablePointsPerBlock(dstw.pointsPerBlock))
	}

	newDst, err := CreateWithOptions(
		dstw.file.Name()+".fill", rets,
		dstw.aggregationMethod, dstw.xFilesFactor,
		&Options{
			FLock: true, Compressed: true,
			PointsPerBlock: DefaultPointsPerBlock,
			InMemory:       true, // need to close file if switch to non in-memory
		},
	)
	if err != nil {
		return err
	}
	defer releaseMemFile(newDst.file.Name())

	for i := len(dstw.archives) - 1; i >= 0; i-- {
		points := pointsByArchives[i]
		if err := newDst.archives[i].appendToBlockAndRotate(points); err != nil {
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

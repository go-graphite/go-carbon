package whisper

import (
	"fmt"
	"os"
)

func (whisper *Whisper) CheckIntegrity() {
	meta := make([]byte, whisper.MetadataSize())
	if err := whisper.fileReadAt(meta, 0); err != nil {
		panic(err)
	}

	var msg string
	empty := [4]byte{}
	copy(meta[whisper.crc32Offset():], empty[:])
	metacrc := crc32(meta, 0)
	if metacrc != whisper.crc32 {
		msg += fmt.Sprintf("    header crc: disk: %08x cal: %08x\n", whisper.crc32, metacrc)
	}

	for _, arc := range whisper.archives {
		for _, block := range arc.blockRanges {
			if block.start == 0 {
				continue
			}

			buf := make([]byte, arc.blockSize)
			if err := whisper.fileReadAt(buf, int64(arc.blockOffset(block.index))); err != nil {
				panic(err)
			}
			_, _, err := arc.ReadFromBlock(buf, []dataPoint{}, 0, maxInt)
			if err != nil {
				panic(err)
			}

			endOffset := arc.blockSize
			if block.index == arc.cblock.index {
				endOffset = arc.cblock.lastByteOffset - arc.blockOffset(block.index)
			}

			crc := crc32(buf[:endOffset], 0)
			if crc != block.crc32 {
				msg += fmt.Sprintf("    archive.%d.block.%d crc32: %08x check: %08x startOffset: %d endOffset: %d/%d\n", arc.secondsPerPoint, block.index, block.crc32, crc, arc.blockOffset(block.index), endOffset, int(arc.blockOffset(block.index))+endOffset)
			}
		}
	}

	if msg != "" {
		fmt.Printf("file: %s\n%s", whisper.file.Name(), msg)
		os.Exit(1)
	}
}

func (whisper *Whisper) Dump(all, showDecompressionInfo bool) {
	debugCompress = showDecompressionInfo

	fmt.Printf("compressed:                %t\n", whisper.compressed)
	fmt.Printf("aggregation_method:        %s\n", whisper.aggregationMethod)
	fmt.Printf("max_retention:             %d\n", whisper.maxRetention)
	fmt.Printf("x_files_factor:            %f\n", whisper.xFilesFactor)
	if whisper.compressed {
		whisper.compressed = false
		ssize := whisper.Size()
		whisper.compressed = true
		csize := whisper.Size()
		var ratio float64
		if ssize != 0 {
			ratio = float64(csize) / float64(ssize)
		}

		fmt.Printf("comp_version:              %d\n", whisper.compVersion)
		fmt.Printf("points_per_block:          %d\n", whisper.pointsPerBlock)
		fmt.Printf("avg_compressed_point_size: %f\n", whisper.avgCompressedPointSize)
		fmt.Printf("crc32:                     %X\n", whisper.crc32)
		fmt.Printf("compression_ratio:         %f (compressed/standard: %d/%d)\n", ratio, csize, ssize)
	}

	fmt.Printf("archives:                  %d\n", len(whisper.archives))
	for i, arc := range whisper.archives {
		fmt.Printf("archives.%d.retention:      %s\n", i, arc.Retention)
	}

	for i, arc := range whisper.archives {
		fmt.Printf("\nArchive %d info:\n", i)
		if whisper.compressed {
			arc.dumpInfoCompressed()
		} else {
			arc.dumpInfoStandard()
		}
	}

	if !all {
		return
	}

	for i, arc := range whisper.archives {
		fmt.Printf("\nArchive %d data:\n", i)
		if whisper.compressed {
			arc.dumpDataPointsCompressed()
		} else {
			whisper.dumpDataPointsStandard(arc)
		}
	}
}

func (archive *archiveInfo) dumpInfoCompressed() {
	fmt.Printf("retention:            %s\n", archive.Retention)
	fmt.Printf("number_of_points:     %d\n", archive.numberOfPoints)
	fmt.Printf("retention:            %s\n", archive.Retention)
	fmt.Printf("buffer_size:          %d\n", archive.bufferSize)
	fmt.Printf("block_size:           %d\n", archive.blockSize)
	fmt.Printf("estimated_point_size: %f\n", archive.avgCompressedPointSize)
	fmt.Printf("real_avg_point_size:  %f\n", archive.avgPointsPerBlockReal())
	fmt.Printf("block_count:          %d\n", archive.blockCount)
	fmt.Printf("points_per_block:     %d\n", archive.calculateSuitablePointsPerBlock(archive.whisper.pointsPerBlock))
	fmt.Printf("compression_ratio:    %f (%d/%d)\n", float64(archive.blockSize*archive.blockCount)/float64(archive.Size()), archive.blockSize*archive.blockCount, archive.Size())
	fmt.Printf("cblock\n")
	fmt.Printf("  index:     %d\n", archive.cblock.index)
	fmt.Printf("  p[0].interval:     %d\n", archive.cblock.p0.interval)
	fmt.Printf("  p[n-2].interval:   %d\n", archive.cblock.pn2.interval)
	fmt.Printf("  p[n-1].interval:   %d\n", archive.cblock.pn1.interval)
	fmt.Printf("  last_byte:         %08b\n", archive.cblock.lastByte)
	fmt.Printf("  last_byte_offset:  %d\n", archive.cblock.lastByteOffset)
	fmt.Printf("  last_byte_bit_pos: %d\n", archive.cblock.lastByteBitPos)
	fmt.Printf("  crc32:             %08x\n", archive.cblock.crc32)
	fmt.Printf("  stats:\n")
	fmt.Printf("     extended:     %d\n", archive.stats.extended)
	fmt.Printf("     discard.oldInterval: %d\n", archive.stats.discard.oldInterval)

	for _, block := range archive.getSortedBlockRanges() {
		lastByteOffset := archive.blockOffset(block.index) + archive.blockSize - 1
		if block.index == archive.cblock.index {
			lastByteOffset = archive.cblock.lastByteOffset
		}
		fmt.Printf(
			"%02d: %10d - %10d count:%5d crc32:%08x start_offset:%d last_byte_offset: %d\n",
			block.index, block.start,
			block.end, block.count, block.crc32,
			archive.blockOffset(block.index), lastByteOffset,
		)
	}
}

func (arc *archiveInfo) dumpDataPointsCompressed() {
	if arc.hasBuffer() {
		arc.dumpBuffer()
	}

	for _, block := range arc.blockRanges {
		fmt.Printf("archive %s block %d @%d\n", arc.Retention, block.index, arc.blockOffset(block.index))
		if block.start == 0 {
			fmt.Printf("    [empty]\n")
			continue
		}

		buf := make([]byte, arc.blockSize)
		if err := arc.whisper.fileReadAt(buf, int64(arc.blockOffset(block.index))); err != nil {
			panic(err)
		}

		dps, _, err := arc.ReadFromBlock(buf, []dataPoint{}, 0, maxInt)
		if err != nil {
			panic(err)
		}

		endOffset := arc.blockSize
		if block.index == arc.cblock.index {
			endOffset = arc.cblock.lastByteOffset - arc.blockOffset(block.index)
		}
		crc := crc32(buf[:endOffset], 0)

		startOffset := int(arc.blockOffset(block.index))
		fmt.Printf("crc32: %08x check: %08x startOffset: %d endOffset: %d length: %d\n", block.crc32, crc, startOffset, startOffset+endOffset, endOffset)

		for i, p := range dps {
			// continue
			fmt.Printf("  % 4d %d: %v\n", i, p.interval, p.value)
		}
	}
}

func (arc *archiveInfo) dumpBuffer() {
	fmt.Printf("archive %s buffer[%d]:\n", arc.Retention, len(arc.buffer)/PointSize)
	dps := unpackDataPoints(arc.buffer)
	for i, p := range dps {
		fmt.Printf("  % 4d %d: %f\n", i, p.interval, p.value)
	}
}

func (archive *archiveInfo) dumpInfoStandard() {
	fmt.Printf("  offset: %d\n", archive.offset)
	fmt.Printf("  second per point: %d\n", archive.secondsPerPoint)
	fmt.Printf("  points: %d\n", archive.numberOfPoints)
	fmt.Printf("  retention: %s\n", archive.Retention)
	fmt.Printf("  size: %d\n", archive.Size())
}

func (whisper *Whisper) dumpDataPointsStandard(archive *archiveInfo) {
	b := make([]byte, archive.Size())
	err := whisper.fileReadAt(b, archive.Offset())
	if err != nil {
		panic(err)
	}
	points := unpackDataPoints(b)

	for i, p := range points {
		fmt.Printf("%d: %d,% 10v\n", i, p.interval, p.value)
	}
}

func GenTestArchive(buf []byte, ret Retention) *archiveInfo {
	na := archiveInfo{
		Retention:   ret,
		offset:      0,
		blockRanges: make([]blockRange, 1),
		blockSize:   len(buf),
		cblock: blockInfo{
			index:          0,
			lastByteBitPos: 7,
			lastByteOffset: 0,
		},
	}

	return &na
}
func GenDataPointSlice() []dataPoint { return []dataPoint{} }

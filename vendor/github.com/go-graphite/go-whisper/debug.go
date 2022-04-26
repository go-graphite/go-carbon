package whisper

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"time"
)

func (whisper *Whisper) CheckIntegrity() error {
	meta := make([]byte, whisper.MetadataSize())
	if err := whisper.fileReadAt(meta, 0); err != nil {
		panic(err)
	}

	// set crc32 in the header to 0 for re-calculation
	copy(meta[whisper.crc32Offset():], make([]byte, 4))

	var msg string
	var metacrc = crc32(meta, 0)
	if metacrc != whisper.crc32 {
		msg += fmt.Sprintf("    header crc: disk: %08x cal: %08x\n", whisper.crc32, metacrc)
	}

	for _, arc := range whisper.archives {
		if arc.avgCompressedPointSize <= 0.0 || math.IsNaN(float64(arc.avgCompressedPointSize)) {
			msg += fmt.Sprintf("    archive.%s has bad avgCompressedPointSize: %f\n", arc.Retention, arc.avgCompressedPointSize)
		}
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
				msg += fmt.Sprintf("    archive.%s.block.%d crc32: %08x check: %08x startOffset: %d endOffset: %d/%d\n", arc.Retention, block.index, block.crc32, crc, arc.blockOffset(block.index), endOffset, int(arc.blockOffset(block.index))+endOffset)
			}
		}
	}

	if msg != "" {
		return errors.New(msg)
	}
	return nil
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
		var agg string
		if arc.aggregationSpec != nil {
			agg = fmt.Sprintf(" (%s)", arc.aggregationSpec)
		}
		fmt.Printf("archives.%d.retention:      %s%s\n", i, arc.Retention, agg)
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
	if archive.aggregationSpec != nil {
		fmt.Printf("aggregation:          %s\n", archive.aggregationSpec)
	}

	toTime := func(t int) string { return time.Unix(int64(t), 0).Format("2006-01-02 15:04:05") }
	fmt.Printf("cblock\n")
	fmt.Printf("  index:     %d\n", archive.cblock.index)
	fmt.Printf("  p[0].interval:     %d %s\n", archive.cblock.p0.interval, toTime(archive.cblock.p0.interval))
	fmt.Printf("  p[n-2].interval:   %d %s\n", archive.cblock.pn2.interval, toTime(archive.cblock.pn2.interval))
	fmt.Printf("  p[n-1].interval:   %d %s\n", archive.cblock.pn1.interval, toTime(archive.cblock.pn1.interval))
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
			"%02d: %10d %s - %10d %s count:%5d crc32:%08x start:%d last_byte:%d end:%d\n",
			block.index,
			block.start, toTime(block.start),
			block.end, toTime(block.end),
			(func() int {
				if block.count == 0 {
					return 0
				}
				return block.count + 1
			})(), block.crc32,
			archive.blockOffset(block.index), lastByteOffset,
			archive.blockOffset(block.index)+archive.blockSize,
		)
	}
}

func (arc *archiveInfo) dumpDataPointsCompressed() {
	if arc.hasBuffer() {
		arc.dumpBuffer()
	}

	if arc.aggregationSpec != nil {
		fmt.Printf("aggregation: %s\n", arc.aggregationSpec)
	}

	toTime := func(t int) string { return time.Unix(int64(t), 0).Format("2006-01-02 15:04:05") }
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

		blockSize := arc.blockSize
		if block.index == arc.cblock.index {
			blockSize = arc.cblock.lastByteOffset - arc.blockOffset(block.index)
		}
		crc := crc32(buf[:blockSize], 0)

		startOffset := int(arc.blockOffset(block.index))
		fmt.Printf("crc32: %08x check: %08x start: %d end: %d length: %d\n", block.crc32, crc, startOffset, startOffset+blockSize, blockSize)

		for i, p := range dps {
			// continue
			fmt.Printf("  %s % 4d %d %s: %v\n", arc.String(), i, p.interval, toTime(p.interval), p.value)
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
		fmt.Printf("%s %d: %d,% 10v\n", archive.String(), i, p.interval, p.value)
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

func Compare(
	file1 string,
	file2 string,
	now int,
	ignoreBuffer bool,
	quarantinesRaw string,
	verbose bool,
	strict bool,
	muteThreshold int,
) (msg string, err error) {
	oflag := os.O_RDONLY
	db1, err := OpenWithOptions(file1, &Options{OpenFileFlag: &oflag})
	if err != nil {
		return "", err
	}
	db2, err := OpenWithOptions(file2, &Options{OpenFileFlag: &oflag})
	if err != nil {
		return "", err
	}
	var quarantines [][2]int
	if quarantinesRaw != "" {
		for _, q := range strings.Split(quarantinesRaw, ";") {
			var quarantine [2]int
			for i, t := range strings.Split(q, ",") {
				tim, err := time.Parse("2006-01-02", t)
				if err != nil {
					return "", err
				}
				quarantine[i] = int(tim.Unix())
			}
			quarantines = append(quarantines, quarantine)
		}
	}

	oldNow := Now
	Now = func() time.Time {
		if now > 0 {
			return time.Unix(int64(now), 0)
		}
		return time.Now()
	}
	defer func() { Now = oldNow }()

	var bad bool
	for index, ret := range db1.Retentions() {
		from := int(Now().Unix()) - ret.MaxRetention() + ret.SecondsPerPoint()*60
		until := int(Now().Unix())

		msg += fmt.Sprintf("%d %s: from = %+v until = %+v (%s - %s)\n", index, ret, from, until, time.Unix(int64(from), 0).Format("2006-01-02 15:04:06"), time.Unix(int64(until), 0).Format("2006-01-02 15:04:06"))

		var dps1, dps2 *TimeSeries
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()

			var err error
			dps1, err = db1.Fetch(from, until)
			if err != nil {
				panic(err)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			var err error
			dps2, err = db2.Fetch(from, until)
			if err != nil {
				panic(err)
			}
		}()

		wg.Wait()

		if ignoreBuffer {
			{
				vals := dps1.Values()
				vals[len(vals)-1] = math.NaN()
				vals[len(vals)-2] = math.NaN()
			}
			{
				vals := dps2.Values()
				vals[len(vals)-1] = math.NaN()
				vals[len(vals)-2] = math.NaN()
			}
		}

		for _, quarantine := range quarantines {
			qfrom := quarantine[0]
			quntil := quarantine[1]
			if from <= qfrom && qfrom <= until {
				qfromIndex := (qfrom - from) / ret.SecondsPerPoint()
				quntilIndex := (quntil - from) / ret.SecondsPerPoint()
				{
					vals := dps1.Values()
					for i := qfromIndex; i <= quntilIndex && i < len(vals); i++ {
						vals[i] = math.NaN()
					}
				}
				{
					vals := dps2.Values()
					for i := qfromIndex; i <= quntilIndex && i < len(vals); i++ {
						vals[i] = math.NaN()
					}
				}
			}
		}

		var vals1, vals2 int
		for _, p := range dps1.Values() {
			if !math.IsNaN(p) {
				vals1++
			}
		}
		for _, p := range dps2.Values() {
			if !math.IsNaN(p) {
				vals2++
			}
		}

		msg += fmt.Sprintf("  len1 = %d len2 = %d vals1 = %d vals2 = %d\n", len(dps1.Values()), len(dps2.Values()), vals1, vals2)

		if len(dps1.Values()) != len(dps2.Values()) {
			bad = true
			msg += fmt.Sprintf("  size doesn't match: %d != %d\n", len(dps1.Values()), len(dps2.Values()))
		}
		if vals1 != vals2 {
			bad = true
			msg += fmt.Sprintf("  values doesn't match: %d != %d (%d)\n", vals1, vals2, vals1-vals2)
		}
		var ptDiff int
		for i, p1 := range dps1.Values() {
			if len(dps2.Values()) < i {
				break
			}
			p2 := dps2.Values()[i]
			if !((math.IsNaN(p1) && math.IsNaN(p2)) || p1 == p2) {
				bad = true
				ptDiff++
				if verbose {
					msg += fmt.Sprintf("    %d: %d %v != %v\n", i, dps1.FromTime()+i*ret.SecondsPerPoint(), p1, p2)
				}
			}
		}
		msg += fmt.Sprintf("  point mismatches: %d\n", ptDiff)
		if ptDiff <= muteThreshold && !strict {
			bad = false
		}
	}
	if db1.IsCompressed() {
		if err := db1.CheckIntegrity(); err != nil {
			msg += fmt.Sprintf("integrity: %s\n%s", file1, err)
			bad = true
		}
	}
	if db2.IsCompressed() {
		if err := db2.CheckIntegrity(); err != nil {
			msg += fmt.Sprintf("integrity: %s\n%s", file2, err)
			bad = true
		}
	}

	if bad {
		err = errors.New("whispers not equal")
	}

	return msg, err
}

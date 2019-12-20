/*
	Package whisper implements Graphite's Whisper database format
*/
package whisper

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"regexp"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	// size constants
	IntSize         = 4
	FloatSize       = 4
	Float64Size     = 8
	PointSize       = 12
	MetadataSize    = 16
	ArchiveInfoSize = 12
)

const (
	Seconds = 1
	Minutes = 60
	Hours   = 3600
	Days    = 86400
	Weeks   = 86400 * 7
	Years   = 86400 * 365
)

type AggregationMethod int

const (
	Average AggregationMethod = iota + 1
	Sum
	Last
	Max
	Min
	First
)

func (am AggregationMethod) String() string {
	switch am {
	case Average:
		return "average"
	case Sum:
		return "sum"
	case First:
		return "first"
	case Last:
		return "last"
	case Max:
		return "max"
	case Min:
		return "min"
	}
	return fmt.Sprintf("%d", am)
}

type Options struct {
	Sparse bool
	FLock  bool

	Compressed     bool
	PointsPerBlock int
	PointSize      float32
	InMemory       bool
	OpenFileFlag   *int
}

func unitMultiplier(s string) (int, error) {
	switch {
	case strings.HasPrefix(s, "s"):
		return Seconds, nil
	case strings.HasPrefix(s, "m"):
		return Minutes, nil
	case strings.HasPrefix(s, "h"):
		return Hours, nil
	case strings.HasPrefix(s, "d"):
		return Days, nil
	case strings.HasPrefix(s, "w"):
		return Weeks, nil
	case strings.HasPrefix(s, "y"):
		return Years, nil
	}
	return 0, fmt.Errorf("Invalid unit multiplier [%v]", s)
}

var retentionRegexp *regexp.Regexp = regexp.MustCompile("^(\\d+)([smhdwy]+)$")

func parseRetentionPart(retentionPart string) (int, error) {
	part, err := strconv.ParseInt(retentionPart, 10, 32)
	if err == nil {
		return int(part), nil
	}
	if !retentionRegexp.MatchString(retentionPart) {
		return 0, fmt.Errorf("%v", retentionPart)
	}
	matches := retentionRegexp.FindStringSubmatch(retentionPart)
	value, err := strconv.ParseInt(matches[1], 10, 32)
	if err != nil {
		panic(fmt.Sprintf("Regex on %v is borked, %v cannot be parsed as int", retentionPart, matches[1]))
	}
	multiplier, err := unitMultiplier(matches[2])
	return multiplier * int(value), err
}

/*
  Parse a retention definition as you would find in the storage-schemas.conf of a Carbon install.
  Note that this only parses a single retention definition, if you have multiple definitions (separated by a comma)
  you will have to split them yourself.

  ParseRetentionDef("10s:14d") Retention{10, 120960}

  See: http://graphite.readthedocs.org/en/1.0/config-carbon.html#storage-schemas-conf
*/
func ParseRetentionDef(retentionDef string) (*Retention, error) {
	parts := strings.Split(retentionDef, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("Not enough parts in retentionDef [%v]", retentionDef)
	}
	precision, err := parseRetentionPart(parts[0])
	if err != nil {
		return nil, fmt.Errorf("Failed to parse precision: %v", err)
	}

	points, err := parseRetentionPart(parts[1])
	if err != nil {
		return nil, fmt.Errorf("Failed to parse points: %v", err)
	}
	points /= precision

	return &Retention{secondsPerPoint: precision, numberOfPoints: points}, err
}

func ParseRetentionDefs(retentionDefs string) (Retentions, error) {
	retentions := make(Retentions, 0)
	for _, retentionDef := range strings.Split(retentionDefs, ",") {
		retention, err := ParseRetentionDef(retentionDef)
		if err != nil {
			return nil, err
		}
		retentions = append(retentions, retention)
	}
	return retentions, nil
}

type file interface {
	Seek(offset int64, whence int) (ret int64, err error)
	Fd() uintptr
	ReadAt(b []byte, off int64) (n int, err error)
	WriteAt(b []byte, off int64) (n int, err error)
	Read(b []byte) (n int, err error)
	Name() string
	Close() error
	Write(b []byte) (n int, err error)
	Truncate(size int64) error
}

/*
	Represents a Whisper database file.
*/
type Whisper struct {
	// file *os.File
	file file

	// Metadata
	aggregationMethod AggregationMethod
	maxRetention      int
	xFilesFactor      float32
	archives          []*archiveInfo

	compressed             bool
	compVersion            uint8
	pointsPerBlock         int
	avgCompressedPointSize float32

	crc32 uint32

	opts     *Options
	Extended bool
}

// Wrappers for whisper.file operations
func (whisper *Whisper) fileWriteAt(b []byte, off int64) error {
	_, err := whisper.file.WriteAt(b, off)
	return err
}

// Wrappers for file.ReadAt operations
func (whisper *Whisper) fileReadAt(b []byte, off int64) error {
	_, err := whisper.file.ReadAt(b, off)
	return err
}

/*
	Create a new Whisper database file and write it's header.
*/
func Create(path string, retentions Retentions, aggregationMethod AggregationMethod, xFilesFactor float32) (whisper *Whisper, err error) {
	return CreateWithOptions(path, retentions, aggregationMethod, xFilesFactor, &Options{
		Sparse: false,
		FLock:  false,
	})
}

// CreateWithOptions is more customizable create function
func CreateWithOptions(path string, retentions Retentions, aggregationMethod AggregationMethod, xFilesFactor float32, options *Options) (whisper *Whisper, err error) {
	if options == nil {
		options = &Options{}
	}
	sort.Sort(retentionsByPrecision{retentions})
	if err = validateRetentions(retentions); err != nil {
		return nil, err
	}
	_, err = os.Stat(path)
	if err == nil {
		return nil, os.ErrExist
	}
	var file file
	if options.InMemory {
		file = newMemFile(path)
		err = nil
	} else {
		file, err = os.Create(path)
	}
	if err != nil {
		return nil, err
	}

	if options.FLock && !options.InMemory {
		if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
			file.Close()
			return nil, err
		}
	}
	if options.PointSize == 0 {
		options.PointSize = avgCompressedPointSize
	}
	if options.PointsPerBlock == 0 {
		options.PointsPerBlock = DefaultPointsPerBlock
	}

	whisper = new(Whisper)

	// Set the metadata
	whisper.file = file
	whisper.aggregationMethod = aggregationMethod
	whisper.xFilesFactor = xFilesFactor
	whisper.opts = options

	whisper.compressed = options.Compressed
	whisper.compVersion = 1
	whisper.pointsPerBlock = options.PointsPerBlock
	whisper.avgCompressedPointSize = options.PointSize
	for _, retention := range retentions {
		if retention.MaxRetention() > whisper.maxRetention {
			whisper.maxRetention = retention.MaxRetention()
		}
	}

	// Set the archive info
	for _, retention := range retentions {
		archive := &archiveInfo{Retention: *retention}

		if archive.avgCompressedPointSize == 0 {
			archive.avgCompressedPointSize = whisper.avgCompressedPointSize
		}
		if archive.blockCount == 0 {
			archive.blockCount = whisper.blockCount(archive)
		}

		whisper.archives = append(whisper.archives, archive)
	}

	offset := whisper.MetadataSize()
	for i, retention := range retentions {
		archive := whisper.archives[i]
		archive.offset = offset

		if whisper.compressed {
			if math.IsNaN(float64(archive.avgCompressedPointSize)) || archive.avgCompressedPointSize <= 0 {
				archive.avgCompressedPointSize = avgCompressedPointSize
			}
			if archive.avgCompressedPointSize > MaxCompressedPointSize {
				archive.avgCompressedPointSize = MaxCompressedPointSize
			}

			archive.cblock.lastByteBitPos = 7
			ppb := archive.calculateSuitablePointsPerBlock(whisper.pointsPerBlock)
			archive.blockSize = int(math.Ceil(float64(ppb)*float64(archive.avgCompressedPointSize))) + endOfBlockSize
			archive.blockRanges = make([]blockRange, archive.blockCount)
			offset += archive.blockSize * archive.blockCount

			if i > 0 {
				size := archive.secondsPerPoint / whisper.archives[i-1].secondsPerPoint * PointSize * 2
				whisper.archives[i-1].buffer = make([]byte, size)
			}
		} else {
			offset += retention.Size()
		}
	}

	if whisper.compressed {
		whisper.initMetaInfo()
		err = whisper.WriteHeaderCompressed()
	} else {
		err = whisper.writeHeader()
	}
	if err != nil {
		return nil, err
	}

	// pre-allocate file size, fallocate proved slower
	//
	// compressed format ignores sparse flag
	if options.Sparse && !options.Compressed {
		if _, err = whisper.file.Seek(int64(whisper.Size()-1), 0); err != nil {
			return nil, err
		}
		if _, err = whisper.file.Write([]byte{0}); err != nil {
			return nil, err
		}
	} else {
		if err := allocateDiskSpace(whisper.file, whisper.Size()-whisper.MetadataSize()); err != nil {
			return nil, err
		}
	}

	return whisper, nil
}

func (whisper *Whisper) blockCount(archive *archiveInfo) int {
	return int(math.Ceil(float64(archive.numberOfPoints)/float64(archive.calculateSuitablePointsPerBlock(whisper.pointsPerBlock)))) + 1
}

func allocateDiskSpace(file file, remaining int) error {
	chunkSize := 16384
	zeros := make([]byte, chunkSize)
	for remaining > chunkSize {
		if _, err := file.Write(zeros); err != nil {
			return err
		}
		remaining -= chunkSize
	}
	if _, err := file.Write(zeros[:remaining]); err != nil {
		return err
	}
	return nil
}

func validateRetentions(retentions Retentions) error {
	if len(retentions) == 0 {
		return fmt.Errorf("No retentions")
	}
	for i, retention := range retentions {
		if i == len(retentions)-1 {
			break
		}

		nextRetention := retentions[i+1]
		if !(retention.secondsPerPoint < nextRetention.secondsPerPoint) {
			return fmt.Errorf("A Whisper database may not be configured having two archives with the same precision (archive%v: %v, archive%v: %v)", i, retention, i+1, nextRetention)
		}

		if mod(nextRetention.secondsPerPoint, retention.secondsPerPoint) != 0 {
			return fmt.Errorf("Higher precision archives' precision must evenly divide all lower precision archives' precision (archive%v: %v, archive%v: %v)", i, retention.secondsPerPoint, i+1, nextRetention.secondsPerPoint)
		}

		if retention.MaxRetention() >= nextRetention.MaxRetention() {
			return fmt.Errorf("Lower precision archives must cover larger time intervals than higher precision archives (archive%v: %v seconds, archive%v: %v seconds)", i, retention.MaxRetention(), i+1, nextRetention.MaxRetention())
		}

		if retention.numberOfPoints < (nextRetention.secondsPerPoint / retention.secondsPerPoint) {
			return fmt.Errorf("Each archive must have at least enough points to consolidate to the next archive (archive%v consolidates %v of archive%v's points but it has only %v total points)", i+1, nextRetention.secondsPerPoint/retention.secondsPerPoint, i, retention.numberOfPoints)
		}
	}
	return nil
}

/*
  Open an existing Whisper database and read it's header
*/
func Open(path string) (whisper *Whisper, err error) {
	return OpenWithOptions(path, &Options{
		FLock: false,
	})
}

func OpenWithOptions(path string, options *Options) (whisper *Whisper, err error) {
	var file file
	if options.InMemory {
		file = newMemFile(path)
	} else {
		flag := os.O_RDWR
		if options.OpenFileFlag != nil {
			flag = *options.OpenFileFlag
		}
		file, err = os.OpenFile(path, flag, 0666)
	}
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			whisper = nil
			file.Close()
		}
	}()

	if options.FLock {
		if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
			return
		}
	}

	whisper = new(Whisper)
	whisper.file = file
	whisper.opts = options

	b := make([]byte, len(compressedMagicString))
	if _, err := whisper.file.Read(b); err != nil {
		return nil, fmt.Errorf("Unable to read magic string: %s", err)
	} else if string(b) == string(compressedMagicString) {
		whisper.compressed = true
	} else if _, err := whisper.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("Unable to reset file offset: %s", err)
	}

	// read the metadata
	if whisper.compressed {
		return whisper, whisper.readHeaderCompressed()
	}

	b = make([]byte, MetadataSize)
	readed, err := file.Read(b)
	offset := 0

	if err != nil {
		err = fmt.Errorf("Unable to read header: %s", err.Error())
		return
	}
	if readed != MetadataSize {
		err = fmt.Errorf("Unable to read header: EOF")
		return
	}

	a := unpackInt(b[offset : offset+IntSize])
	if a > 1024 { // support very old format. File starts with lastUpdate and has only average aggregation method
		whisper.aggregationMethod = Average
	} else {
		whisper.aggregationMethod = AggregationMethod(a)
	}
	offset += IntSize
	whisper.maxRetention = unpackInt(b[offset : offset+IntSize])
	offset += IntSize
	whisper.xFilesFactor = unpackFloat32(b[offset : offset+FloatSize])
	offset += FloatSize
	archiveCount := unpackInt(b[offset : offset+IntSize])
	offset += IntSize

	// read the archive info
	b = make([]byte, ArchiveInfoSize)

	whisper.archives = make([]*archiveInfo, 0)
	for i := 0; i < archiveCount; i++ {
		readed, err = file.Read(b)
		if err != nil || readed != ArchiveInfoSize {
			err = fmt.Errorf("Unable to read archive %d metadata: %s", i, err)
			return
		}
		whisper.archives = append(whisper.archives, unpackArchiveInfo(b))
	}

	return whisper, nil
}

func (whisper *Whisper) initMetaInfo() {
	for i, arc := range whisper.archives {
		if arc.cblock.lastByteOffset == 0 {
			arc.cblock.lastByteOffset = arc.blockOffset(arc.cblock.index)
		}
		arc.whisper = whisper

		for i := range arc.blockRanges {
			arc.blockRanges[i].index = i
		}

		if i == 0 {
			continue
		}

		prevArc := whisper.archives[i-1]
		prevArc.next = arc
		prevArc.bufferSize = arc.secondsPerPoint / prevArc.secondsPerPoint * PointSize * bufferCount
	}
}

func (whisper *Whisper) writeHeader() (err error) {
	b := make([]byte, whisper.MetadataSize())
	i := 0
	i += packInt(b, int(whisper.aggregationMethod), i)
	i += packInt(b, whisper.maxRetention, i)
	i += packFloat32(b, whisper.xFilesFactor, i)
	i += packInt(b, len(whisper.archives), i)
	for _, archive := range whisper.archives {
		i += packInt(b, archive.offset, i)
		i += packInt(b, archive.secondsPerPoint, i)
		i += packInt(b, archive.numberOfPoints, i)
	}
	_, err = whisper.file.Write(b)

	return err
}

func (whisper *Whisper) crc32Offset() int {
	return len(compressedMagicString) + VersionSize + CompressedMetadataSize - 4 - FreeCompressedMetadataSize
}

/*
  Close the whisper file
*/
func (whisper *Whisper) Close() error {
	return whisper.file.Close()
}

/*
  Calculate the total number of bytes the Whisper file should be according to the metadata.
*/
func (whisper *Whisper) Size() int {
	size := whisper.MetadataSize()
	for _, archive := range whisper.archives {
		if whisper.compressed {
			size += archive.blockSize * archive.blockCount
		} else {
			size += archive.Size()
		}
	}
	return size
}

/*
  Calculate the number of bytes the metadata section will be.
*/
func (whisper *Whisper) MetadataSize() int {
	if whisper.compressed {
		return len(compressedMagicString) + VersionSize + CompressedMetadataSize + (CompressedArchiveInfoSize * len(whisper.archives)) + whisper.blockRangesSize() + whisper.bufferSize()
	}

	return MetadataSize + (ArchiveInfoSize * len(whisper.archives))
}

func (whisper *Whisper) blockRangesSize() int {
	var blockRangesSize int
	for _, arc := range whisper.archives {
		blockRangesSize += BlockRangeSize * arc.blockCount
	}
	return blockRangesSize
}

func (whisper *Whisper) bufferSize() int {
	if len(whisper.archives) == 0 {
		return 0
	}
	var bufSize int
	for i, arc := range whisper.archives[1:] {
		bufSize += arc.secondsPerPoint / whisper.archives[i].secondsPerPoint * PointSize * 2
	}
	return bufSize
}

/* Return aggregation method */
func (whisper *Whisper) AggregationMethod() string {
	aggr := "unknown"
	switch whisper.aggregationMethod {
	case Average:
		aggr = "Average"
	case Sum:
		aggr = "Sum"
	case First:
		aggr = "First"
	case Last:
		aggr = "Last"
	case Max:
		aggr = "Max"
	case Min:
		aggr = "Min"
	}
	return aggr
}

/* Return max retention in seconds */
func (whisper *Whisper) MaxRetention() int {
	return whisper.maxRetention
}

/* Return xFilesFactor */
func (whisper *Whisper) XFilesFactor() float32 {
	return whisper.xFilesFactor
}

/* Return retentions */
func (whisper *Whisper) Retentions() []Retention {
	ret := make([]Retention, 0, 4)
	for _, archive := range whisper.archives {
		ret = append(ret, archive.Retention)
	}

	return ret
}

/*
  Update a value in the database.

  If the timestamp is in the future or outside of the maximum retention it will
  fail immediately.
*/
func (whisper *Whisper) Update(value float64, timestamp int) (err error) {
	// recover panics and return as error
	defer func() {
		if e := recover(); e != nil {
			err = errors.New(e.(string))
		}
	}()

	diff := int(time.Now().Unix()) - timestamp
	if !(diff < whisper.maxRetention && diff >= 0) {
		return fmt.Errorf("Timestamp not covered by any archives in this database")
	}
	var archive *archiveInfo
	var lowerArchives []*archiveInfo
	var i int
	for i, archive = range whisper.archives {
		if archive.MaxRetention() < diff {
			continue
		}
		lowerArchives = whisper.archives[i+1:] // TODO: investigate just returning the positions
		break
	}

	myInterval := timestamp - mod(timestamp, archive.secondsPerPoint)
	point := dataPoint{myInterval, value}

	_, err = whisper.file.WriteAt(point.Bytes(), whisper.getPointOffset(myInterval, archive))
	if err != nil {
		return err
	}

	higher := archive
	for _, lower := range lowerArchives {
		propagated, err := whisper.propagate(myInterval, higher, lower)
		if err != nil {
			return err
		} else if !propagated {
			break
		}
		higher = lower
	}

	return nil
}

func reversePoints(points []*TimeSeriesPoint) {
	size := len(points)
	end := size / 2

	for i := 0; i < end; i++ {
		points[i], points[size-i-1] = points[size-i-1], points[i]
	}
}

var Now = func() time.Time { return time.Now() }

func (whisper *Whisper) UpdateMany(points []*TimeSeriesPoint) (err error) {
	return whisper.UpdateManyForArchive(points, -1)
}

func (whisper *Whisper) UpdateManyForArchive(points []*TimeSeriesPoint, archiveSpecifier int) (err error) {
	// recover panics and return as error
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%s\n%s", e, debug.Stack())
		}
	}()

	// sort the points, newest first
	reversePoints(points)
	sort.Stable(timeSeriesPointsNewestFirst{points})

	now := int(Now().Unix()) // TODO: danger of 2030 something overflow

	var currentPoints []*TimeSeriesPoint
	for i := 0; i < len(whisper.archives); i++ {
		archive := whisper.archives[i]
		if archiveSpecifier != -1 && archiveSpecifier != archive.MaxRetention() {
			continue
		}

		currentPoints, points = extractPoints(points, now, archive.MaxRetention())

		if len(currentPoints) == 0 {
			continue
		}
		// reverse currentPoints
		reversePoints(currentPoints)
		if whisper.compressed {
			err = whisper.archiveUpdateManyCompressed(archive, currentPoints)
		} else {
			err = whisper.archiveUpdateMany(archive, currentPoints)
		}
		if err != nil {
			return
		}

		if len(points) == 0 { // nothing left to do
			break
		}
	}

	if whisper.compressed {
		if err := whisper.WriteHeaderCompressed(); err != nil {
			return err
		}

		if err := whisper.extendIfNeeded(); err != nil {
			return err
		}
	}

	return
}

func (whisper *Whisper) archiveUpdateMany(archive *archiveInfo, points []*TimeSeriesPoint) error {
	alignedPoints := alignPoints(archive, points)
	intervals, packedBlocks := packSequences(archive, alignedPoints)

	baseInterval := whisper.getBaseInterval(archive)
	if baseInterval == 0 {
		baseInterval = intervals[0]
	}

	for i := range intervals {
		myOffset := archive.PointOffset(baseInterval, intervals[i])
		bytesBeyond := int(myOffset-archive.End()) + len(packedBlocks[i])
		if bytesBeyond > 0 {
			pos := len(packedBlocks[i]) - bytesBeyond
			err := whisper.fileWriteAt(packedBlocks[i][:pos], myOffset)
			if err != nil {
				return err
			}
			err = whisper.fileWriteAt(packedBlocks[i][pos:], archive.Offset())
			if err != nil {
				return err
			}
		} else {
			err := whisper.fileWriteAt(packedBlocks[i], myOffset)
			if err != nil {
				return err
			}
		}
	}

	higher := archive
	lowerArchives := whisper.lowerArchives(archive)

	for _, lower := range lowerArchives {
		seen := make(map[int]bool)
		propagateFurther := false
		for _, point := range alignedPoints {
			interval := point.interval - mod(point.interval, lower.secondsPerPoint)
			if !seen[interval] {
				if propagated, err := whisper.propagate(interval, higher, lower); err != nil {
					panic("Failed to propagate")
				} else if propagated {
					propagateFurther = true
				}
			}
		}
		if !propagateFurther {
			break
		}
		higher = lower
	}
	return nil
}

func extractPoints(points []*TimeSeriesPoint, now int, maxRetention int) (currentPoints []*TimeSeriesPoint, remainingPoints []*TimeSeriesPoint) {
	maxAge := now - maxRetention
	for i, point := range points {
		if point.Time < maxAge {
			if i > 0 {
				return points[:i-1], points[i-1:]
			} else {
				return []*TimeSeriesPoint{}, points
			}
		}
	}
	return points, remainingPoints
}

func alignPoints(archive *archiveInfo, points []*TimeSeriesPoint) []dataPoint {
	alignedPoints := make([]dataPoint, 0, len(points))
	positions := make(map[int]int)
	for _, point := range points {
		dPoint := dataPoint{point.Time - mod(point.Time, archive.secondsPerPoint), point.Value}
		if p, ok := positions[dPoint.interval]; ok {
			alignedPoints[p] = dPoint
		} else {
			alignedPoints = append(alignedPoints, dPoint)
			positions[dPoint.interval] = len(alignedPoints) - 1
		}
	}
	return alignedPoints
}

func packSequences(archive *archiveInfo, points []dataPoint) (intervals []int, packedBlocks [][]byte) {
	intervals = make([]int, 0)
	packedBlocks = make([][]byte, 0)
	for i, point := range points {
		if i == 0 || point.interval != intervals[len(intervals)-1]+archive.secondsPerPoint {
			intervals = append(intervals, point.interval)
			packedBlocks = append(packedBlocks, point.Bytes())
		} else {
			packedBlocks[len(packedBlocks)-1] = append(packedBlocks[len(packedBlocks)-1], point.Bytes()...)
		}
	}
	return
}

/*
	Calculate the offset for a given interval in an archive

	This method retrieves the baseInterval and the
*/
func (whisper *Whisper) getPointOffset(start int, archive *archiveInfo) int64 {
	baseInterval := whisper.getBaseInterval(archive)
	if baseInterval == 0 {
		return archive.Offset()
	}
	return archive.PointOffset(baseInterval, start)
}

func (whisper *Whisper) getBaseInterval(archive *archiveInfo) int {
	if whisper.compressed {
		return unpackInt(archive.buffer)
	}

	baseInterval, err := whisper.readInt(archive.Offset())
	if err != nil {
		panic(fmt.Sprintf("Failed to read baseInterval: %s", err.Error()))
	}
	return baseInterval
}

func (whisper *Whisper) lowerArchives(archive *archiveInfo) (lowerArchives []*archiveInfo) {
	for i, lower := range whisper.archives {
		if lower.secondsPerPoint > archive.secondsPerPoint {
			return whisper.archives[i:]
		}
	}
	return
}

func (whisper *Whisper) propagate(timestamp int, higher, lower *archiveInfo) (bool, error) {
	lowerIntervalStart := timestamp - mod(timestamp, lower.secondsPerPoint)

	higherFirstOffset := whisper.getPointOffset(lowerIntervalStart, higher)

	// TODO: extract all this series extraction stuff
	higherPoints := lower.secondsPerPoint / higher.secondsPerPoint
	higherSize := higherPoints * PointSize
	relativeFirstOffset := higherFirstOffset - higher.Offset()
	relativeLastOffset := int64(mod(int(relativeFirstOffset+int64(higherSize)), higher.Size()))
	higherLastOffset := relativeLastOffset + higher.Offset()

	series, err := whisper.readSeries(higherFirstOffset, higherLastOffset, higher)
	if err != nil {
		return false, err
	}

	// and finally we construct a list of values
	knownValues := make([]float64, 0, len(series))
	currentInterval := lowerIntervalStart

	for _, dPoint := range series {
		if dPoint.interval == currentInterval {
			knownValues = append(knownValues, dPoint.value)
		}
		currentInterval += higher.secondsPerPoint
	}

	// propagate aggregateValue to propagate from neighborValues if we have enough known points
	if len(knownValues) == 0 {
		return false, nil
	}
	knownPercent := float32(len(knownValues)) / float32(len(series))
	if knownPercent < whisper.xFilesFactor { // check we have enough data points to propagate a value
		return false, nil
	} else {
		aggregateValue := aggregate(whisper.aggregationMethod, knownValues)
		point := dataPoint{lowerIntervalStart, aggregateValue}
		if _, err := whisper.file.WriteAt(point.Bytes(), whisper.getPointOffset(lowerIntervalStart, lower)); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (whisper *Whisper) readSeries(start, end int64, archive *archiveInfo) ([]dataPoint, error) {
	var b []byte
	if start < end {
		b = make([]byte, end-start)
		err := whisper.fileReadAt(b, start)
		if err != nil {
			return nil, err
		}
	} else {
		b = make([]byte, archive.End()-start)
		err := whisper.fileReadAt(b, start)
		if err != nil {
			return nil, err
		}
		b2 := make([]byte, end-archive.Offset())
		err = whisper.fileReadAt(b2, archive.Offset())
		if err != nil {
			return nil, err
		}
		b = append(b, b2...)
	}
	return unpackDataPoints(b), nil
}

func (whisper *Whisper) checkSeriesEmpty(start, end int64, archive *archiveInfo, fromTime, untilTime int) (bool, error) {
	if start < end {
		len := end - start
		return whisper.checkSeriesEmptyAt(start, len, fromTime, untilTime)
	}
	len := archive.End() - start
	empty, err := whisper.checkSeriesEmptyAt(start, len, fromTime, untilTime)
	if err != nil || !empty {
		return empty, err
	}
	return whisper.checkSeriesEmptyAt(archive.Offset(), end-archive.Offset(), fromTime, untilTime)

}

func (whisper *Whisper) checkSeriesEmptyAt(start, len int64, fromTime, untilTime int) (bool, error) {
	b1 := make([]byte, 4)
	// Read first point
	err := whisper.fileReadAt(b1, start)
	if err != nil {
		return false, err
	}
	pointTime := unpackInt(b1)
	if pointTime > fromTime && pointTime < untilTime {
		return false, nil
	}

	b2 := make([]byte, 4)
	// Read last point
	err = whisper.fileReadAt(b2, len-12)
	if err != nil {
		return false, err
	}
	pointTime = unpackInt(b1)
	if pointTime > fromTime && pointTime < untilTime {
		return false, nil
	}
	return true, nil
}

/*
  Calculate the starting time for a whisper db.
*/
func (whisper *Whisper) StartTime() int {
	now := int(Now().Unix()) // TODO: danger of 2030 something overflow
	return now - whisper.maxRetention
}

/*
  Fetch a TimeSeries for a given time span from the file.
*/
func (whisper *Whisper) Fetch(fromTime, untilTime int) (timeSeries *TimeSeries, err error) {
	now := int(Now().Unix()) // TODO: danger of 2030 something overflow
	if fromTime > untilTime {
		return nil, fmt.Errorf("Invalid time interval: from time '%d' is after until time '%d'", fromTime, untilTime)
	}
	// oldestTime := whisper.StartTime()
	oldestTime := now - whisper.maxRetention
	// range is in the future
	if fromTime > now {
		return nil, nil
	}
	// range is beyond retention
	if untilTime < oldestTime {
		return nil, nil
	}
	if fromTime < oldestTime {
		fromTime = oldestTime
	}
	if untilTime > now {
		untilTime = now
	}

	// TODO: improve this algorithm it's ugly
	diff := now - fromTime
	var archive *archiveInfo
	for _, archive = range whisper.archives {
		if archive.MaxRetention() >= diff {
			break
		}
	}

	fromInterval := archive.Interval(fromTime)
	untilInterval := archive.Interval(untilTime)

	var series []dataPoint
	if whisper.compressed {
		series, err = whisper.fetchCompressed(int64(fromInterval), int64(untilInterval), archive)
		if err != nil {
			return nil, err
		}

		irange := untilInterval - fromInterval
		values := make([]float64, irange/archive.secondsPerPoint)

		for i := range values {
			values[i] = math.NaN()
		}
		step := archive.secondsPerPoint
		for _, dPoint := range series {
			index := (dPoint.interval - fromInterval) / archive.secondsPerPoint
			if index >= len(values) {
				break
			}
			values[index] = dPoint.value
		}
		return &TimeSeries{fromInterval, untilInterval, step, values}, nil
	} else {
		baseInterval := whisper.getBaseInterval(archive)

		if baseInterval == 0 {
			step := archive.secondsPerPoint
			points := (untilInterval - fromInterval) / step
			values := make([]float64, points)
			for i := range values {
				values[i] = math.NaN()
			}
			return &TimeSeries{fromInterval, untilInterval, step, values}, nil
		}

		// Zero-length time range: always include the next point
		if fromInterval == untilInterval {
			untilInterval += archive.SecondsPerPoint()
		}

		fromOffset := archive.PointOffset(baseInterval, fromInterval)
		untilOffset := archive.PointOffset(baseInterval, untilInterval)

		series, err = whisper.readSeries(fromOffset, untilOffset, archive)
		if err != nil {
			return nil, err
		}

		values := make([]float64, len(series))
		for i := range values {
			values[i] = math.NaN()
		}
		currentInterval := fromInterval
		step := archive.secondsPerPoint

		for i, dPoint := range series {
			if dPoint.interval == currentInterval {
				values[i] = dPoint.value
			}
			currentInterval += step
		}
		return &TimeSeries{fromInterval, untilInterval, step, values}, nil
	}
}

/*
  Check a TimeSeries has a points for a given time span from the file.
*/
func (whisper *Whisper) CheckEmpty(fromTime, untilTime int) (exist bool, err error) {
	now := int(time.Now().Unix()) // TODO: danger of 2030 something overflow
	if fromTime > untilTime {
		return true, fmt.Errorf("Invalid time interval: from time '%d' is after until time '%d'", fromTime, untilTime)
	}
	oldestTime := whisper.StartTime()
	// range is in the future
	if fromTime > now {
		return true, nil
	}
	// range is beyond retention
	if untilTime < oldestTime {
		return true, nil
	}
	if fromTime < oldestTime {
		fromTime = oldestTime
	}
	if untilTime > now {
		untilTime = now
	}

	// TODO: improve this algorithm it's ugly
	diff := now - fromTime
	var archive *archiveInfo
	for _, archive = range whisper.archives {
		fromInterval := archive.Interval(fromTime)
		untilInterval := archive.Interval(untilTime)
		baseInterval := whisper.getBaseInterval(archive)

		if baseInterval == 0 {
			return true, nil
		}

		// Zero-length time range: always include the next point
		if fromInterval == untilInterval {
			untilInterval += archive.SecondsPerPoint()
		}

		fromOffset := archive.PointOffset(baseInterval, fromInterval)
		untilOffset := archive.PointOffset(baseInterval, untilInterval)

		empty, err := whisper.checkSeriesEmpty(fromOffset, untilOffset, archive, fromTime, untilTime)
		if err != nil || !empty {
			return empty, err
		}
		if archive.MaxRetention() >= diff {
			break
		}
	}
	return true, nil
}

func (whisper *Whisper) readInt(offset int64) (int, error) {
	// TODO: make errors better
	b := make([]byte, IntSize)
	_, err := whisper.file.ReadAt(b, offset)
	if err != nil {
		return 0, err
	}

	return unpackInt(b), nil
}

/*
  A retention level.

  Retention levels describe a given archive in the database. How detailed it is and how far back
  it records.
*/
type Retention struct {
	secondsPerPoint int
	numberOfPoints  int

	// for compressed whisper (internal)
	avgCompressedPointSize float32
	blockCount             int
}

func (r *Retention) MaxRetention() int                      { return r.secondsPerPoint * r.numberOfPoints }
func (r *Retention) Size() int                              { return r.numberOfPoints * PointSize }
func (r *Retention) SecondsPerPoint() int                   { return r.secondsPerPoint }
func (r *Retention) NumberOfPoints() int                    { return r.numberOfPoints }
func (r *Retention) SetAvgCompressedPointSize(size float32) { r.avgCompressedPointSize = size }

func (r *Retention) calculateSuitablePointsPerBlock(defaultSize int) int {
	if defaultSize == 0 {
		defaultSize = DefaultPointsPerBlock
	}

	if r.numberOfPoints >= defaultSize*3 {
		return defaultSize
	} else if r.numberOfPoints > 256 {
		return (r.numberOfPoints + 16) / 4
	}
	return r.numberOfPoints + 16
}

func (r Retention) String() string {
	toStr := func(v int) string {
		switch {
		case v >= 365*24*60*60:
			return fmt.Sprintf("%dy", v/(365*24*60*60))
		case v >= 24*60*60:
			return fmt.Sprintf("%dd", v/(24*60*60))
		case v >= 60*60:
			return fmt.Sprintf("%dh", v/(60*60))
		case v >= 60:
			return fmt.Sprintf("%dm", v/(60))
		default:
			return fmt.Sprintf("%ds", v)
		}
	}
	return fmt.Sprintf("%s:%s", toStr(r.secondsPerPoint), toStr(r.secondsPerPoint*r.numberOfPoints))
}

func NewRetention(secondsPerPoint, numberOfPoints int) Retention {
	return Retention{
		secondsPerPoint: secondsPerPoint,
		numberOfPoints:  numberOfPoints,
	}
}

type Retentions []*Retention

func (r Retentions) Len() int {
	return len(r)
}

func (r Retentions) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

type retentionsByPrecision struct{ Retentions }

func (r retentionsByPrecision) Less(i, j int) bool {
	return r.Retentions[i].secondsPerPoint < r.Retentions[j].secondsPerPoint
}

/*
  Describes a time series in a file.

  The only addition this type has over a Retention is the offset at which it exists within the
  whisper file.
*/
type archiveInfo struct {
	Retention
	offset int

	next    *archiveInfo
	whisper *Whisper

	// reason:
	// 	1. less file writes per point
	// 	2. less file reads & no decompressions on propagation
	buffer     []byte
	bufferSize int

	blockRanges []blockRange // TODO: remove: sorted by start
	blockSize   int
	cblock      blockInfo // mostly for quick block write

	stats struct {
		// interval and value stats are not saved on disk because they could be
		// regenerated by scanning blocks
		interval struct {
			len1, len9, len12, len16, len36 uint32
		}
		value struct {
			same, sameLen, variedLen uint32
		}

		extended uint32

		discard struct {
			oldInterval uint32
		}
	}
}

func (archive *archiveInfo) Offset() int64 {
	return int64(archive.offset)
}

func (archive *archiveInfo) PointOffset(baseInterval, interval int) int64 {
	timeDistance := interval - baseInterval
	pointDistance := timeDistance / archive.secondsPerPoint
	byteDistance := pointDistance * PointSize
	myOffset := archive.Offset() + int64(mod(byteDistance, archive.Size()))

	return myOffset
}

func (archive *archiveInfo) End() int64 {
	return archive.Offset() + int64(archive.Size())
}

func (archive *archiveInfo) Interval(time int) int {
	return time - mod(time, archive.secondsPerPoint) + archive.secondsPerPoint
}

func (archive *archiveInfo) AggregateInterval(time int) int {
	return time - mod(time, archive.next.secondsPerPoint)
}

type TimeSeries struct {
	fromTime  int
	untilTime int
	step      int
	values    []float64
}

func (ts *TimeSeries) FromTime() int {
	return ts.fromTime
}

func (ts *TimeSeries) UntilTime() int {
	return ts.untilTime
}

func (ts *TimeSeries) Step() int {
	return ts.step
}

func (ts *TimeSeries) Values() []float64 {
	return ts.values
}

func (ts *TimeSeries) Points() []TimeSeriesPoint {
	points := make([]TimeSeriesPoint, len(ts.values))
	for i, value := range ts.values {
		points[i] = TimeSeriesPoint{Time: ts.fromTime + ts.step*i, Value: value}
	}
	return points
}

func (ts *TimeSeries) PointPointers() []*TimeSeriesPoint {
	points := make([]*TimeSeriesPoint, len(ts.values))
	for i, value := range ts.values {
		points[i] = &TimeSeriesPoint{Time: ts.fromTime + ts.step*i, Value: value}
	}
	return points
}

func (ts *TimeSeries) String() string {
	return fmt.Sprintf("TimeSeries{'%v' '%-v' %v %v}", time.Unix(int64(ts.fromTime), 0), time.Unix(int64(ts.untilTime), 0), ts.step, ts.values)
}

type TimeSeriesPoint struct {
	Time  int
	Value float64
}

type timeSeriesPoints []*TimeSeriesPoint

func (p timeSeriesPoints) Len() int {
	return len(p)
}

func (p timeSeriesPoints) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

type timeSeriesPointsNewestFirst struct {
	timeSeriesPoints
}

func (p timeSeriesPointsNewestFirst) Less(i, j int) bool {
	return p.timeSeriesPoints[i].Time > p.timeSeriesPoints[j].Time
}

type dataPoint struct {
	interval int
	value    float64
}

func (point *dataPoint) Bytes() []byte {
	b := make([]byte, PointSize)
	packInt(b, point.interval, 0)
	packFloat64(b, point.value, IntSize)
	return b
}

func sum(values []float64) float64 {
	result := 0.0
	for _, value := range values {
		result += value
	}
	return result
}

func aggregate(method AggregationMethod, knownValues []float64) float64 {
	switch method {
	case Average:
		return sum(knownValues) / float64(len(knownValues))
	case Sum:
		return sum(knownValues)
	case First:
		return knownValues[0]
	case Last:
		return knownValues[len(knownValues)-1]
	case Max:
		max := knownValues[0]
		for _, value := range knownValues {
			if value > max {
				max = value
			}
		}
		return max
	case Min:
		min := knownValues[0]
		for _, value := range knownValues {
			if value < min {
				min = value
			}
		}
		return min
	}
	panic("Invalid aggregation method")
}

func packInt(b []byte, v, i int) int {
	binary.BigEndian.PutUint32(b[i:i+IntSize], uint32(v))
	return IntSize
}

func packFloat32(b []byte, v float32, i int) int {
	binary.BigEndian.PutUint32(b[i:i+FloatSize], math.Float32bits(v))
	return FloatSize
}

func packFloat64(b []byte, v float64, i int) int {
	binary.BigEndian.PutUint64(b[i:i+Float64Size], math.Float64bits(v))
	return Float64Size
}

func unpackInt(b []byte) int {
	return int(binary.BigEndian.Uint32(b))
}

func unpackFloat32(b []byte) float32 {
	return math.Float32frombits(binary.BigEndian.Uint32(b))
}

func unpackFloat64(b []byte) float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(b))
}

func unpackArchiveInfo(b []byte) *archiveInfo {
	return &archiveInfo{
		Retention: Retention{secondsPerPoint: unpackInt(b[IntSize : IntSize*2]), numberOfPoints: unpackInt(b[IntSize*2 : IntSize*3])},
		offset:    unpackInt(b[:IntSize]),
	}
}

func unpackDataPoint(b []byte) dataPoint {
	return dataPoint{unpackInt(b[0:IntSize]), unpackFloat64(b[IntSize:PointSize])}
}

func unpackDataPoints(b []byte) (series []dataPoint) {
	series = make([]dataPoint, 0, len(b)/PointSize)
	for i := 0; i < len(b); i += PointSize {
		series = append(series, unpackDataPoint(b[i:i+PointSize]))
	}
	return
}

func unpackDataPointsStrict(b []byte) (series []dataPoint) {
	series = make([]dataPoint, 0, len(b)/PointSize)
	for i := 0; i < len(b); i += PointSize {
		dp := unpackDataPoint(b[i : i+PointSize])
		if dp.interval == 0 {
			continue
		}
		series = append(series, dp)
	}
	return
}

func getFirstDataPointStrict(b []byte) dataPoint {
	for i := 0; i < len(b); i += PointSize {
		dp := unpackDataPoint(b[i : i+PointSize])
		if dp.interval == 0 {
			continue
		}
		return dp
	}
	return dataPoint{}
}

/*
	Implementation of modulo that works like Python
	Thanks @timmow for this
*/
func mod(a, b int) int {
	return a - (b * int(math.Floor(float64(a)/float64(b))))
}

// TODO: optmize with assembly
// from https://create.stephan-brumme.com/crc32/
func crc32(data []byte, prev uint32) uint32 {
	const polynomial uint32 = 0xEDB88320

	crc := prev ^ 0xFFFFFFFF
	for _, b := range data {
		crc ^= uint32(b)
		for i := 0; i < 8; i++ {
			crc = (crc >> 1) ^ (uint32(-1*int32(crc&1)) & polynomial)
		}
	}
	return crc ^ 0xFFFFFFFF
}

func (whisper *Whisper) File() *os.File { return whisper.file.(*os.File) }

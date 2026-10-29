package recovery

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"slices"
	"strings"

	"github.com/go-graphite/go-carbon/points"
)

// Format is the record format of a cache dump. Input files (the WAL written
// during a dump and the handoff tail) always hold legacy records.
type Format int

const (
	// FormatLegacy records are points.AppendBinary records, readable by
	// every go-carbon release.
	FormatLegacy Format = iota
	// FormatCompact records hold a metric's points in the cache's compact
	// encoding, copied from memory without re-encoding:
	//
	//	varint  name length, then the name   (as in legacy records)
	//	varint  point count                  (as in legacy records)
	//	uvarint encoded length, then the encoded points
	//
	// The encoding is that of cache/compact.go: per point a header byte
	// (bit 7: the timestamp delta changed; bits 0-6: value code), the
	// change of delta as a zigzag uvarint when bit 7 is set, then the value
	// bits XOR the previous value's: code 0 repeats them, otherwise code-1
	// is tz<<3|(n-1) and n bytes follow, least significant first, shifted
	// left by tz bytes. The first point is encoded against zero timestamp,
	// delta and bits, and leaves delta zero.
	//
	// Releases before this format ignore these files: their names start
	// with CompactPrefix, not "cache.".
	FormatCompact
	// FormatBlocks holds compact records in blocks that name their records
	// once, sorted and front coded under zstd; see blocks.go. Releases
	// before it ignore these files: their names start with BlocksPrefix.
	FormatBlocks
)

// CompactPrefix starts the name of a compact cache dump, which otherwise is
// named like a legacy one: cache-compact.<pid>.<nanoseconds>.bin.
const CompactPrefix = "cache-compact."

// BlocksPrefix starts the name of a cache dump of FormatBlocks.
const BlocksPrefix = "cache-blocks."

func (f Format) String() string {
	switch f {
	case FormatCompact:
		return "compact"
	case FormatBlocks:
		return "blocks"
	}
	return "legacy"
}

// Compact reports whether f holds points in the cache's compact encoding.
func (f Format) Compact() bool { return f == FormatCompact || f == FormatBlocks }

// CacheFormat reports the format of the cache dump called name.
func CacheFormat(name string) Format {
	switch {
	case strings.HasPrefix(name, CompactPrefix):
		return FormatCompact
	case strings.HasPrefix(name, BlocksPrefix):
		return FormatBlocks
	}
	return FormatLegacy
}

// CacheName names a cache dump of format f with the given suffix.
func CacheName(f Format, suffix string) string {
	switch f {
	case FormatCompact:
		return CompactPrefix + suffix
	case FormatBlocks:
		return BlocksPrefix + suffix
	}
	return "cache." + suffix
}

// isGeneration reports whether name is a dump or input file of some
// generation.
func isGeneration(name string) bool {
	return strings.HasPrefix(name, "cache.") || strings.HasPrefix(name, CompactPrefix) || strings.HasPrefix(name, BlocksPrefix) ||
		strings.HasPrefix(name, "input.") || strings.HasPrefix(name, CompactInputPrefix)
}

// CompactState is the encoder state after a compact record's last point:
// its timestamp, timestamp delta (zero after a single point) and value bits.
// An encoder continuing from it appends points the same decoder reads.
type CompactState struct {
	Timestamp, Delta int64
	Bits             uint64
}

// CompactRecord is a validated compact encoding of N points.
type CompactRecord struct {
	Data  []byte
	N     int
	State CompactState
}

var errCompact = errors.New("invalid compact recovery record")

// AppendCompactRecord appends a compact record of the n points encoded in
// data.
func AppendCompactRecord(dst []byte, metric string, data []byte, n int) []byte {
	dst = binary.AppendVarint(dst, int64(len(metric)))
	dst = append(dst, metric...)
	dst = binary.AppendVarint(dst, int64(n))
	dst = binary.AppendUvarint(dst, uint64(len(data)))
	return append(dst, data...)
}

// AppendCompactPoints encodes data as a fresh compact sequence.
func AppendCompactPoints(dst []byte, data []points.Point) []byte {
	var ts, delta int64
	var prev uint64
	for i, p := range data {
		d := p.Timestamp - ts
		change := d - delta
		if i == 0 {
			d = 0
		}
		v := math.Float64bits(p.Value)
		x := v ^ prev
		ts, delta, prev = p.Timestamp, d, v
		var header byte
		var tz, n int
		if x != 0 {
			tz = bits.TrailingZeros64(x) >> 3
			n = 8 - tz - bits.LeadingZeros64(x)>>3
			header = byte(tz<<3|(n-1)) + 1
		}
		if change != 0 {
			header |= 0x80
		}
		dst = append(dst, header)
		if change != 0 {
			dst = binary.AppendUvarint(dst, uint64(change<<1)^uint64(change>>63))
		}
		for low := x >> (8 * tz); n > 0; n-- {
			dst = append(dst, byte(low))
			low >>= 8
		}
	}
	return dst
}

// decodeCompact decodes the n points encoded in data, appending them to dst
// unless dst is nil and scan is set. Every byte is bounds checked: data
// comes from disk.
func decodeCompact(dst []points.Point, data []byte, n int, scan bool) ([]points.Point, CompactState, error) {
	if n <= 0 || n > len(data) {
		return dst, CompactState{}, errCompact
	}
	if !scan {
		dst = slices.Grow(dst, n)
	}
	var ts, delta int64
	var v uint64
	pos := 0
	for i := range n {
		if pos >= len(data) {
			return dst, CompactState{}, errCompact
		}
		header := data[pos]
		pos++
		if header&0x80 != 0 {
			u, k := binary.Uvarint(data[pos:])
			if k <= 0 {
				return dst, CompactState{}, errCompact
			}
			pos += k
			delta += int64(u>>1) ^ -int64(u&1)
		}
		ts += delta
		if code := int(header&0x7f) - 1; code >= 0 {
			tz, size := code>>3, code&7+1
			if tz+size > 8 || pos+size > len(data) {
				return dst, CompactState{}, errCompact
			}
			var low uint64
			for j := size - 1; j >= 0; j-- {
				low = low<<8 | uint64(data[pos+j])
			}
			v ^= low << (8 * tz)
			pos += size
		}
		if !scan {
			dst = append(dst, points.Point{Value: math.Float64frombits(v), Timestamp: ts})
		}
		if i == 0 {
			delta = 0
		}
	}
	if pos != len(data) {
		return dst, CompactState{}, errCompact
	}
	return dst, CompactState{Timestamp: ts, Delta: delta, Bits: v}, nil
}

// compactBody returns a compact record's metric, point count and encoded
// points.
func compactBody(raw []byte) ([]byte, int, []byte, error) {
	name, err := recordMetric(raw)
	if err != nil {
		return nil, 0, nil, err
	}
	_, k := binary.Varint(raw)
	rest := raw[k+len(name):]
	count, m := binary.Varint(rest)
	if m <= 0 || count <= 0 || count > int64(len(rest)) {
		return nil, 0, nil, errCompact
	}
	rest = rest[m:]
	size, m := binary.Uvarint(rest)
	if m <= 0 || size != uint64(len(rest)-m) {
		return nil, 0, nil, errCompact
	}
	return name, int(count), rest[m:], nil
}

// ReadCompactFile visits the records of a compact cache dump in order, for an
// ordered restore. The points passed to visit own their storage.
func ReadCompactFile(path string, visit func(*points.Points)) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return ReadCompact(f, visit)
}

// ReadCompact is ReadCompactFile on a reader.
func ReadCompact(r io.Reader, visit func(*points.Points)) error {
	br := bufio.NewReaderSize(r, points.MB)
	var name, data []byte
	for {
		nameLength, err := binary.ReadVarint(br)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if nameLength <= 0 || nameLength > points.MB {
			return fmt.Errorf("invalid metric name length: %d", nameLength)
		}
		name = slices.Grow(name[:0], int(nameLength))[:nameLength]
		if _, err = io.ReadFull(br, name); err != nil {
			return unexpected(err)
		}
		count, err := binary.ReadVarint(br)
		if err != nil {
			return unexpected(err)
		}
		size, err := binary.ReadUvarint(br)
		if err != nil {
			return unexpected(err)
		}
		if count <= 0 || size < uint64(count) || size > uint64(math.MaxInt32) {
			return errCompact
		}
		data = slices.Grow(data[:0], int(size))[:size]
		if _, err = io.ReadFull(br, data); err != nil {
			return unexpected(err)
		}
		values, _, err := decodeCompact(make([]points.Point, 0, count), data, int(count), false)
		if err != nil {
			return err
		}
		visit(&points.Points{Metric: string(name), Data: values})
	}
}

func unexpected(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

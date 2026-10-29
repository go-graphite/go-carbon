package inputlog

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"

	"github.com/go-graphite/go-carbon/points"
)

// A segment starts with a fixed header, followed by frames up to its
// preallocated size, or a little past it when a cycle's frames do not fit:
//
//	segment header  magic "GCINLOG1", seq, salt, created, crc32c
//	frame header    magic, codec, partition, salt, payload and raw length,
//	                points, replay-from position, payload crc32c, crc32c
//	frame payload   zstd of the frame's rows, see codecStream; frames of
//	                codecRecords hold legacy binary records
//	                (points.AppendBinary) and are still replayed
//
// Segments are recycled, so the space after the last valid frame can hold
// frames of an earlier use. Every frame repeats its segment's random salt; a
// scan stops at the first frame that does not match, which also ends a frame
// torn by a crash before its sync.
const (
	segmentMagic      = "GCINLOG1"
	segmentHeaderSize = 64
	frameMagic        = 0x4c494347 // "GCIL"
	frameHeaderSize   = 52
	codecRecords      = 1
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// position addresses a frame: its segment sequence number and file offset.
type position struct {
	seq uint64
	off int64
}

func (p position) less(o position) bool {
	return p.seq < o.seq || (p.seq == o.seq && p.off < o.off)
}

type segmentHeader struct {
	seq     uint64
	salt    uint64
	created int64
}

func (h segmentHeader) append(dst []byte) []byte {
	start := len(dst)
	dst = append(dst, segmentMagic...)
	dst = binary.LittleEndian.AppendUint64(dst, h.seq)
	dst = binary.LittleEndian.AppendUint64(dst, h.salt)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(h.created))
	dst = binary.LittleEndian.AppendUint32(dst, crc32.Checksum(dst[start:], castagnoli))
	return append(dst, make([]byte, segmentHeaderSize-(len(dst)-start))...)
}

func parseSegmentHeader(b []byte) (segmentHeader, bool) {
	if len(b) < 36 || string(b[:8]) != segmentMagic ||
		binary.LittleEndian.Uint32(b[32:]) != crc32.Checksum(b[:32], castagnoli) {
		return segmentHeader{}, false
	}
	return segmentHeader{
		seq:     binary.LittleEndian.Uint64(b[8:]),
		salt:    binary.LittleEndian.Uint64(b[16:]),
		created: int64(binary.LittleEndian.Uint64(b[24:])),
	}, true
}

type frameHeader struct {
	codec     uint8
	partition uint16
	salt      uint64
	length    uint32 // payload bytes
	rawLength uint32 // decompressed payload bytes
	points    uint32
	// Every point recorded before this position was persisted when the frame
	// was written. Replay starts at the last valid frame's position.
	replayFrom position
	crc        uint32 // payload crc32c
}

func (h frameHeader) append(dst []byte) []byte {
	start := len(dst)
	dst = binary.LittleEndian.AppendUint32(dst, frameMagic)
	dst = append(dst, h.codec, 0)
	dst = binary.LittleEndian.AppendUint16(dst, h.partition)
	dst = binary.LittleEndian.AppendUint64(dst, h.salt)
	dst = binary.LittleEndian.AppendUint32(dst, h.length)
	dst = binary.LittleEndian.AppendUint32(dst, h.rawLength)
	dst = binary.LittleEndian.AppendUint32(dst, h.points)
	dst = binary.LittleEndian.AppendUint64(dst, h.replayFrom.seq)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(h.replayFrom.off))
	dst = binary.LittleEndian.AppendUint32(dst, h.crc)
	return binary.LittleEndian.AppendUint32(dst, crc32.Checksum(dst[start:], castagnoli))
}

func parseFrameHeader(b []byte) (frameHeader, bool) {
	if len(b) < frameHeaderSize || binary.LittleEndian.Uint32(b) != frameMagic ||
		binary.LittleEndian.Uint32(b[48:]) != crc32.Checksum(b[:48], castagnoli) {
		return frameHeader{}, false
	}
	return frameHeader{
		codec:      b[4],
		partition:  binary.LittleEndian.Uint16(b[6:]),
		salt:       binary.LittleEndian.Uint64(b[8:]),
		length:     binary.LittleEndian.Uint32(b[16:]),
		rawLength:  binary.LittleEndian.Uint32(b[20:]),
		points:     binary.LittleEndian.Uint32(b[24:]),
		replayFrom: position{binary.LittleEndian.Uint64(b[28:]), int64(binary.LittleEndian.Uint64(b[36:]))},
		crc:        binary.LittleEndian.Uint32(b[44:]),
	}, true
}

var errRecord = errors.New("invalid input log record")

// recordLength returns the size of the legacy binary record at the start of
// data without decoding it; points.AppendBinaryPoints then validates it.
func recordLength(data []byte) (int, error) {
	n := 0
	next := func() (int64, bool) {
		v, l := binary.Varint(data[n:])
		n += l
		return v, l > 0
	}
	nameLength, ok := next()
	if !ok || nameLength < 0 || nameLength > int64(len(data)-n) {
		return 0, errRecord
	}
	n += int(nameLength)
	count, ok := next()
	if !ok || count < 0 || count > math.MaxInt32 {
		return 0, errRecord
	}
	for range 2 * count {
		if _, ok = next(); !ok {
			return 0, errRecord
		}
	}
	return n, nil
}

// decodeRecords calls visit for every record of a codecRecords payload. The
// points passed to visit own their storage.
func decodeRecords(data []byte, visit func(*points.Points)) (int, error) {
	total := 0
	for len(data) > 0 {
		n, err := recordLength(data)
		if err != nil {
			return total, err
		}
		name, values, err := points.AppendBinaryPoints(nil, data[:n])
		if err != nil {
			return total, err
		}
		data = data[n:]
		if len(values) == 0 {
			continue
		}
		total += len(values)
		visit(&points.Points{Metric: string(name), Data: values})
	}
	return total, nil
}

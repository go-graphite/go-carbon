package chunkstore

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"math"
)

// TransferStream reads the existing v2 framing one metric at a time. Keys and
// values are valid until Next; SST writers consume them before advancing. The
// complete stream checksum must pass before any catalog is published.
type TransferStream struct {
	Window                                   *NativeWindow
	Count                                    int
	index                                    int
	input                                    *bufio.Reader
	reader                                   io.Reader
	hash                                     hash.Hash
	arena                                    transferArena
	err                                      error
	bytes                                    int64
	word                                     [8]byte // parsing is sequential; avoid one escaping slice per integer
	metricLimit, metricStart, metricOverhead int64
}

func NewTransferStream(input io.Reader) (*TransferStream, error) {
	s := &TransferStream{input: bufio.NewReaderSize(input, 256<<10), hash: sha256.New()}
	s.reader = io.TeeReader(s.input, s.hash)
	var magic [8]byte
	s.read(magic[:])
	if s.err != nil {
		return nil, s.err
	}
	if string(magic[:]) == "BKTFR003" {
		s.Window = &NativeWindow{From: int64(s.u64()), Until: int64(s.u64())}
		if err := s.Window.Validate(); err != nil {
			return nil, err
		}
	} else if string(magic[:]) != "BKTFR002" {
		return nil, ErrFormat
	}
	s.Count = int(s.u32())
	if s.err != nil {
		return nil, s.err
	}
	return s, nil
}
func (s *TransferStream) read(out []byte) {
	if s.err == nil {
		n, err := io.ReadFull(s.reader, out)
		s.bytes += int64(n)
		s.err = err
	}
}
func (s *TransferStream) u32() uint32 {
	s.read(s.word[:4])
	if s.err != nil {
		return 0
	}
	return binary.BigEndian.Uint32(s.word[:4])
}
func (s *TransferStream) u64() uint64 {
	s.read(s.word[:])
	if s.err != nil {
		return 0
	}
	return binary.BigEndian.Uint64(s.word[:])
}
func (s *TransferStream) blob(limit uint32) []byte {
	n := s.u32()
	if s.err != nil {
		return nil
	}
	if n > limit {
		s.err = ErrFormat
		return nil
	}
	if s.metricLimit > 0 && s.bytes-s.metricStart+int64(n)+s.metricOverhead > s.metricLimit {
		s.err = ErrTransferLimit
		return nil
	}
	b := s.arena.alloc(int(n))
	s.read(b)
	return b
}
func (s *TransferStream) Next() (TransferMetric, error) {
	if s.index >= s.Count {
		return TransferMetric{}, io.EOF
	}
	s.arena.free = s.arena.slab
	s.metricStart, s.metricOverhead = s.bytes, 0
	r := TransferMetric{}
	r.Config.Name = string(s.blob(4096))
	r.Version.Name = r.Config.Name
	r.Version.ID = s.u64()
	r.Version.Generation = s.u64()
	r.Version.Revision = s.u64()
	flags := s.u32()
	if flags > 3 {
		return r, ErrFormat
	}
	r.Continuation = flags&1 != 0
	r.Config.AggregationMethod = AggregationMethod(s.u32())
	r.Config.XFilesFactor = math.Float32frombits(s.u32())
	n := s.u32()
	if n > 4096 {
		return r, ErrFormat
	}
	r.Config.Retentions = make([]Retention, int(n))
	for i := range r.Config.Retentions {
		r.Config.Retentions[i] = Retention{Step: int(s.u32()), Count: int(s.u32())}
	}
	r.PointDigest = string(s.blob(256))
	n = s.u32()
	if n > 4096 {
		return r, ErrFormat
	}
	if n > 0 {
		r.Counts = make([]int, int(n))
	}
	for i := range r.Counts {
		r.Counts[i] = int(s.u32())
	}
	n = s.u32()
	// Reserve a small bounded metadata vector, then grow only as bytes arrive.
	// A forged count cannot reserve a purported multi-gigabyte metric. Real
	// histories commonly have hundreds of keys; avoid nine growing vectors and
	// their copies per metric without trusting an unbounded wire count.
	r.Keys = make([]RelayoutKV, 0, min(n, 256))
	for range n {
		s.metricOverhead += 48 // two slice headers, even for empty values
		key, value := s.blob(4096), s.blob(16<<20)
		if s.err != nil {
			return r, s.err
		}
		r.Keys = append(r.Keys, RelayoutKV{key, value})
	}
	if flags&2 != 0 {
		r.NativeSkip = &NativeSkip{Name: r.Config.Name, Bytes: int64(s.u64()), PhysicalPoints: int64(s.u64()), ArchiveSlots: int64(s.u64()), MemoryBytes: int64(s.u64()), Reason: string(s.blob(128))}
	}
	if s.Window != nil {
		n := s.u32()
		if n > uint32(2*len(r.Config.Retentions)) {
			return r, ErrFormat
		}
		for range n {
			skip := NativeWindowSkip{Archive: int(s.u32()), Timestamp: int64(s.u64()), Reason: string(s.blob(128))}
			if skip.Archive >= len(r.Config.Retentions) || skip.Timestamp < 0 || skip.Reason == "" {
				return r, ErrFormat
			}
			r.WindowSkipped = append(r.WindowSkipped, skip)
		}
	}
	if s.err != nil {
		return r, s.err
	}
	s.index++
	return r, nil
}
func (s *TransferStream) Finish() (string, int64, error) {
	if s.err != nil {
		return "", s.bytes, s.err
	}
	if s.index != s.Count {
		return "", s.bytes, errors.New("native stream not fully consumed")
	}
	var want [sha256.Size]byte
	if _, err := io.ReadFull(s.input, want[:]); err != nil {
		return "", s.bytes, err
	}
	got := s.hash.Sum(nil)
	if !bytes.Equal(want[:], got) {
		return "", s.bytes, errors.New("native stream checksum mismatch")
	}
	if _, err := s.input.ReadByte(); err != io.EOF {
		return "", s.bytes, errors.New("trailing native stream bytes")
	}
	return hex.EncodeToString(got), s.bytes + sha256.Size, nil
}

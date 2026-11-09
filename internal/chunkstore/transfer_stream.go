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
	Count  int
	index  int
	input  *bufio.Reader
	reader io.Reader
	hash   hash.Hash
	arena  transferArena
	err    error
	bytes  int64
}

func NewTransferStream(input io.Reader) (*TransferStream, error) {
	s := &TransferStream{input: bufio.NewReaderSize(input, 256<<10), hash: sha256.New()}
	s.reader = io.TeeReader(s.input, s.hash)
	var magic [8]byte
	s.read(magic[:])
	if s.err != nil {
		return nil, s.err
	}
	if string(magic[:]) != "BKTFR002" {
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
	var b [4]byte
	s.read(b[:])
	return binary.BigEndian.Uint32(b[:])
}
func (s *TransferStream) u64() uint64 {
	var b [8]byte
	s.read(b[:])
	return binary.BigEndian.Uint64(b[:])
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
	b := s.arena.alloc(int(n))
	s.read(b)
	return b
}
func (s *TransferStream) Next() (TransferMetric, error) {
	if s.index >= s.Count {
		return TransferMetric{}, io.EOF
	}
	s.arena.free = s.arena.slab
	r := TransferMetric{}
	r.Config.Name = string(s.blob(4096))
	r.Version.Name = r.Config.Name
	r.Version.ID = s.u64()
	r.Version.Generation = s.u64()
	r.Version.Revision = s.u64()
	flags := s.u32()
	if flags > 1 {
		return r, ErrFormat
	}
	r.Continuation = flags == 1
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
	// Grow only as bytes arrive: a corrupt count cannot allocate an entire
	// purported multi-gigabyte metric before the first truncated key is read.
	for range n {
		key, value := s.blob(4096), s.blob(16<<20)
		if s.err != nil {
			return r, s.err
		}
		r.Keys = append(r.Keys, RelayoutKV{key, value})
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

package codec

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"

	"github.com/klauspost/compress/zstd"
)

const (
	Magic                       = "\x00GCS\r\n\x1a\n"
	Version                byte = 1
	FlagZstd               byte = 1
	PreambleSize                = len(Magic) + 2
	HeaderSize                  = 20
	DefaultMaxFrameSize         = 8 << 20
	DefaultMaxNames             = 5_000_000
	DefaultDictionaryBytes      = 64 << 20
	DefaultMaxRows              = 64 << 10
	DefaultMaxNameSize          = 64 << 10
	DataFrame              byte = 0
	ResetFrame             byte = 1
	StateFrame             byte = 2
)

var (
	ErrPreamble = errors.New("invalid stream preamble, version, or flags")
	ErrFrame    = errors.New("invalid stream frame")
	ErrLimit    = errors.New("stream limit exceeded")
	ErrBroken   = errors.New("stream decoder is broken")
	castagnoli  = crc32.MakeTable(crc32.Castagnoli)
)

// Options bounds memory use. Zero fields select defaults. MaxFrameSize bounds
// both compressed and uncompressed payloads. MaxRows also controls encoder flush
// cadence; FrameSize is its target payload size (default 256 KiB).
// DictionaryBytes bounds connection dictionary capacity (default 64 MiB).
// Shared encoders receive a separately budgeted NameTable.
type Options struct {
	Flags           byte
	MaxFrameSize    int
	FrameSize       int
	MaxRows         int
	MaxNames        int
	MaxNameSize     int
	DictionaryBytes int
}

func options(o Options) (Options, error) {
	if o.Flags & ^FlagZstd != 0 {
		return o, ErrPreamble
	}
	if o.MaxFrameSize == 0 {
		o.MaxFrameSize = DefaultMaxFrameSize
	}
	if o.FrameSize == 0 {
		o.FrameSize = 256 << 10
	}
	if o.MaxRows == 0 {
		o.MaxRows = DefaultMaxRows
	}
	if o.MaxNames == 0 {
		o.MaxNames = DefaultMaxNames
	}
	if o.MaxNameSize == 0 {
		o.MaxNameSize = DefaultMaxNameSize
	}
	if o.DictionaryBytes == 0 {
		o.DictionaryBytes = DefaultDictionaryBytes
	}
	if o.DictionaryBytes < 4096 {
		return o, ErrLimit
	}
	if o.MaxFrameSize < 64 || uint64(o.MaxFrameSize) > uint64(^uint32(0)) || o.FrameSize < 1 || o.MaxRows < 1 || uint64(o.MaxRows) > uint64(^uint32(0)) || o.MaxNames < 1 || uint64(o.MaxNames) > uint64(^uint32(0)) || o.MaxNameSize < 1 {
		return o, ErrLimit
	}
	o.FrameSize = min(o.FrameSize, o.MaxFrameSize)
	o.MaxNameSize = min(o.MaxNameSize, o.MaxFrameSize-32)
	return o, nil
}

// AppendPreamble appends the connection marker. Only FlagZstd is supported.
func AppendPreamble(dst []byte, flags byte) ([]byte, error) {
	if flags & ^FlagZstd != 0 {
		return dst, ErrPreamble
	}
	dst = append(dst, Magic...)
	return append(dst, Version, flags), nil
}

// ParsePreamble validates exactly one complete connection preamble.
func ParsePreamble(p []byte) (byte, error) {
	if len(p) != PreambleSize || string(p[:len(Magic)]) != Magic || p[len(Magic)] != Version || p[len(Magic)+1] & ^FlagZstd != 0 {
		return 0, ErrPreamble
	}
	return p[len(Magic)+1], nil
}

// FrameInfo describes an accepted frame, including its wire payload bytes.
type FrameInfo struct {
	Kind         byte
	PayloadBytes uint32
	RawBytes     uint32
	Rows         uint32
	Names        uint32
}

func appendFrame(dst, raw []byte, kind byte, rows int, z *zstd.Encoder, maxSize int) ([]byte, error) {
	if len(raw) > maxSize {
		return dst, ErrLimit
	}
	start := len(dst)
	dst = append(dst, make([]byte, HeaderSize)...)
	if z != nil && len(raw) > 0 {
		dst = z.EncodeAll(raw, dst)
	} else {
		dst = append(dst, raw...)
	}
	payload := dst[start+HeaderSize:]
	if len(payload) > maxSize {
		return dst[:start], ErrLimit
	}
	h := dst[start : start+HeaderSize]
	h[0] = kind
	binary.BigEndian.PutUint32(h[4:8], uint32(len(payload)))
	binary.BigEndian.PutUint32(h[8:12], uint32(len(raw)))
	binary.BigEndian.PutUint32(h[12:16], uint32(rows))
	binary.BigEndian.PutUint32(h[16:20], crc32.Checksum(payload, castagnoli))
	return dst, nil
}

type reader struct {
	data []byte
	err  error
}

func (r *reader) fail() { r.err = ErrFrame; r.data = nil }
func (r *reader) uint() uint64 {
	v, n := binary.Uvarint(r.data)
	if n <= 0 {
		r.fail()
		return 0
	}
	r.data = r.data[n:]
	return v
}
func (r *reader) sint() int64 {
	v, n := binary.Varint(r.data)
	if n <= 0 {
		r.fail()
		return 0
	}
	r.data = r.data[n:]
	return v
}
func (r *reader) bytes(n uint64) []byte {
	if n > uint64(len(r.data)) {
		r.fail()
		return nil
	}
	b := r.data[:n]
	r.data = r.data[n:]
	return b
}
func (r *reader) xor() uint64 {
	h := r.bytes(1)
	if len(h) == 0 || h[0] == 0 {
		return 0
	}
	lead, n := int(h[0]-1)/8, int(h[0]-1)%8+1
	if lead+n > 8 {
		r.fail()
		return 0
	}
	var x uint64
	for _, b := range r.bytes(uint64(n)) {
		x = x<<8 | uint64(b)
	}
	return x << (8 * (8 - lead - n))
}

// readMore preserves bytes consumed before a transport error for restart handoff.
func readMore(r io.Reader, dst []byte, n *int) error {
	got, err := io.ReadFull(r, dst[*n:])
	*n += got
	return err
}

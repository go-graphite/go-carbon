package codec

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

// WireReader bounds and CRC-checks frames without decompressing or decoding
// points. Successful validation says nothing about payload semantics. A full
// Decoder must validate the durable frame before any points are applied.
// It is not concurrency safe. Returned bytes belong to the reader until Read.
type WireReader struct {
	opts Options
	wire []byte
	n    int
}

func NewWireReader(o Options) (*WireReader, error) {
	o, err := options(o)
	if err != nil {
		return nil, err
	}
	return &WireReader{opts: o}, nil
}

// Validate accepts exactly one wire frame; it performs no dictionary access.
func (r *WireReader) Validate(frame []byte) (FrameInfo, error) {
	info, err := frameHeader(frame, r.opts)
	if err != nil {
		return info, err
	}
	if uint64(len(frame)) != uint64(HeaderSize)+uint64(info.PayloadBytes) {
		return info, ErrFrame
	}
	if crc32.Checksum(frame[HeaderSize:], castagnoli) != binary.BigEndian.Uint32(frame[16:20]) {
		return info, ErrFrame
	}
	return info, nil
}

// Read preserves partial input on transport errors. Discard after protocol
// errors. A clean EOF occurs only between frames.
func (r *WireReader) Read(input io.Reader) ([]byte, FrameInfo, error) {
	if len(r.wire) < HeaderSize {
		r.wire = make([]byte, HeaderSize)
	}
	if r.n < HeaderSize {
		if err := readMore(input, r.wire[:HeaderSize], &r.n); err != nil {
			return nil, FrameInfo{}, err
		}
	}
	info, err := frameHeader(r.wire[:HeaderSize], r.opts)
	if err != nil {
		return nil, info, err
	}
	size := HeaderSize + int(info.PayloadBytes)
	if cap(r.wire) < size {
		b := make([]byte, size)
		copy(b, r.wire)
		r.wire = b
	} else {
		r.wire = r.wire[:size]
	}
	if r.n < size {
		if err = readMore(input, r.wire, &r.n); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, info, err
		}
	}
	info, err = r.Validate(r.wire)
	if err != nil {
		return nil, info, err
	}
	r.n = 0
	return r.wire, info, nil
}

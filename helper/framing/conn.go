// Package framing provides a prefix length framed net.Conn connection.
//
// It implements a wrapper around net.Conn that frames each packet with a size
// prefix of length 1, 2 or 4 bytes of either little or big endianess.
//
// Original author: Adam Lindberg
package framing

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
)

// Errors returned by framed connections.
var (
	// ErrPrefixLength is returned when an invalid prefix length is given.
	ErrPrefixLength = errors.New("Invalid frame prefix length")
	// ErrFrameTooLarge is returned from Write(b []byte) when b is larger than
	// MaxFrameSize.
	ErrFrameTooLarge = errors.New("Frame too large for buffer size")
)

// Conn wraps a net.Conn making it aware about frames and their size.
type Conn struct {
	// The inner net.Conn
	net.Conn
	// The frame size: 1, 2 or 4.
	PrefixLength byte
	// The endianess of the length prefix.
	Endianess binary.ByteOrder
	// The maximum size of a frame for the current prefix size.
	MaxFrameSize uint
	// The remaining bytes of the current frame
	bytesLeft int
}

// NewConn returns a framing.Conn that acts like a net.Conn. prefixLength
// should be one of 1, 2 or 4. endianess should be one of binary.BigEndian or
// binary.LittleEndian.
//
// The difference to a normal net.Conn is that Write(b []byte) prefixes each
// packet with a size and Read(b []byte) reads that size and waits for that
// many bytes.
//
// A complementary ReadFrame()
// function can be used to read and wait for a full frame.
func NewConn(inner net.Conn, prefixLength byte, endianess binary.ByteOrder) (conn *Conn, err error) {
	if !(prefixLength == 1 || prefixLength == 2 || prefixLength == 4) {
		return nil, ErrPrefixLength
	}

	var max uint
	switch prefixLength {
	case 1:
		max = math.MaxUint8
	case 2:
		max = math.MaxUint16
	case 4:
		max = math.MaxUint32
	}

	return &Conn{
		Conn:         inner,
		PrefixLength: prefixLength,
		Endianess:    endianess,
		MaxFrameSize: max,
	}, nil
}

// Read implements net.Conn.Read(b []byte)
func (f *Conn) Read(b []byte) (n int, err error) {
	total := 0

	if f.bytesLeft <= 0 {
		frameSize, err := f.readSize()
		if err != nil {
			return total, err
		}
		f.bytesLeft = frameSize
	}
	n, err = io.ReadFull(f.Conn, b[:min(f.bytesLeft, len(b))])
	f.bytesLeft -= n
	return
}

// ReadFrame returns the next full frame in the stream.
func (f *Conn) ReadFrame() (frame []byte, err error) {
	size, err := f.readSize()
	if err != nil {
		return nil, err
	}

	buf := make([]byte, size)
	_, err = io.ReadFull(f.Conn, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (f *Conn) readSize() (size int, err error) {
	switch f.PrefixLength {
	case 1:
		var s uint8
		err = binary.Read(f.Conn, f.Endianess, &s)
		size = int(s)
	case 2:
		var s uint16
		err = binary.Read(f.Conn, f.Endianess, &s)
		size = int(s)
	case 4:
		var s uint32
		err = binary.Read(f.Conn, f.Endianess, &s)
		size = int(s)
	}
	if err != nil {
		return 0, err
	}
	if uint(size) > f.MaxFrameSize {
		return 0, ErrPrefixLength
	}
	return size, nil
}

// Write implements net.Conn.Write(b []byte)
func (f *Conn) Write(b []byte) (n int, err error) {
	length := len(b)

	if uint(length) > f.MaxFrameSize {
		return 0, ErrFrameTooLarge
	}
	switch f.PrefixLength {
	case 1:
		err = binary.Write(f.Conn, f.Endianess, uint8(length))
	case 2:
		err = binary.Write(f.Conn, f.Endianess, uint16(length))
	case 4:
		err = binary.Write(f.Conn, f.Endianess, uint32(length))
	}
	if err != nil {
		return 0, err
	}

	return f.Conn.Write(b)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

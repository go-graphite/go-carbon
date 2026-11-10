package points

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

// ReadBinaryBorrowed has ReadBinary's record and partial-record semantics, but
// lends its name and point buffers only until visit returns. A batching caller
// can copy into a reusable arena instead of allocating three objects per row.
func ReadBinaryBorrowed(r io.Reader, visit func([]byte, []Point)) error {
	return ReadBinaryBorrowedSize(r, MB, visit)
}

// ReadBinaryBorrowedSize adapts read-ahead memory to the number of independent
// startup replay streams. Record limits and partial-record behavior are unchanged.
func ReadBinaryBorrowedSize(r io.Reader, bufferSize int, visit func([]byte, []Point)) error {
	return readBinaryBorrowed(r, bufferSize, 0, visit)
}

// ReadBinaryBorrowedBatches preserves point order and complete partial-record
// recovery, but visits at most batchPoints points at a time. It does not retain
// an entire legacy record before the caller can apply admission or pacing.
// The name and points are borrowed until visit returns; one record may produce
// several visits. ReadBinaryBorrowedSize retains its whole-record semantics.
func ReadBinaryBorrowedBatches(r io.Reader, bufferSize, batchPoints int, visit func([]byte, []Point)) error {
	if batchPoints <= 0 {
		return fmt.Errorf("binary batch size must be positive")
	}
	return readBinaryBorrowed(r, bufferSize, batchPoints, visit)
}

func readBinaryBorrowed(r io.Reader, bufferSize, batchPoints int, visit func([]byte, []Point)) error {
	reader := bufio.NewReaderSize(r, max(4096, bufferSize))
	names := make([]byte, MB)
	var scratch [64]Point
	data := scratch[:0]
	if batchPoints > 0 {
		if batchPoints <= len(scratch) {
			data = scratch[:0:batchPoints]
		} else {
			data = make([]Point, 0, batchPoints)
		}
	}
	for {
		if reader.Buffered() == 0 {
			if _, err := reader.Peek(1); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
		}
		// Capture writes single-point records. Decode complete buffered ones
		// directly: ReadVarint's ByteReader interface dispatch per byte was
		// the serial bottleneck even with allocation-free worker batches.
		raw, _ := reader.Peek(reader.Buffered())
		if name, p, size, ok := singleBinaryPoint(raw); ok {
			scratch[0] = p
			visit(name, scratch[:1])
			_, _ = reader.Discard(size)
			continue
		}
		length, err := binary.ReadVarint(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if length < 0 || length > MB {
			return fmt.Errorf("invalid metric name length: %d", length)
		}
		if _, err := io.ReadFull(reader, names[:length]); err != nil {
			return err
		}
		count, err := binary.ReadVarint(reader)
		if err != nil {
			return err
		}
		if count < 0 {
			return fmt.Errorf("invalid point count: %d", count)
		}
		data = data[:0]
		var value, stamp int64
		for range count {
			v, err := binary.ReadVarint(reader)
			if err != nil {
				if len(data) > 0 {
					visit(names[:length], data)
				}
				return err
			}
			ts, err := binary.ReadVarint(reader)
			if err != nil {
				if len(data) > 0 {
					visit(names[:length], data)
				}
				return err
			}
			value += v
			stamp += ts
			data = append(data, Point{Value: math.Float64frombits(uint64(value)), Timestamp: stamp})
			if batchPoints > 0 && len(data) == batchPoints {
				visit(names[:length], data)
				data = data[:0]
			}
		}
		if len(data) > 0 {
			visit(names[:length], data)
		}
	}
}

func singleBinaryPoint(raw []byte) ([]byte, Point, int, bool) {
	length, n := binary.Varint(raw)
	if n <= 0 || length < 0 || length > MB || length > int64(len(raw)-n) {
		return nil, Point{}, 0, false
	}
	name := raw[n : n+int(length)]
	off := n + int(length)
	count, n := binary.Varint(raw[off:])
	if n <= 0 || count != 1 {
		return nil, Point{}, 0, false
	}
	off += n
	value, n := binary.Varint(raw[off:])
	if n <= 0 {
		return nil, Point{}, 0, false
	}
	off += n
	stamp, n := binary.Varint(raw[off:])
	if n <= 0 {
		return nil, Point{}, 0, false
	}
	off += n
	return name, Point{Value: math.Float64frombits(uint64(value)), Timestamp: stamp}, off, true
}

// ReadBinaryBytesBorrowed decodes a complete in-memory frame without allocating
// bufio/name arenas for each frame. The reference decoder handles uncommon
// multi-point records and preserves its exact partial-record semantics.
func ReadBinaryBytesBorrowed(raw []byte, visit func([]byte, []Point)) error {
	return readBinaryBytesBorrowed(raw, 0, visit)
}

// ReadBinaryBytesBorrowedBatches also bounds scratch space for multi-point
// records inside a complete frame. Single-point records retain the fast path.
func ReadBinaryBytesBorrowedBatches(raw []byte, batchPoints int, visit func([]byte, []Point)) error {
	if batchPoints <= 0 {
		return fmt.Errorf("binary batch size must be positive")
	}
	return readBinaryBytesBorrowed(raw, batchPoints, visit)
}

func readBinaryBytesBorrowed(raw []byte, batchPoints int, visit func([]byte, []Point)) error {
	var scratch [1]Point
	for len(raw) > 0 {
		name, p, n, ok := singleBinaryPoint(raw)
		if !ok {
			return readBinaryBorrowed(bytes.NewReader(raw), 64<<10, batchPoints, visit)
		}
		scratch[0] = p
		visit(name, scratch[:])
		raw = raw[n:]
	}
	return nil
}

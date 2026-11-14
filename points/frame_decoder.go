package points

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// FrameDecoder lends names and point slices only until the next callback. It
// reuses its point arena across complete in-memory frames, including multi-point
// records, so callers retaining data must copy it before returning.
type FrameDecoder struct {
	single [1]Point
	data   []Point
}

func (d *FrameDecoder) Read(raw []byte, visit func([]byte, []Point)) error {
	read := func() (int64, error) {
		v, n := binary.Varint(raw)
		if n == 0 {
			if len(raw) == 0 {
				return 0, io.EOF
			}
			return 0, io.ErrUnexpectedEOF
		}
		if n < 0 {
			return 0, fmt.Errorf("binary varint overflow")
		}
		raw = raw[n:]
		return v, nil
	}
	for len(raw) > 0 {
		if name, p, n, ok := singleBinaryPoint(raw); ok {
			d.single[0] = p
			visit(name, d.single[:])
			raw = raw[n:]
			continue
		}
		length, err := read()
		if err != nil {
			return err
		}
		if length < 0 || length > MB {
			return fmt.Errorf("invalid metric name length: %d", length)
		}
		if length > int64(len(raw)) {
			return io.ErrUnexpectedEOF
		}
		name := raw[:int(length)]
		raw = raw[int(length):]
		count, err := read()
		if err != nil {
			return err
		}
		if count < 0 {
			return fmt.Errorf("invalid point count: %d", count)
		}
		d.data = d.data[:0]
		var value, stamp int64
		for range count {
			v, err := read()
			if err != nil {
				if len(d.data) > 0 {
					visit(name, d.data)
				}
				return err
			}
			ts, err := read()
			if err != nil {
				if len(d.data) > 0 {
					visit(name, d.data)
				}
				return err
			}
			value += v
			stamp += ts
			d.data = append(d.data, Point{Value: math.Float64frombits(uint64(value)), Timestamp: stamp})
		}
		if len(d.data) > 0 {
			visit(name, d.data)
		}
	}
	return nil
}

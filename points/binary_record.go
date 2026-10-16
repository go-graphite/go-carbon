package points

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"slices"
)

// UnmarshalBinary decodes exactly one AppendBinary record. Unlike the streaming
// recovery reader, an indexed record must be complete: its verified extent cannot
// include a partial record or trailing bytes. The returned metric and points own
// their storage, so a caller may release the mapped input after this method.
func (p *Points) UnmarshalBinary(data []byte) error {
	name, values, err := AppendBinaryPoints([]Point{}, data)
	if err != nil {
		return err
	}
	p.Metric, p.Data = string(name), values
	return nil
}

// AppendBinaryPoints decodes exactly one AppendBinary record into dst, reusing
// its capacity. The returned name borrows data and must be copied if retained.
// On error, dst's existing points are unchanged, but its unused capacity may
// have been overwritten; the returned slice is the original dst.
func AppendBinaryPoints(dst []Point, data []byte) ([]byte, []Point, error) {
	nameLength, n := binary.Varint(data)
	if n <= 0 || nameLength < 0 || nameLength > MB {
		return nil, dst, fmt.Errorf("invalid binary metric name length")
	}
	data = data[n:]
	if nameLength > int64(len(data)) {
		return nil, dst, io.ErrUnexpectedEOF
	}
	name := data[:nameLength]
	data = data[nameLength:]
	count, n := binary.Varint(data)
	if n <= 0 || count < 0 {
		return nil, dst, fmt.Errorf("invalid binary point count")
	}
	data = data[n:]
	// Every point needs at least two varints; this also bounds forged allocations.
	if count > int64(len(data)/2) {
		return nil, dst, io.ErrUnexpectedEOF
	}
	if count > int64(math.MaxInt/16-len(dst)) {
		return nil, dst, fmt.Errorf("binary point count is too large")
	}
	values := slices.Grow(dst, int(count))
	values = values[:len(dst)+int(count)]
	var value, timestamp int64
	for i := len(dst); i < len(values); i++ {
		delta, n := binary.Varint(data)
		if n <= 0 {
			return nil, dst, fmt.Errorf("invalid binary point value")
		}
		data = data[n:]
		value += delta
		delta, n = binary.Varint(data)
		if n <= 0 {
			return nil, dst, fmt.Errorf("invalid binary point timestamp")
		}
		data = data[n:]
		timestamp += delta
		values[i] = Point{Value: math.Float64frombits(uint64(value)), Timestamp: timestamp}
	}
	if len(data) != 0 {
		return nil, dst, fmt.Errorf("trailing bytes after binary record")
	}
	return name, values, nil
}

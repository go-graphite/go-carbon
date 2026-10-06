package points

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// UnmarshalBinary decodes exactly one AppendBinary record. Unlike the streaming
// recovery reader, an indexed record must be complete: its verified extent cannot
// include a partial record or trailing bytes. The returned metric and points own
// their storage, so a caller may release the mapped input after this method.
func (p *Points) UnmarshalBinary(data []byte) error {
	nameLength, n := binary.Varint(data)
	if n <= 0 || nameLength < 0 || nameLength > MB {
		return fmt.Errorf("invalid binary metric name length")
	}
	data = data[n:]
	if nameLength > int64(len(data)) {
		return io.ErrUnexpectedEOF
	}
	name := data[:nameLength]
	data = data[nameLength:]
	count, n := binary.Varint(data)
	if n <= 0 || count < 0 {
		return fmt.Errorf("invalid binary point count")
	}
	data = data[n:]
	// Every point needs at least two varints; this also bounds forged allocations.
	if count > int64(len(data)/2) {
		return io.ErrUnexpectedEOF
	}
	values := make([]Point, int(count))
	var value, timestamp int64
	for i := range values {
		delta, n := binary.Varint(data)
		if n <= 0 {
			return fmt.Errorf("invalid binary point value")
		}
		data = data[n:]
		value += delta
		delta, n = binary.Varint(data)
		if n <= 0 {
			return fmt.Errorf("invalid binary point timestamp")
		}
		data = data[n:]
		timestamp += delta
		values[i] = Point{Value: math.Float64frombits(uint64(value)), Timestamp: timestamp}
	}
	if len(data) != 0 {
		return fmt.Errorf("trailing bytes after binary record")
	}
	p.Metric, p.Data = string(name), values
	return nil
}

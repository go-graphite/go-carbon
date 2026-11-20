// Package partsrun encodes immutable metric runs without changing timestamp,
// float bits, order or sequence. Keys, policy and archive identity live outside
// the value. Version 1 is shared by the compact head and persisted archive runs.
package partsrun

import (
	"encoding/binary"
	"errors"
	"math/bits"

	"github.com/golang/snappy"
)

type Sample struct {
	Time           int64
	Bits, Sequence uint64
}
type Header struct {
	Clock      int64
	Count      int
	Compressed bool
}

// Workspace reuses bounded codec scratch on one serial owner. Encoded output
// belongs to dst and never aliases this scratch. Use one workspace per owner.
type Workspace struct {
	payload, compressed, decoded []byte
}

// Append reads the accessor repeatedly and appends one complete version-1 run. The accessor must
// remain stable until Append returns. No expanded sample slice is allocated.
func Append(dst []byte, clock int64, count int, at func(int) Sample) ([]byte, error) {
	return AppendWithWorkspace(dst, clock, count, at, nil)
}

// AppendWithWorkspace is byte-identical to Append, with reusable temporary bytes.
func AppendWithWorkspace(dst []byte, clock int64, count int, at func(int) Sample, w *Workspace) ([]byte, error) {
	if count < 0 || count > 1<<26 || at == nil {
		return dst, errors.New("invalid run input")
	}
	if w == nil {
		w = &Workspace{}
	}
	payload := w.payload[:0]
	regularTime, regularSequence := true, true
	var timeStep int64
	var sequenceStep uint64
	if count > 1 {
		timeStep = at(1).Time - at(0).Time
		sequenceStep = at(1).Sequence - at(0).Sequence
	}
	var previous Sample
	for i := range count {
		p := at(i)
		if i > 0 {
			regularTime = regularTime && p.Time-previous.Time == timeStep
			regularSequence = regularSequence && p.Sequence-previous.Sequence == sequenceStep
		}
		previous = p
	}

	timestamp, sequence, value := clock, uint64(0), uint64(0)
	flags := byte(0)
	if regularTime {
		flags |= 1
	}
	if regularSequence {
		flags |= 2
	}
	payload = append(payload, flags)
	if count > 0 {
		if regularTime {
			payload = binary.AppendVarint(payload, at(0).Time-clock)
			payload = binary.AppendVarint(payload, timeStep)
		}
		if regularSequence {
			payload = binary.AppendUvarint(payload, at(0).Sequence)
			payload = binary.AppendUvarint(payload, sequenceStep)
		}
	}
	for i := range count {
		p := at(i)
		if !regularTime {
			payload = binary.AppendVarint(payload, p.Time-timestamp)
		}
		if !regularSequence {
			payload = binary.AppendUvarint(payload, p.Sequence-sequence)
		}
		xor := p.Bits ^ value
		trailing, width := 0, 0
		if xor != 0 {
			trailing = bits.TrailingZeros64(xor) / 8
			width = 8 - bits.LeadingZeros64(xor)/8 - trailing
		}
		payload = append(payload, byte(trailing<<4|width))
		xor >>= uint(trailing * 8)
		for range width {
			payload = append(payload, byte(xor))
			xor >>= 8
		}
		timestamp, sequence, value = p.Time, p.Sequence, p.Bits
	}

	w.payload = payload
	compressed := snappy.Encode(w.compressed[:cap(w.compressed)], payload)
	w.compressed = compressed
	flag := byte(0)
	if len(compressed) < len(payload) {
		payload = compressed
		flag = 1
	}
	offset := len(dst)
	dst = append(dst, make([]byte, 18)...)
	dst[offset] = 1
	dst[offset+1] = flag
	binary.BigEndian.PutUint64(dst[offset+2:], uint64(count))
	binary.BigEndian.PutUint64(dst[offset+10:], uint64(clock))
	return append(dst, payload...), nil
}

// Inspect validates the allocation bound before a decoder allocates scratch.
// maxPoints is supplied by the containing manifest or engine policy.
func Inspect(value []byte, maxPoints int) (Header, error) {
	if len(value) < 18 || value[0] != 1 || value[1] > 1 || maxPoints < 0 {
		return Header{}, errors.New("invalid compact run header")
	}
	n := binary.BigEndian.Uint64(value[2:])
	if n > uint64(maxPoints) || n > 1<<26 {
		return Header{}, errors.New("compact run count exceeds bound")
	}
	size := len(value) - 18
	if value[1] == 1 {
		var err error
		size, err = snappy.DecodedLen(value[18:])
		if err != nil {
			return Header{}, err
		}
	}
	if size < 1 || uint64(size) < n || uint64(size) > 30*n+64 {
		return Header{}, errors.New("compact run count/payload mismatch")
	}
	return Header{Clock: int64(binary.BigEndian.Uint64(value[10:])), Count: int(n), Compressed: value[1] == 1}, nil
}

// Visit decodes straight into the consumer. The only temporary allocation is
// the bounded compressed payload; there is no expanded row allocation.
func Visit(encoded []byte, maxPoints int, visit func(Sample) error) error {
	return VisitWithWorkspace(encoded, maxPoints, visit, nil)
}

// VisitWithWorkspace reuses compressed-payload scratch; samples are copied to
// the visitor by value, and no input or scratch byte slice escapes the call.
func VisitWithWorkspace(encoded []byte, maxPoints int, visit func(Sample) error, w *Workspace) error {
	header, err := Inspect(encoded, maxPoints)
	if err != nil {
		return err
	}
	if visit == nil {
		return errors.New("nil run visitor")
	}
	data := encoded[18:]
	if header.Compressed {
		if w == nil {
			w = &Workspace{}
		}
		data, err = snappy.Decode(w.decoded[:cap(w.decoded)], data)
		if err != nil {
			return err
		}
		w.decoded = data
	}
	timestamp, sequence, value := header.Clock, uint64(0), uint64(0)
	if len(data) == 0 {
		return errors.New("missing compact flags")
	}
	flags := data[0]
	data = data[1:]
	if flags > 3 {
		return errors.New("invalid compact flags")
	}
	var timeStep int64
	var sequenceStep uint64
	if header.Count > 0 {
		if flags&1 != 0 {
			dt, n := binary.Varint(data)
			if n <= 0 {
				return errors.New("invalid compact base time")
			}
			data = data[n:]
			timestamp += dt
			timeStep, n = binary.Varint(data)
			if n <= 0 {
				return errors.New("invalid compact time stride")
			}
			data = data[n:]
		}
		if flags&2 != 0 {
			var n int
			sequence, n = binary.Uvarint(data)
			if n <= 0 {
				return errors.New("invalid compact base sequence")
			}
			data = data[n:]
			sequenceStep, n = binary.Uvarint(data)
			if n <= 0 {
				return errors.New("invalid compact sequence stride")
			}
			data = data[n:]
		}
	}
	for i := range header.Count {
		if flags&1 == 0 {
			dt, n := binary.Varint(data)
			if n <= 0 {
				return errors.New("invalid compact timestamp")
			}
			data = data[n:]
			timestamp += dt
		} else if i > 0 {
			timestamp += timeStep
		}
		if flags&2 == 0 {
			ds, n := binary.Uvarint(data)
			if n <= 0 {
				return errors.New("invalid compact sequence")
			}
			data = data[n:]
			sequence += ds
		} else if i > 0 {
			sequence += sequenceStep
		}
		if len(data) == 0 {
			return errors.New("missing compact value")
		}
		tag := data[0]
		data = data[1:]
		width, trailing := int(tag&15), int(tag>>4)
		if width > 8 || trailing+width > 8 || len(data) < width {
			return errors.New("invalid compact value")
		}
		xor := uint64(0)
		for j := 0; j < width; j++ {
			xor |= uint64(data[j]) << uint((j+trailing)*8)
		}
		data = data[width:]
		value ^= xor
		if err := visit(Sample{Time: timestamp, Sequence: sequence, Bits: value}); err != nil {
			return err
		}
	}
	if len(data) != 0 {
		return errors.New("trailing compact samples")
	}
	return nil
}

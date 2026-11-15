package partsconvert

import (
	"context"
	"encoding/binary"
	"errors"
	"math"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

// StoredMetric is a frozen archive image. It preserves every occupied slot,
// including NaN payloads, negative zero and stale ring occupants. Missing slots
// are absent; an occupied NaN must not be turned into an absent slot.
// The caller supplies already stored coarse values, never newly aggregated raw
// points. Continuation buffers/OOO state need a separately certified adapter.
type StoredMetric struct {
	Name             partsformat.ID
	Group            partsformat.Group
	Method           uint32
	XFilesFactorBits uint32
	Archives         []StoredArchive
}
type StoredPoint struct {
	Timestamp int64
	ValueBits uint64
}
type StoredArchive struct {
	Step, Slots uint32
	Points      []StoredPoint
}

// ArchiveSource adapts a key-ordered per-vshard legacy scan. ScanMetrics must
// emit each canonical metric once and keep the source immutable for the entire
// conversion lifetime. The index is built once by the legacy catalogue owner;
// do not rescan the whole catalogue separately for all 4096 vshards.
type ArchiveSource struct {
	SnapshotID       string
	PartitionSeconds int64
	BlockPoints      int
	// EmptyPartition retains schema-only metrics in the cutover partition.
	EmptyPartition int64
	UpperBound     func(context.Context, Unit) (uint64, error)
	ScanMetrics    func(context.Context, Unit, func(StoredMetric) error) error
}

func (a ArchiveSource) Identity() string { return a.SnapshotID }
func (a ArchiveSource) Estimate(ctx context.Context, u Unit) (uint64, error) {
	if a.UpperBound == nil {
		return 0, errors.New("missing source size bound")
	}
	return a.UpperBound(ctx, u)
}
func (a ArchiveSource) Scan(ctx context.Context, u Unit, yield func(Record) error) error {
	if a.PartitionSeconds < 1 || a.BlockPoints < 1 || a.BlockPoints > 65536 || a.ScanMetrics == nil {
		return errors.New("invalid stored archive source")
	}
	return a.ScanMetrics(ctx, u, func(m StoredMetric) error {
		if string(m.Group) != u.Group || partsformat.Route(m.Name) != u.VShard {
			return errors.New("legacy metric ownership mismatch")
		}
		if len(m.Archives) == 0 || len(m.Archives) > 64 {
			return errors.New("invalid stored retention count")
		}
		for _, ar := range m.Archives {
			if ar.Step == 0 || ar.Slots == 0 {
				return errors.New("invalid stored retention")
			}
		}
		for ai, ar := range m.Archives {
			if len(ar.Points) == 0 {
				key := partsformat.Key{Group: m.Group, VShard: u.VShard, Partition: a.EmptyPartition, Metric: m.Name, Archive: uint32(ai)}
				if err := yield(Record{Key: key, Value: encodeArchive(m, uint32(ai), nil)}); err != nil {
					return err
				}
			}
			for start := 0; start < len(ar.Points); {
				if err := ctx.Err(); err != nil {
					return err
				}
				partition := floorPartition(ar.Points[start].Timestamp, a.PartitionSeconds)
				end := start + 1
				for end < len(ar.Points) && end-start < a.BlockPoints && floorPartition(ar.Points[end].Timestamp, a.PartitionSeconds) == partition {
					end++
				}
				if start > math.MaxUint32 {
					return errors.New("archive ordinal overflow")
				}
				value := encodeArchive(m, uint32(ai), ar.Points[start:end])
				key := partsformat.Key{Group: m.Group, VShard: u.VShard, Partition: partition, Metric: m.Name, Archive: uint32(ai), BlockStart: ar.Points[start].Timestamp, Ordinal: uint32(start)}
				if err := yield(Record{Key: key, Value: value, Points: uint64(end - start)}); err != nil {
					return err
				}
				start = end
			}
		}
		return nil
	})
}
func floorPartition(timestamp, seconds int64) int64 {
	q := timestamp / seconds
	if timestamp%seconds < 0 {
		q--
	}
	return q
}

// Archive payload v1 is deliberately lossless. All numbers are big endian;
// values remain IEEE bits. Metadata repeated in each block makes an immutable
// history part independently interpretable without a host-local metric ID.
func encodeArchive(m StoredMetric, archive uint32, points []StoredPoint) []byte {
	// Reuse the existing exact cold-pack codec, including its zstd path.
	// Signed/out-of-domain timestamps retain a raw IEEE fallback.
	raw := make([]chunkstore.Point, len(points))
	for i, p := range points {
		raw[i] = chunkstore.Point{Timestamp: p.Timestamp, Value: math.Float64frombits(p.ValueBits)}
	}
	sealed, err := chunkstore.EncodeSealedPoints(raw, int(m.Archives[archive].Step))
	codec := uint32(0)
	payload := 16 * len(points)
	if err == nil && len(sealed) < payload {
		codec = 1
		payload = len(sealed)
	}
	b := make([]byte, 28+8*len(m.Archives)+payload)
	copy(b, "GCA2")
	binary.BigEndian.PutUint32(b[4:], m.Method)
	binary.BigEndian.PutUint32(b[8:], m.XFilesFactorBits)
	binary.BigEndian.PutUint32(b[12:], uint32(len(m.Archives)))
	binary.BigEndian.PutUint32(b[16:], archive)
	binary.BigEndian.PutUint32(b[20:], uint32(len(points)))
	binary.BigEndian.PutUint32(b[24:], codec)
	pos := 28
	for _, ar := range m.Archives {
		binary.BigEndian.PutUint32(b[pos:], ar.Step)
		binary.BigEndian.PutUint32(b[pos+4:], ar.Slots)
		pos += 8
	}
	if codec == 1 {
		copy(b[pos:], sealed)
		return b
	}
	for _, p := range points {
		binary.BigEndian.PutUint64(b[pos:], uint64(p.Timestamp))
		binary.BigEndian.PutUint64(b[pos+8:], p.ValueBits)
		pos += 16
	}
	return b
}

// DecodeArchive returns one stored block with its complete original schema.
func DecodeArchive(b []byte) (StoredMetric, uint32, []StoredPoint, error) {
	var m StoredMetric
	if len(b) < 28 || string(b[:4]) != "GCA2" {
		return m, 0, nil, errors.New("invalid archive payload")
	}
	n := uint64(binary.BigEndian.Uint32(b[12:]))
	archive := binary.BigEndian.Uint32(b[16:])
	count := uint64(binary.BigEndian.Uint32(b[20:]))
	codec := binary.BigEndian.Uint32(b[24:])
	if n < 1 || n > 64 || uint64(archive) >= n || count > 65536 || 28+8*n > uint64(len(b)) || codec > 1 || (codec == 0 && 28+8*n+16*count != uint64(len(b))) {
		return m, 0, nil, errors.New("invalid archive payload bounds")
	}
	m.Method = binary.BigEndian.Uint32(b[4:])
	m.XFilesFactorBits = binary.BigEndian.Uint32(b[8:])
	m.Archives = make([]StoredArchive, n)
	pos := 28
	for i := range m.Archives {
		m.Archives[i].Step = binary.BigEndian.Uint32(b[pos:])
		m.Archives[i].Slots = binary.BigEndian.Uint32(b[pos+4:])
		pos += 8
		if m.Archives[i].Step == 0 || m.Archives[i].Slots == 0 {
			return StoredMetric{}, 0, nil, errors.New("invalid archive retention")
		}
	}
	points := make([]StoredPoint, count)
	if codec == 1 {
		decoded, err := chunkstore.DecodeSealedPoints(b[pos:])
		if err != nil {
			return StoredMetric{}, 0, nil, err
		}
		if uint64(len(decoded)) != count {
			return StoredMetric{}, 0, nil, errors.New("sealed archive count mismatch")
		}
		for i, p := range decoded {
			points[i] = StoredPoint{Timestamp: p.Timestamp, ValueBits: math.Float64bits(p.Value)}
		}
		return m, archive, points, nil
	}
	for i := range points {
		points[i] = StoredPoint{int64(binary.BigEndian.Uint64(b[pos:])), binary.BigEndian.Uint64(b[pos+8:])}
		pos += 16
	}
	return m, archive, points, nil
}

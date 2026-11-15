// Package partspack shares a policy descriptor across immutable cold-pack blocks.
// It reuses chunkstore's exact timestamp/value codec without changing its bytes.
package partspack

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

const MaxPoints = chunkstore.MaxSealedPoints

// Retention is one archive's physical ring geometry, in archive order.
type Retention struct{ Step, Slots uint32 }

// Policy belongs to the immutable manifest, once per compatible set of parts.
// Its digest is the existing partsformat.Manifest.Policy. A physical part must
// contain one policy; archive ordinals in its keys index this descriptor.
type Policy struct {
	Method, XFilesFactorBits uint32
	Archives                 []Retention
}

func (p Policy) MarshalBinary() ([]byte, error) {
	if len(p.Archives) == 0 || len(p.Archives) > 64 {
		return nil, errors.New("invalid archive count")
	}
	b := make([]byte, 16+8*len(p.Archives))
	copy(b, "GCP1")
	binary.BigEndian.PutUint32(b[4:], p.Method)
	binary.BigEndian.PutUint32(b[8:], p.XFilesFactorBits)
	binary.BigEndian.PutUint32(b[12:], uint32(len(p.Archives)))
	for i, a := range p.Archives {
		if a.Step == 0 || a.Slots == 0 {
			return nil, errors.New("invalid retention")
		}
		binary.BigEndian.PutUint32(b[16+8*i:], a.Step)
		binary.BigEndian.PutUint32(b[20+8*i:], a.Slots)
	}
	return b, nil
}
func ParsePolicy(b []byte) (Policy, error) {
	var p Policy
	if len(b) < 16 || string(b[:4]) != "GCP1" {
		return p, errors.New("invalid policy")
	}
	n := int(binary.BigEndian.Uint32(b[12:]))
	if n < 1 || n > 64 || len(b) != 16+8*n {
		return p, errors.New("invalid policy length")
	}
	p.Method = binary.BigEndian.Uint32(b[4:])
	p.XFilesFactorBits = binary.BigEndian.Uint32(b[8:])
	p.Archives = make([]Retention, n)
	for i := range p.Archives {
		p.Archives[i] = Retention{binary.BigEndian.Uint32(b[16+8*i:]), binary.BigEndian.Uint32(b[20+8*i:])}
		if p.Archives[i].Step == 0 || p.Archives[i].Slots == 0 {
			return Policy{}, errors.New("invalid retention")
		}
	}
	return p, nil
}
func (p Policy) Digest() ([32]byte, error) {
	b, err := p.MarshalBinary()
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(b), nil
}

// EncodeBlock returns the existing exact cold-pack bytes for ordinary archive
// points. Version zero is an IEEE fallback for signed/out-of-domain timestamps,
// empty archives, and non-increasing stored ring images. It never rounds values.
// The caller bounds the block to its ownership, policy and time partition.
func EncodeBlock(points []chunkstore.Point, step uint32) ([]byte, error) {
	if len(points) > MaxPoints || step == 0 {
		return nil, errors.New("invalid block bounds")
	}
	if b, err := chunkstore.EncodeSealedPoints(points, int(step)); err == nil {
		return b, nil
	}
	b := make([]byte, 9+16*len(points))
	binary.BigEndian.PutUint32(b[1:], uint32(len(points)))
	for i, p := range points {
		binary.BigEndian.PutUint64(b[9+16*i:], uint64(p.Timestamp))
		binary.BigEndian.PutUint64(b[17+16*i:], math.Float64bits(p.Value))
	}
	checksum := crc32.Update(0, crc32.IEEETable, b[:5])
	checksum = crc32.Update(checksum, crc32.IEEETable, b[9:])
	binary.BigEndian.PutUint32(b[5:], checksum)
	return b, nil
}

// DecodeBlock returns owned points, including occupied NaNs and negative zero.
func DecodeBlock(b []byte) ([]chunkstore.Point, error) {
	if len(b) == 0 {
		return nil, errors.New("empty encoded block")
	}
	if b[0] != 0 {
		return chunkstore.DecodeSealedPoints(b)
	}
	if len(b) < 9 {
		return nil, errors.New("truncated raw block")
	}
	n := uint64(binary.BigEndian.Uint32(b[1:]))
	if n > MaxPoints || 9+16*n != uint64(len(b)) {
		return nil, errors.New("invalid raw block length")
	}
	checksum := crc32.Update(0, crc32.IEEETable, b[:5])
	checksum = crc32.Update(checksum, crc32.IEEETable, b[9:])
	if binary.BigEndian.Uint32(b[5:]) != checksum {
		return nil, errors.New("raw block checksum mismatch")
	}
	points := make([]chunkstore.Point, int(n))
	for i := range points {
		points[i] = chunkstore.Point{Timestamp: int64(binary.BigEndian.Uint64(b[9+16*i:])), Value: math.Float64frombits(binary.BigEndian.Uint64(b[17+16*i:]))}
	}
	return points, nil
}

// PartitionOptions chooses an experimental per-archive partition width. The
// minimum base is caller policy. Additional expiry lag is bounded by both the
// archive's horizon divided by HorizonDivisor and MaxSeconds, except when Base
// itself exceeds those caps. Callers must count the resulting retained bytes.
type PartitionOptions struct {
	BaseSeconds, MaxSeconds      int64
	TargetPoints, HorizonDivisor uint32
}

func (o PartitionOptions) Width(a Retention) (int64, error) {
	if a.Step == 0 || a.Slots == 0 || o.BaseSeconds < 1 || o.MaxSeconds < o.BaseSeconds || o.TargetPoints < 1 || o.HorizonDivisor < 1 {
		return 0, errors.New("invalid partition policy")
	}
	target64 := uint64(a.Step) * uint64(o.TargetPoints)
	horizon64 := uint64(a.Step) * uint64(a.Slots)
	if target64 > math.MaxInt64 || horizon64 > math.MaxInt64 {
		return 0, errors.New("partition horizon overflow")
	}
	target := int64(target64)
	horizon := int64(horizon64)
	return max(o.BaseSeconds, min(target, o.MaxSeconds, horizon/int64(o.HorizonDivisor))), nil
}

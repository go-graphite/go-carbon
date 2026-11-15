// Codec copied verbatim from the exact pack encoder; benchmark-local until its package is on the base.
package main

import (
	"encoding/binary"
	"errors"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"hash/crc32"
	"math"
)

const MaxPoints = chunkstore.MaxSealedPoints

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

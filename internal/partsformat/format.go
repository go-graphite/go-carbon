// Package partsformat defines the experimental portable immutable-part format.
// It is a contract for copy experiments, not a production storage format.
package partsformat

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"slices"
)

const VirtualShards = 4096
const MaxNameBytes = 1 << 20

var magic = []byte{'G', 'C', 'P', 2}
var routingTable = crc32.MakeTable(crc32.Castagnoli)

// ID is the exact normalized canonical name, not a host-local catalogue ordinal
// or a truncated hash. Equal IDs therefore mean equal bytes on every replica.
// Names cost more uncompressed key bytes; SST prefix compression amortizes that
// cost. Compact IDs require an independently proved collision/catalogue protocol.
type ID string

func Route(id ID) uint16 {
	return uint16(crc32.Checksum([]byte(id), routingTable) & (VirtualShards - 1))
}

// PartID must be globally unique across all writers, restarts and shard moves.
// A UUID generated once per immutable part is suitable. Local counters alone
// are unsafe: newly assigned owners would overwrite keys copied from peers.
type PartID [16]byte

// Keys sort by ownership, time partition, immutable part, then metric/archive/block.
// Part identity precedes metric identity so separate SSTs have disjoint ranges
// and can be ingested together atomically without rewriting their keys.
type Key struct {
	VShard     uint16
	Partition  int64
	Metric     ID
	Archive    uint32 // portable archive definition, validated by manifest Policy
	BlockStart int64
	Part       PartID
	Ordinal    uint32
}

func PartitionPrefix(vshard uint16, partition int64) ([]byte, error) {
	if vshard >= VirtualShards {
		return nil, errors.New("virtual shard out of range")
	}
	b := make([]byte, 14)
	copy(b, magic)
	binary.BigEndian.PutUint16(b[4:6], vshard)
	binary.BigEndian.PutUint64(b[6:14], uint64(partition)^(1<<63))
	return b, nil
}

// PrefixEnd returns the shortest exclusive upper bound for an entire prefix.
func PrefixEnd(prefix []byte) []byte {
	out := slices.Clone(prefix)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 255 {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}
func VShardRange(vshard uint16) ([]byte, []byte, error) {
	if vshard >= VirtualShards {
		return nil, nil, errors.New("virtual shard out of range")
	}
	lower := make([]byte, 6)
	copy(lower, magic)
	binary.BigEndian.PutUint16(lower[4:], vshard)
	return lower, PrefixEnd(lower), nil
}
func Encode(k Key) ([]byte, error) {
	if len(k.Metric) == 0 || len(k.Metric) > MaxNameBytes {
		return nil, errors.New("invalid canonical name length")
	}
	if Route(k.Metric) != k.VShard {
		return nil, errors.New("metric routes to another virtual shard")
	}
	prefix, err := PartitionPrefix(k.VShard, k.Partition)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 14+4+len(k.Metric)+4+8+16+4)
	copy(b, prefix)
	copy(b[14:30], k.Part[:])
	binary.BigEndian.PutUint32(b[30:], uint32(len(k.Metric)))
	copy(b[34:], k.Metric)
	pos := 34 + len(k.Metric)
	binary.BigEndian.PutUint32(b[pos:], k.Archive)
	pos += 4
	binary.BigEndian.PutUint64(b[pos:], uint64(k.BlockStart)^(1<<63))
	pos += 8
	binary.BigEndian.PutUint32(b[pos:], k.Ordinal)
	return b, nil
}
func Decode(b []byte) (Key, error) {
	var k Key
	if len(b) < 50 || !bytes.Equal(b[:4], magic) {
		return k, errors.New("invalid part key header")
	}
	n := int(binary.BigEndian.Uint32(b[30:34]))
	if n == 0 || n > MaxNameBytes || len(b) != 50+n {
		return k, errors.New("invalid part key length")
	}
	k.VShard = binary.BigEndian.Uint16(b[4:6])
	k.Partition = int64(binary.BigEndian.Uint64(b[6:14]) ^ (1 << 63))
	copy(k.Part[:], b[14:30])
	k.Metric = ID(string(b[34 : 34+n]))
	pos := 34 + n
	k.Archive = binary.BigEndian.Uint32(b[pos:])
	pos += 4
	k.BlockStart = int64(binary.BigEndian.Uint64(b[pos:]) ^ (1 << 63))
	pos += 8
	k.Ordinal = binary.BigEndian.Uint32(b[pos:])
	if k.VShard >= VirtualShards || Route(k.Metric) != k.VShard {
		return Key{}, errors.New("invalid part key routing")
	}
	return k, nil
}

type Part struct {
	ID     PartID
	Bytes  uint64
	SHA256 [32]byte // computed while streaming the immutable file once
}
type Manifest struct {
	VShard    uint16
	Partition int64
	Policy    [32]byte // exact schema/aggregation/encoding compatibility digest
	Parts     []Part
}

// Digest compares immutable part inventories without visiting metric names or
// decoding blocks. Different sealing histories may mismatch conservatively;
// this is deliberately not a claim of logical archive equality.
func (m Manifest) Digest() ([32]byte, error) {
	if m.VShard >= VirtualShards {
		return [32]byte{}, errors.New("virtual shard out of range")
	}
	parts := slices.Clone(m.Parts)
	slices.SortFunc(parts, func(a, b Part) int { return bytes.Compare(a.ID[:], b.ID[:]) })
	h := sha256.New()
	_, _ = h.Write([]byte("immutable-part-manifest-v2"))
	prefix, _ := PartitionPrefix(m.VShard, m.Partition)
	_, _ = h.Write(prefix)
	_, _ = h.Write(m.Policy[:])
	for i, p := range parts {
		if i > 0 && p.ID == parts[i-1].ID {
			return [32]byte{}, errors.New("duplicate immutable part ID")
		}
		_, _ = h.Write(p.ID[:])
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], p.Bytes)
		_, _ = h.Write(n[:])
		_, _ = h.Write(p.SHA256[:])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out, nil
}

// ValidateFileRange prevents accidental ingestion of an SST spanning ownership
// or retention boundaries. Writers must emit one file per (vshard,partition).
// Merely prefixing keys does not make arbitrary mixed-range SSTs portable.
func ValidateFileRange(m Manifest, smallest, largest []byte) error {
	lo, err := PartitionPrefix(m.VShard, m.Partition)
	if err != nil {
		return err
	}
	hi := PrefixEnd(lo)
	if bytes.Compare(smallest, largest) > 0 || bytes.Compare(smallest, lo) < 0 || bytes.Compare(largest, hi) >= 0 {
		return errors.New("SST crosses virtual shard/time partition boundary")
	}
	return nil
}

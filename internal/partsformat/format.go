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
	"strings"
)

const VirtualShards = 4096
const MaxNameBytes = 1 << 20

var magic = []byte{'G', 'C', 'P', 3}
var routingTable = crc32.MakeTable(crc32.Castagnoli)

// ID is the exact normalized canonical name, not a host-local catalogue ordinal
// or a truncated hash. Equal IDs therefore mean equal bytes on every replica.
// Names cost more uncompressed key bytes; SST prefix compression amortizes that
// cost. Compact IDs require an independently proved collision/catalogue protocol.
type ID string

// Group is the exact portable placement label persisted by the name catalogue.
// It is not a hash or a host-local ordinal. Empty is reserved for legacy tests.
type Group string
type PlacementRule struct {
	Prefix string
	Group  Group
}

// AssignGroup applies only at admission. Persist the result: changing rules must
// never silently reroute existing names. Rules match complete path components;
// the longest prefix wins. Canonical tag suffixes do not change namespace.
func AssignGroup(id ID, rules []PlacementRule) (Group, error) {
	base, _, _ := strings.Cut(string(id), ";")
	if base == "" {
		return "", errors.New("empty canonical base name")
	}
	var group Group
	longest := -1
	for _, rule := range rules {
		if rule.Prefix == "" || rule.Group == "" {
			return "", errors.New("invalid placement rule")
		}
		if base == rule.Prefix || strings.HasPrefix(base, rule.Prefix+".") {
			if len(rule.Prefix) == longest && group != rule.Group {
				return "", errors.New("ambiguous placement rule")
			}
			if len(rule.Prefix) > longest {
				group = rule.Group
				longest = len(rule.Prefix)
			}
		}
	}
	if longest >= 0 {
		return group, nil
	}
	first, _, _ := strings.Cut(base, ".")
	if first == "" {
		return "", errors.New("empty default namespace")
	}
	return Group(first), nil
}
func GroupRange(group Group) ([]byte, []byte, error) {
	if len(group) > MaxNameBytes {
		return nil, nil, errors.New("placement group too long")
	}
	lower := make([]byte, 8+len(group))
	copy(lower, magic)
	binary.BigEndian.PutUint32(lower[4:8], uint32(len(group)))
	copy(lower[8:], group)
	return lower, PrefixEnd(lower), nil
}

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
	Group      Group
	VShard     uint16
	Partition  int64
	Metric     ID
	Archive    uint32 // portable archive definition, validated by manifest Policy
	BlockStart int64
	Part       PartID
	Ordinal    uint32
}

func PartitionPrefix(vshard uint16, partition int64) ([]byte, error) {
	return PartitionPrefixForGroup("", vshard, partition)
}
func PartitionPrefixForGroup(group Group, vshard uint16, partition int64) ([]byte, error) {
	if vshard >= VirtualShards {
		return nil, errors.New("virtual shard out of range")
	}
	lower, _, err := GroupRange(group)
	if err != nil {
		return nil, err
	}
	pos := len(lower)
	b := make([]byte, pos+10)
	copy(b, lower)
	binary.BigEndian.PutUint16(b[pos:pos+2], vshard)
	binary.BigEndian.PutUint64(b[pos+2:], uint64(partition)^(1<<63))
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
	return VShardRangeForGroup("", vshard)
}
func VShardRangeForGroup(group Group, vshard uint16) ([]byte, []byte, error) {
	if vshard >= VirtualShards {
		return nil, nil, errors.New("virtual shard out of range")
	}
	lower, _, err := GroupRange(group)
	if err != nil {
		return nil, nil, err
	}
	lower = binary.BigEndian.AppendUint16(lower, vshard)
	return lower, PrefixEnd(lower), nil
}
func Encode(k Key) ([]byte, error) {
	if len(k.Metric) == 0 || len(k.Metric) > MaxNameBytes {
		return nil, errors.New("invalid canonical name length")
	}
	if Route(k.Metric) != k.VShard {
		return nil, errors.New("metric routes to another virtual shard")
	}
	prefix, err := PartitionPrefixForGroup(k.Group, k.VShard, k.Partition)
	if err != nil {
		return nil, err
	}
	b := make([]byte, len(prefix)+36+len(k.Metric))
	copy(b, prefix)
	pos := len(prefix)
	copy(b[pos:pos+16], k.Part[:])
	pos += 16
	binary.BigEndian.PutUint32(b[pos:], uint32(len(k.Metric)))
	pos += 4
	copy(b[pos:], k.Metric)
	pos += len(k.Metric)
	binary.BigEndian.PutUint32(b[pos:], k.Archive)
	pos += 4
	binary.BigEndian.PutUint64(b[pos:], uint64(k.BlockStart)^(1<<63))
	pos += 8
	binary.BigEndian.PutUint32(b[pos:], k.Ordinal)
	return b, nil
}
func Decode(b []byte) (Key, error) {
	var k Key
	if len(b) < 54 || !bytes.Equal(b[:4], magic) {
		return k, errors.New("invalid part key header")
	}
	gn32 := binary.BigEndian.Uint32(b[4:8])
	if gn32 > MaxNameBytes {
		return k, errors.New("invalid placement group length")
	}
	gn := int(gn32)
	if gn > len(b)-54 {
		return k, errors.New("invalid placement group length")
	}
	k.Group = Group(string(b[8 : 8+gn]))
	pos := 8 + gn
	k.VShard = binary.BigEndian.Uint16(b[pos : pos+2])
	pos += 2
	k.Partition = int64(binary.BigEndian.Uint64(b[pos:pos+8]) ^ (1 << 63))
	pos += 8
	copy(k.Part[:], b[pos:pos+16])
	pos += 16
	n32 := binary.BigEndian.Uint32(b[pos : pos+4])
	pos += 4
	if n32 == 0 || n32 > MaxNameBytes {
		return Key{}, errors.New("invalid part key length")
	}
	n := int(n32)
	if len(b) != 54+gn+n {
		return Key{}, errors.New("invalid part key length")
	}
	k.Metric = ID(string(b[pos : pos+n]))
	pos += n
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
	Group     Group
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
	_, _ = h.Write([]byte("immutable-part-manifest-v3"))
	prefix, err := PartitionPrefixForGroup(m.Group, m.VShard, m.Partition)
	if err != nil {
		return [32]byte{}, err
	}
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
// or retention boundaries. Writers must emit one file per (group,vshard,partition).
// Merely prefixing keys does not make arbitrary mixed-range SSTs portable.
func ValidateFileRange(m Manifest, smallest, largest []byte) error {
	lo, err := PartitionPrefixForGroup(m.Group, m.VShard, m.Partition)
	if err != nil {
		return err
	}
	hi := PrefixEnd(lo)
	if bytes.Compare(smallest, largest) > 0 || bytes.Compare(smallest, lo) < 0 || bytes.Compare(largest, hi) >= 0 {
		return errors.New("SST crosses virtual shard/time partition boundary")
	}
	return nil
}

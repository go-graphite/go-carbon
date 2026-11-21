// Package partsformat defines the experimental portable immutable-part format.
// It is a contract for copy experiments, not a production storage format.
package partsformat

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"slices"
	"strings"
	"unsafe"
)

const VirtualShards = 4096
const MaxNameBytes = 1 << 20

var magic = []byte{'G', 'C', 'P', 3}

// idMagic starts ID-keyed (GCI2) keys. The magic is the key generation:
// GCP3 keys carry the canonical name, GCI2 keys a vshard-local dictionary ID.
var idMagic = []byte{'G', 'C', 'I', 2}

// Key tails: the bytes after a key's identity prefix. GCP3 ends with archive
// u32, physical slot u64 and ordinal u32; GCI2 with archive u8 (IDEnvelope for
// the metric envelope) and slot u32. A GCI2 key has no ordinal (always 0).
const (
	NameKeyTail = 16
	IDKeyTail   = 5
	IDEnvelope  = 0xff
)

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
func GroupRange(group Group) ([]byte, []byte, error) { return groupRange(magic, group) }

// IDGroupRange is GroupRange for ID-keyed (GCI2) keys.
func IDGroupRange(group Group) ([]byte, []byte, error) { return groupRange(idMagic, group) }

func groupRange(m []byte, group Group) ([]byte, []byte, error) {
	if len(group) > MaxNameBytes {
		return nil, nil, errors.New("placement group too long")
	}
	lower := make([]byte, 8+len(group))
	copy(lower, m)
	binary.BigEndian.PutUint32(lower[4:8], uint32(len(group)))
	copy(lower[8:], group)
	return lower, PrefixEnd(lower), nil
}

func Route(id ID) uint16 {
	// Checksum only reads its input; avoid copying every metric name to hash it.
	return uint16(crc32.Checksum(unsafe.Slice(unsafe.StringData(string(id)), len(id)), routingTable) & (VirtualShards - 1))
}

// RouteBytes is Route over encoded name bytes.
func RouteBytes(name []byte) uint16 {
	return uint16(crc32.Checksum(name, routingTable) & (VirtualShards - 1))
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
	// ByID marks an ID-keyed (GCI2) key: Local is the metric's vshard-local
	// dictionary ID and Metric is empty. Name keys (GCP3) leave both zero.
	ByID  bool
	Local uint32
}

// SameMetric reports whether a and b name the same metric of one key
// generation: equal names, or equal dictionary IDs.
func SameMetric(a, b Key) bool { return a.Metric == b.Metric && a.Local == b.Local && a.ByID == b.ByID }

func PartitionPrefixForGroup(group Group, vshard uint16, partition int64) ([]byte, error) {
	return partitionPrefix(magic, group, vshard, partition)
}

// IDPartitionPrefixForGroup is PartitionPrefixForGroup for GCI2 keys.
func IDPartitionPrefixForGroup(group Group, vshard uint16, partition int64) ([]byte, error) {
	return partitionPrefix(idMagic, group, vshard, partition)
}

func partitionPrefix(m []byte, group Group, vshard uint16, partition int64) ([]byte, error) {
	if vshard >= VirtualShards {
		return nil, errors.New("virtual shard out of range")
	}
	lower, _, err := groupRange(m, group)
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
	if k.ByID {
		return EncodeID(k)
	}
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

// EncodeID encodes an ID-keyed (GCI2) key:
// magic | u32 len(group) | group | vshard u16 | partition u64^ | part [16] |
// local ID u32 | archive u8 | slot u32, all big endian, so keys sort by
// ownership, partition, part, ID, archive, then slot.
func EncodeID(k Key) ([]byte, error) {
	if !k.ByID || k.Metric != "" {
		return nil, errors.New("ID-keyed part key carries a name")
	}
	prefix, err := IDPartitionPrefixForGroup(k.Group, k.VShard, k.Partition)
	if err != nil {
		return nil, err
	}
	b := append(prefix, k.Part[:]...)
	b = binary.BigEndian.AppendUint32(b, k.Local)
	return AppendIDTail(b, k.Archive, k.BlockStart, k.Ordinal)
}

// AppendIDTail appends a GCI2 key's archive and slot. It rejects what GCI2
// cannot hold: archives from 255 up (other than the envelope), slots outside
// [0, 2^32) and nonzero ordinals.
func AppendIDTail(dst []byte, archive uint32, slot int64, ordinal uint32) ([]byte, error) {
	a := byte(IDEnvelope)
	if archive != ^uint32(0) {
		if archive >= IDEnvelope {
			return nil, errors.New("archive does not fit an ID-keyed part key")
		}
		a = byte(archive)
	}
	if slot < 0 || slot > math.MaxUint32 || ordinal != 0 {
		return nil, errors.New("slot or ordinal does not fit an ID-keyed part key")
	}
	return binary.BigEndian.AppendUint32(append(dst, a), uint32(slot)), nil
}

// IsIDKey reports whether b starts with the GCI2 magic.
func IsIDKey(b []byte) bool { return bytes.HasPrefix(b, idMagic) }

// DecodeKey decodes either key generation. A GCP3 key decodes as DecodeName
// does; a GCI2 key returns ByID set, its ID in Local and a nil name.
func DecodeKey(b []byte, group Group, metric ID) (Key, []byte, error) {
	if !IsIDKey(b) {
		k, name, err := DecodeName(b, group)
		if err == nil {
			if string(name) == string(metric) {
				k.Metric = metric
			} else {
				k.Metric = ID(string(name))
			}
		}
		return k, name, err
	}
	var k Key
	if len(b) < 8 {
		return k, nil, errors.New("invalid part key header")
	}
	gn32 := binary.BigEndian.Uint32(b[4:8])
	if gn32 > MaxNameBytes || int(gn32) != len(b)-43 {
		return k, nil, errors.New("invalid ID-keyed part key length")
	}
	gn := int(gn32)
	if string(b[8:8+gn]) == string(group) {
		k.Group = group
	} else {
		k.Group = Group(string(b[8 : 8+gn]))
	}
	pos := 8 + gn
	k.VShard = binary.BigEndian.Uint16(b[pos:])
	k.Partition = int64(binary.BigEndian.Uint64(b[pos+2:]) ^ (1 << 63))
	copy(k.Part[:], b[pos+10:pos+26])
	k.ByID, k.Local = true, binary.BigEndian.Uint32(b[pos+26:])
	if k.VShard >= VirtualShards {
		return Key{}, nil, errors.New("invalid part key routing")
	}
	DecodeIDTail(&k, b[pos+30:])
	return k, nil, nil
}

// DecodeIDTail sets k's archive and slot from a GCI2 key tail.
func DecodeIDTail(k *Key, tail []byte) {
	k.Archive = uint32(tail[0])
	if tail[0] == IDEnvelope {
		k.Archive = ^uint32(0)
	}
	k.BlockStart, k.Ordinal = int64(binary.BigEndian.Uint32(tail[1:])), 0
}

func Decode(b []byte) (Key, error) { return DecodeReusing(b, "", "") }

// DecodeReusing is Decode for callers that already hold the likely group and
// metric strings: a matching encoded group or name shares the given string
// instead of copying the key bytes. Validation and decoded values are
// unchanged; a non-matching hint only costs the usual copy.
func DecodeReusing(b []byte, group Group, metric ID) (Key, error) {
	k, name, err := DecodeName(b, group)
	if err != nil {
		return k, err
	}
	if string(name) == string(metric) {
		k.Metric = metric
	} else {
		k.Metric = ID(string(name))
	}
	return k, nil
}

// DecodeName validates b exactly as Decode does but leaves Key.Metric empty and
// returns the name bytes as a sub-slice of b, so a caller that compares or
// copies names allocates no string per key.
func DecodeName(b []byte, group Group) (Key, []byte, error) {
	var k Key
	if len(b) < 54 || !bytes.Equal(b[:4], magic) {
		return k, nil, errors.New("invalid part key header")
	}
	gn32 := binary.BigEndian.Uint32(b[4:8])
	if gn32 > MaxNameBytes {
		return k, nil, errors.New("invalid placement group length")
	}
	gn := int(gn32)
	if gn > len(b)-54 {
		return k, nil, errors.New("invalid placement group length")
	}
	if string(b[8:8+gn]) == string(group) {
		k.Group = group
	} else {
		k.Group = Group(string(b[8 : 8+gn]))
	}
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
		return Key{}, nil, errors.New("invalid part key length")
	}
	n := int(n32)
	if len(b) != 54+gn+n {
		return Key{}, nil, errors.New("invalid part key length")
	}
	name := b[pos : pos+n : pos+n]
	pos += n
	k.Archive = binary.BigEndian.Uint32(b[pos:])
	pos += 4
	k.BlockStart = int64(binary.BigEndian.Uint64(b[pos:]) ^ (1 << 63))
	pos += 8
	k.Ordinal = binary.BigEndian.Uint32(b[pos:])
	if k.VShard >= VirtualShards || RouteBytes(name) != k.VShard {
		return Key{}, nil, errors.New("invalid part key routing")
	}
	return k, name, nil
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

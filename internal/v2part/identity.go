// Package v2part defines the portable immutable SST and inventory contract shared
// by core, offline conversion and whole-part transport. It does not install files
// into a live store or manage their snapshot/GC lifetime.
package v2part

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync/atomic"

	"github.com/go-graphite/go-carbon/internal/partsformat"
)

// Keep the published GCP3 envelope byte-for-byte. These are aliases, so existing
// producers and readers need no ID remapping or second key encoder.
type ID = partsformat.ID
type Group = partsformat.Group
type PartID = partsformat.PartID
type Key = partsformat.Key

const VirtualShards = partsformat.VirtualShards
const MaxNameBytes = partsformat.MaxNameBytes
const KeyVersion = 3
const SchemaVersion = 1

// GroupFileVersion versions all dense descriptors, optionally spanning the
// vshards of one independently movable placement group. Key bytes remain GCP3.
const GroupFileVersion = 2

// IDFileVersion is the descriptor schema of ID-keyed dense group files: their
// keys are GCI2 (IDKeyVersion) and the descriptor records the dictionary
// generation its IDs need (DictGeneration). Readers dispatch per file on the schema.
const IDFileVersion = 3
const IDKeyVersion = 2

// IDKeys switches ID-keyed parts on. Off (the default), writers refuse
// IDKeyed and descriptors of schema IDFileVersion fail validation, so
// this build accepts exactly the parts the previous one did, and no ID-keyed
// part can reach a reader that only seeks name keys. The seal turns it on
// once core reads resolve IDs through the name dictionary.
var IDKeys atomic.Bool

const GroupVShard uint16 = 65535
const MetadataArchive = ^uint32(0)

func Encode(k Key) ([]byte, error) { return partsformat.Encode(k) }
func Decode(b []byte) (Key, error) { return partsformat.Decode(b) }
func SameMetric(a, b Key) bool     { return partsformat.SameMetric(a, b) }

// DecodeKey decodes either key generation (GCP3 name keys, GCI2 ID keys).
func DecodeKey(b []byte) (Key, error) {
	k, _, err := partsformat.DecodeKey(b, "", "")
	return k, err
}

// keyTail is the length of the bytes after k's identity prefix.
func keyTail(byID bool) int {
	if byID {
		return partsformat.IDKeyTail
	}
	return partsformat.NameKeyTail
}

// setKeySlot overwrites the physical slot of encoded key b in place.
func setKeySlot(b []byte, slot int64) bool {
	if partsformat.IsIDKey(b) {
		if slot < 0 || slot > 1<<32-1 {
			return false
		}
		binary.BigEndian.PutUint32(b[len(b)-4:], uint32(slot))
		return true
	}
	binary.BigEndian.PutUint64(b[len(b)-12:], uint64(slot)^(1<<63))
	return true
}
func Route(id ID) uint16                                { return partsformat.Route(id) }
func DecodeName(b []byte, g Group) (Key, []byte, error) { return partsformat.DecodeName(b, g) }
func GroupRange(g Group) ([]byte, []byte, error)        { return partsformat.GroupRange(g) }
func PrefixEnd(b []byte) []byte                         { return partsformat.PrefixEnd(b) }
func PartitionPrefixForGroup(g Group, v uint16, p int64) ([]byte, error) {
	return partsformat.PartitionPrefixForGroup(g, v, p)
}

// Scope is independent of local owner, DB, worker, physical store and placement
// epoch. Part IDs need only be unique within this complete scope, as in GCP3.
type Scope struct {
	Group     Group
	VShard    uint16
	Partition int64
	Part      PartID
}

func (s Scope) contains(k Key) bool {
	return k.Group == s.Group && (k.VShard == s.VShard || s.VShard == GroupVShard) && k.Partition == s.Partition && k.Part == s.Part
}

// ValidateRange checks the physical file scope. GroupVShard is descriptor-only:
// schema-2 dense files can span routed vshards of one group/partition/part.
// Readers additionally inspect interior keys; min/max alone cannot validate an
// untrusted file's contents.
func (s Scope) ValidateRange(first, last []byte) error {
	a, err := DecodeKey(first)
	if err != nil {
		return err
	}
	b, err := DecodeKey(last)
	if err != nil {
		return err
	}
	if !s.contains(a) || !s.contains(b) || a.ByID != b.ByID || bytes.Compare(first, last) > 0 {
		return errors.New("part key range outside scope")
	}
	return nil
}

// keyDecoder retains only a fully validated immutable identity prefix. GCP3's
// final 16 bytes contain archive, physical slot and ordinal; changing any byte
// before that boundary requires normal full validation, including route hashing.
// Prefix bytes and strings are owned, so callers can reuse their input buffer.
type keyDecoder struct {
	prefix []byte
	tail   int // key tail length after prefix; 0 before the first decode
	key    Key
	// name is an optional caller hint for the next identity. A full decode
	// shares it, and the previous group, when the key bytes match.
	name ID
}

func (d *keyDecoder) decode(encoded []byte) (Key, error) {
	if t := d.tail; t > 0 && len(d.prefix) == len(encoded)-t && bytes.Equal(d.prefix, encoded[:len(encoded)-t]) {
		k := d.key
		tail := encoded[len(encoded)-t:]
		if k.ByID {
			partsformat.DecodeIDTail(&k, tail)
			return k, nil
		}
		k.Archive = binary.BigEndian.Uint32(tail)
		k.BlockStart = int64(binary.BigEndian.Uint64(tail[4:]) ^ (1 << 63))
		k.Ordinal = binary.BigEndian.Uint32(tail[12:])
		return k, nil
	}
	k, _, err := partsformat.DecodeKey(encoded, d.key.Group, d.name)
	if err != nil {
		return Key{}, err
	}
	d.tail = keyTail(k.ByID)
	d.prefix = append(d.prefix[:0], encoded[:len(encoded)-d.tail]...)
	d.key = k
	return k, nil
}

// KeyDecoder reuses the validated identity prefix across neighboring archive
// keys. Returned names remain owned after the source iterator advances.
type KeyDecoder struct{ keyDecoder }

func (d *KeyDecoder) Decode(encoded []byte) (Key, error) { return d.decode(encoded) }

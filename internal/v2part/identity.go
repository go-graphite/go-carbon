// Package v2part defines the portable immutable SST and inventory contract shared
// by core, offline conversion and whole-part transport. It does not install files
// into a live store or manage their snapshot/GC lifetime.
package v2part

import (
	"bytes"
	"encoding/binary"
	"errors"

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
const GroupVShard uint16 = 65535
const MetadataArchive = ^uint32(0)

func Encode(k Key) ([]byte, error)               { return partsformat.Encode(k) }
func Decode(b []byte) (Key, error)               { return partsformat.Decode(b) }
func Route(id ID) uint16                         { return partsformat.Route(id) }
func GroupRange(g Group) ([]byte, []byte, error) { return partsformat.GroupRange(g) }
func PrefixEnd(b []byte) []byte                  { return partsformat.PrefixEnd(b) }
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
	a, err := Decode(first)
	if err != nil {
		return err
	}
	b, err := Decode(last)
	if err != nil {
		return err
	}
	if !s.contains(a) || !s.contains(b) || bytes.Compare(first, last) > 0 {
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
	key    Key
}

func (d *keyDecoder) decode(encoded []byte) (Key, error) {
	if len(encoded) >= 54 && len(d.prefix) == len(encoded)-16 && bytes.Equal(d.prefix, encoded[:len(encoded)-16]) {
		k := d.key
		tail := encoded[len(encoded)-16:]
		k.Archive = binary.BigEndian.Uint32(tail)
		k.BlockStart = int64(binary.BigEndian.Uint64(tail[4:]) ^ (1 << 63))
		k.Ordinal = binary.BigEndian.Uint32(tail[12:])
		return k, nil
	}
	k, err := Decode(encoded)
	if err != nil {
		return Key{}, err
	}
	d.prefix = append(d.prefix[:0], encoded[:len(encoded)-16]...)
	d.key = k
	return k, nil
}

// KeyDecoder reuses the validated identity prefix across neighboring archive
// keys. Returned names remain owned after the source iterator advances.
type KeyDecoder struct{ keyDecoder }

func (d *KeyDecoder) Decode(encoded []byte) (Key, error) { return d.decode(encoded) }

package v2core

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/v2dict"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Readers of both key generations. A part record key ends in a tail after
// the metric identity: GCP3 archive u32, slot u64^ and ordinal u32; GCI2
// archive u8 (0xff = envelope) and slot u32.

// idKeyMagic prefixes every ID-keyed (GCI2) part record key.
var idKeyMagic = []byte{'G', 'C', 'I', 2}

// errIDKeyedMove reports an ID-keyed part on a path that moves parts with
// their names (copies, adoption, portable export): moving ID-keyed parts
// needs the dictionary key range as well (C2c) and is not supported yet.
var errIDKeyedMove = errors.New("ID-keyed parts cannot be moved or copied yet")

// partKeyTailOf returns the tail length of part record key k, or 0 when k
// is not a part record key of either generation.
func partKeyTailOf(k []byte) int {
	switch {
	case len(k) >= partsformat.NameKeyTail && bytes.HasPrefix(k, partKeyMagic):
		return partsformat.NameKeyTail
	case len(k) >= partsformat.IDKeyTail && bytes.HasPrefix(k, idKeyMagic):
		return partsformat.IDKeyTail
	}
	return 0
}

// isEnvelopeKey reports whether part key k, with tail length tail, is its
// metric's envelope (metadata) key.
func isEnvelopeKey(k []byte, tail int) bool {
	if tail == partsformat.IDKeyTail {
		return k[len(k)-tail] == partsformat.IDEnvelope
	}
	return binary.BigEndian.Uint32(k[len(k)-tail:]) == v2part.MetadataArchive
}

// appendEnvelopeKey appends the envelope key of part key k's metric.
func appendEnvelopeKey(dst, k []byte, tail int) []byte {
	dst = append(dst, k[:len(k)-tail]...)
	if tail == partsformat.IDKeyTail {
		dst = append(dst, partsformat.IDEnvelope)
		return binary.BigEndian.AppendUint32(dst, 0)
	}
	dst = binary.BigEndian.AppendUint32(dst, v2part.MetadataArchive)
	dst = binary.BigEndian.AppendUint64(dst, 1<<63)
	return binary.BigEndian.AppendUint32(dst, 0)
}

// partKeyRanges are the key ranges of both part key generations, for scans
// over every part record.
var partKeyRanges = [2][]byte{partKeyMagic, idKeyMagic}

// archivePrefixLen is the length of encoded key k up to and including its
// archive: the prefix every record of one metric archive in one part shares.
func archivePrefixLen(k []byte, byID bool) int {
	if byID {
		return len(k) - 4 // slot u32
	}
	return len(k) - 12 // slot u64, ordinal u32
}

// metricIDs sets one metric's identity in scope keys of either generation,
// looking its dictionary ID up once per (group, vshard).
type metricIDs struct {
	dict   *v2dict.Dict
	group  v2part.Group
	vshard uint16
	id     uint32
	state  uint8 // 0 not looked up, 1 found, 2 no live ID
}

// key is metricKey with the lookup cached.
func (c *metricIDs) key(k v2part.Key, name v2part.ID) (v2part.Key, bool, error) {
	if !k.ByID {
		k.Metric, k.Local = name, 0
		return k, true, nil
	}
	if c.state == 0 || c.group != k.Group || c.vshard != k.VShard {
		found, ok, err := metricKey(k, name, c.dict)
		if err != nil {
			return k, false, err
		}
		c.group, c.vshard, c.id, c.state = k.Group, k.VShard, found.Local, 2
		if ok {
			c.state = 1
		}
	}
	if c.state != 1 {
		return k, false, nil
	}
	k.Metric, k.Local = "", c.id
	return k, true, nil
}

// snapshotHasIDKeyedParts reports whether any published descriptor in
// snapshot is ID-keyed. It reads only the small descriptor namespace.
func snapshotHasIDKeyedParts(snapshot *pebble.Snapshot) (bool, error) {
	if snapshot == nil {
		return false, nil
	}
	it, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: sealedPartPrefix, UpperBound: v2part.PrefixEnd(sealedPartPrefix)})
	if err != nil {
		return false, err
	}
	found := false
	for valid := it.First(); valid && !found; valid = it.Next() {
		d, err := v2part.ParseDescriptor(it.Value())
		if err != nil {
			return false, errors.Join(err, it.Close())
		}
		found = descriptorByID(d)
	}
	return found, errors.Join(it.Error(), it.Close())
}

// keyNames fills in the name of decoded ID-keyed keys, caching the last ID:
// one metric's keys are adjacent in a part, so a scan resolves each metric
// once per part.
type keyNames struct {
	dict    *v2dict.Dict
	group   v2part.Group
	vshard  uint16
	id      uint32
	name    v2part.ID
	cached  bool
	scratch []byte
}

// resolve sets k.Metric for an ID-keyed key; name keys pass unchanged.
func (c *keyNames) resolve(k *v2part.Key) error {
	if !k.ByID {
		return nil
	}
	if !c.cached || c.id != k.Local || c.vshard != k.VShard || c.group != k.Group {
		name, err := keyName(*k, c.dict, c.scratch)
		if err != nil {
			return err
		}
		c.group, c.vshard, c.id, c.name, c.cached = k.Group, k.VShard, k.Local, name, true
	}
	k.Metric = c.name
	return nil
}

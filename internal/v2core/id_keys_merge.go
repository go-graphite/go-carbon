package v2core

import (
	"encoding/binary"
	"errors"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/v2dict"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Merges of either key generation. A merge's inputs share one generation
// (selectMergeLimit splits windows at a generation change), and the output
// keeps it. The cursor keeps the metric's identity encoded: GCP3 the u32
// length and the name, GCI2 the u32 big-endian ID, so comparing those bytes
// orders metrics as their keys sort and merges never resolve a name per key.

// decodeMergeKey decodes a part record key of either generation without
// building a name string: a GCP3 key returns its encoded name, a GCI2 key
// its ID in k.Local and a nil name.
func decodeMergeKey(encoded []byte, group v2part.Group) (v2part.Key, []byte, error) {
	if partsformat.IsIDKey(encoded) {
		k, _, err := partsformat.DecodeKey(encoded, group, "")
		return k, nil, err
	}
	return v2part.DecodeName(encoded, group)
}

// setKeyTail sets k's archive, slot and ordinal from an encoded key tail of
// either generation (partKeyTail bytes).
func setKeyTail(k *v2part.Key, tail []byte) {
	if len(tail) == partsformat.IDKeyTail {
		partsformat.DecodeIDTail(k, tail)
		return
	}
	k.Archive = binary.BigEndian.Uint32(tail)
	k.BlockStart = int64(binary.BigEndian.Uint64(tail[4:]) ^ (1 << 63))
	k.Ordinal = binary.BigEndian.Uint32(tail[12:])
}

// putKeyTail writes archive and slot into an encoded key tail of either
// generation, leaving a GCP3 ordinal as it is. GCI2 merge inputs only hold
// archives and slots that fit their tail.
func putKeyTail(tail []byte, archive uint32, slot int64) {
	if len(tail) == partsformat.IDKeyTail {
		a := byte(archive)
		if archive == v2part.MetadataArchive {
			a = partsformat.IDEnvelope
		}
		tail[0] = a
		binary.BigEndian.PutUint32(tail[1:], uint32(slot))
		return
	}
	binary.BigEndian.PutUint32(tail, archive)
	binary.BigEndian.PutUint64(tail[4:], uint64(slot)^(1<<63))
}

// mergeGeneration checks that inputs share one key generation and returns
// the dictionary generation the output needs (the newest input's), 0 for
// name-keyed inputs.
func mergeGeneration(inputs []v2part.Descriptor) (uint64, error) {
	byID := descriptorByID(inputs[0])
	var generation uint64
	for _, d := range inputs {
		if descriptorByID(d) != byID {
			return 0, errors.New("merge mixes part key generations")
		}
		generation = max(generation, d.DictGeneration)
	}
	return generation, nil
}

// mergeNames resolves the dictionary IDs of one GCI2 merge, once per metric.
type mergeNames struct {
	dict *v2dict.Dict
	// identities is the store catalog, whose identities remember their
	// current dictionary ID; nil reads every ID from the dictionary.
	identities *PolicyCatalog
	name       []byte
}

// live reports whether the merge keeps ID-keyed metric k: its ID is the
// newest entry of a name that is not deleted. A deleted name's ID, and the
// old ID of a revived name (assigned, but no name's newest entry), drop
// with all their records. An ID the dictionary never assigned is an error,
// never a drop: a lost or foreign dictionary must not delete data. Without
// a dictionary every metric is kept.
//
// The common case reads no dictionary: the identity of the envelope's
// numeric ID (metaID) holds k's ID as its current one, set by the seal that
// appended the name or by a read that resolved it.
func (n *mergeNames) live(k v2part.Key, metaID uint64) (bool, error) {
	if n.dict == nil || !k.ByID {
		return true, nil
	}
	if n.identities != nil {
		if id, err := n.identities.keyEngineID(metaID, k); err == nil {
			metaID = id
		}
		if h := n.identities.lookup(metaID); h != nil && h.dictID.Load() == k.Local+1 {
			return true, nil
		}
	}
	var e v2dict.Entry
	var ok bool
	n.name, e, ok = n.dict.LookupID(string(k.Group), k.VShard, k.Local, n.name[:0])
	if ok {
		return !e.Deleted(), nil
	}
	if k.Local >= n.dict.NextID(string(k.Group), k.VShard) {
		return false, errors.New("ID-keyed part holds an ID the store dictionary never assigned")
	}
	return false, nil
}

// definitionName returns metric k's name for a full metadata definition:
// the encoded GCP3 name, or the dictionary name of a GCI2 ID.
func (n *mergeNames) definitionName(k v2part.Key, encodedName []byte) (v2part.ID, error) {
	if !k.ByID {
		return v2part.ID(encodedName), nil
	}
	name, err := keyName(k, n.dict, n.name)
	n.name = n.name[:0]
	return name, err
}

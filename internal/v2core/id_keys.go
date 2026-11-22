package v2core

import (
	"errors"
	"path/filepath"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/v2dict"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// ID-keyed (GCI2) parts key each metric by its vshard-local ID in the store's
// name dictionary instead of its name. The dictionary hangs off the store's
// PolicyCatalog, the in-memory name interner, so every reader that already
// resolves names through the catalog resolves IDs through the same handle.
// A nil dictionary means name keys (GCP3) only: the seal writes GCP3 parts
// and a reader that meets a GCI2 key fails loudly.

// storeDictDir is the dictionary directory under the core root.
func storeDictDir(root string) string { return filepath.Join(root, "dict") }

// errNoPartDict reports an ID-keyed part met by a reader without the store
// dictionary.
var errNoPartDict = errors.New("ID-keyed part needs the store name dictionary")

// partDict returns the dictionary ID-keyed parts resolve through, or nil.
func (c *PolicyCatalog) partDict() *v2dict.Dict {
	if c == nil {
		return nil
	}
	return c.dict.Load()
}

// partKeyTail is the length of an encoded key after the metric identity:
// archive, slot and (GCP3) ordinal.
func partKeyTail(byID bool) int {
	if byID {
		return partsformat.IDKeyTail
	}
	return partsformat.NameKeyTail
}

// scopePrefix is the partition prefix of either key generation.
func scopePrefix(group v2part.Group, vshard uint16, partition int64, byID bool) ([]byte, error) {
	if byID {
		return partsformat.IDPartitionPrefixForGroup(group, vshard, partition)
	}
	return v2part.PartitionPrefixForGroup(group, vshard, partition)
}

// descriptorByID reports whether d describes an ID-keyed part.
func descriptorByID(d v2part.Descriptor) bool { return d.DictGeneration != 0 }

// metricKey sets the metric of scope key k (Group, VShard, Partition, Part
// and ByID set) to name: the name for a GCP3 part, the name's dictionary ID
// for a GCI2 part. ok is false when the dictionary has no live ID for the
// name, so no GCI2 part holds it.
func metricKey(k v2part.Key, name v2part.ID, dict *v2dict.Dict) (v2part.Key, bool, error) {
	if !k.ByID {
		k.Metric, k.Local = name, 0
		return k, true, nil
	}
	if dict == nil {
		return k, false, errNoPartDict
	}
	e, ok := dict.Lookup(string(k.Group), k.VShard, []byte(name))
	if !ok || e.Deleted() {
		return k, false, nil
	}
	k.Metric, k.Local = "", e.ID
	return k, true, nil
}

// keyName returns the name of decoded key k: k.Metric for a GCP3 key, the
// dictionary name of k.Local for a GCI2 key. An unknown or deleted ID is an
// error: a published part only holds IDs synced before it.
func keyName(k v2part.Key, dict *v2dict.Dict, dst []byte) (v2part.ID, error) {
	if !k.ByID {
		return k.Metric, nil
	}
	if dict == nil {
		return "", errNoPartDict
	}
	name, e, ok := dict.LookupID(string(k.Group), k.VShard, k.Local, dst[:0])
	if !ok || e.Deleted() {
		return "", errUnknownPartID
	}
	return v2part.ID(name), nil
}

var errUnknownPartID = errors.New("ID-keyed part holds an unknown or deleted dictionary ID")

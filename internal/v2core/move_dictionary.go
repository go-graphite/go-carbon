package v2core

import (
	"cmp"
	"errors"
	"slices"

	"github.com/go-graphite/go-carbon/internal/v2dict"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// MoveDictionary is the donor's durable name range accompanying an immutable
// ID-keyed part. Copied IDs stay in their source namespace, including when the
// receiver already owns the vshard and has assigned the same names other IDs.
// The range is bound atomically with the copied part, so replay never depends
// on the sender or on the receiver's unrelated dictionary generation.
type MoveDictionary struct {
	Generation uint64
	Names      []MoveDictionaryName
}

type MoveDictionaryName struct {
	VShard uint16
	Name   string
	Entry  v2dict.Entry
}

func compareMoveDictionaryName(a, b MoveDictionaryName) int {
	if c := cmp.Compare(a.VShard, b.VShard); c != 0 {
		return c
	}
	return cmp.Compare(a.Entry.ID, b.Entry.ID)
}

// ExportMoveDictionary captures the part's vshards with their original IDs.
// Generation is checked on the donor; generations from two stores never compare.
func ExportMoveDictionary(d v2part.Descriptor, dict *v2dict.Dict) (*MoveDictionary, error) {
	if !descriptorByID(d) {
		return nil, nil
	}
	if dict == nil {
		return nil, errNoPartDict
	}
	out := &MoveDictionary{Generation: dict.Generation()}
	if out.Generation < d.DictGeneration {
		return nil, errors.New("donor dictionary precedes part")
	}
	for _, shard := range d.VShards {
		for name, entry := range dict.Entries(string(d.Scope.Group), shard) {
			out.Names = append(out.Names, MoveDictionaryName{shard, string(name), entry})
		}
	}
	slices.SortFunc(out.Names, compareMoveDictionaryName)
	return out, out.validate(d)
}

func (d *MoveDictionary) validate(part v2part.Descriptor) error {
	if !descriptorByID(part) {
		if d != nil {
			return errors.New("name-keyed part has a move dictionary")
		}
		return nil
	}
	if d == nil || d.Generation < part.DictGeneration || len(d.Names) == 0 {
		return errors.New("missing or stale donor dictionary range")
	}
	for i, n := range d.Names {
		if n.Name == "" || n.Entry.ID == v2dict.NoID || v2part.Route(v2part.ID(n.Name)) != n.VShard || (i > 0 && compareMoveDictionaryName(d.Names[i-1], n) >= 0) {
			return errors.New("invalid donor dictionary range")
		}
	}
	return nil
}

func (d *MoveDictionary) resolve(k *v2part.Key) error {
	if !k.ByID {
		return nil
	}
	if d == nil {
		return errNoPartDict
	}
	i, ok := slices.BinarySearchFunc(d.Names, MoveDictionaryName{VShard: k.VShard, Entry: v2dict.Entry{ID: k.Local}}, compareMoveDictionaryName)
	if !ok || d.Names[i].Entry.Deleted() {
		return errUnknownPartID
	}
	k.Metric = v2part.ID(d.Names[i].Name)
	return nil
}

package v2part

import (
	"cmp"
	"context"
	"errors"
	"slices"
)

// RekeyDense rewrites the ID-keyed dense part at src (descriptor d) as a new
// ID-keyed part at dst under spec and the target dictionary's generation,
// mapping every record's ID through remap:
// the streaming rewrite that copies a replica's parts into a live owner,
// whose dictionary assigned the same names other IDs (v2dict Rekey). Records
// whose ID remap rejects (deleted or unknown names) are dropped. Values are
// copied byte for byte. Records re-sort by their new IDs one vshard at a
// time, so only one vshard's records are buffered, and the output equals the
// part a seal of the same records under the new IDs writes.
func RekeyDense(ctx context.Context, src string, d Descriptor, dst string, spec Spec, generation uint64, remap func(vshard uint16, id uint32) (uint32, bool)) (Descriptor, error) {
	if d.DictGeneration == 0 {
		return Descriptor{}, errors.New("rekey needs an ID-keyed part")
	}
	w, err := NewDenseWriter(dst, spec)
	if err != nil {
		return Descriptor{}, err
	}
	defer w.Abort()
	if err = w.IDKeyed(generation); err != nil {
		return Descriptor{}, err
	}
	type rec struct {
		id         uint32
		key        Key
		start, end int
	}
	var recs []rec
	var arena, key []byte
	var shards []uint16
	vshard, have := uint16(0), false
	flush := func() error {
		// Stable: an ID's runs keep their archive/slot order, envelope last.
		slices.SortStableFunc(recs, func(a, b rec) int { return cmp.Compare(a.id, b.id) })
		for _, r := range recs {
			k := r.key
			k.Group, k.Partition, k.Part, k.Local = spec.Scope.Group, spec.Scope.Partition, spec.Scope.Part, r.id
			var e error
			if key, e = Encode(k); e != nil {
				return e
			}
			if k.Archive == MetadataArchive {
				e = w.SetEncoded(key, arena[r.start:r.end])
			} else {
				e = w.SetSlotsEncoded(key, arena[r.start:r.end])
			}
			if e != nil {
				return e
			}
		}
		if len(recs) > 0 {
			shards = append(shards, vshard)
		}
		recs, arena = recs[:0], arena[:0]
		return nil
	}
	err = Read(ctx, src, &d, func(k Key, v []byte) error {
		if have && k.VShard != vshard {
			if e := flush(); e != nil {
				return e
			}
		}
		vshard, have = k.VShard, true
		id, ok := remap(k.VShard, k.Local)
		if !ok {
			return nil
		}
		start := len(arena)
		arena = append(arena, v...)
		recs = append(recs, rec{id: id, key: k, start: start, end: len(arena)})
		return nil
	})
	if err == nil {
		err = flush()
	}
	if err != nil {
		return Descriptor{}, err
	}
	out, err := w.Close()
	if err != nil {
		return Descriptor{}, err
	}
	if len(d.VShards) > 0 {
		out.VShards = shards
	}
	return out, out.Validate()
}

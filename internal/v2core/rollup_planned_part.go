package v2core

import (
	"bytes"

	"github.com/go-graphite/go-carbon/internal/v2part"
)

// The owner planner already derived authoritative coverage and immutable schema
// definitions. The shared writer still validates ordering/scope and owns packing.
func writePlannedRollupPart(path string, file []coreKV, spec v2part.Spec) (v2part.Descriptor, error) {
	w, err := v2part.NewDenseWriter(path, spec)
	if err != nil {
		return v2part.Descriptor{}, err
	}
	defer w.Abort()
	// Size-tier merges rewrite fresh seal parts at the rest level.
	w.SealCompression()
	var shards []uint16
	var prefix []byte
	for _, kv := range file {
		// Keys are sorted, so each routed shard occurs once. Decode only at
		// shard boundaries; archive records need no extra name/route hashing.
		if len(shards) == 0 || !bytes.HasPrefix(kv.Key, prefix) {
			key, err := v2part.Decode(kv.Key)
			if err != nil {
				return v2part.Descriptor{}, err
			}
			prefix, err = v2part.PartitionPrefixForGroup(key.Group, key.VShard, key.Partition)
			if err != nil {
				return v2part.Descriptor{}, err
			}
			shards = append(shards, key.VShard)
		}
		w.NameHint(kv.Name)
		if kv.Record != nil {
			err = w.SetRecordEncoded(kv.Key, *kv.Record)
		} else {
			err = w.SetEncoded(kv.Key, kv.Value)
		}
		if err != nil {
			return v2part.Descriptor{}, err
		}
	}
	d, err := w.Close()
	if err != nil {
		return v2part.Descriptor{}, err
	}
	d.VShards = shards
	return d, d.Validate()
}

// writePlannedRollupTuples writes the planner's tuples straight into the shared
// dense writer: each key is formatted once, only where a run or envelope
// starts, and the writer validates order and scope using the planner's route. The part
// bytes and descriptor equal writePlannedRollupPart over the encoded keys.
func writePlannedRollupTuples(path string, part plannedRollupPart) (v2part.Descriptor, error) {
	w, err := v2part.NewDenseWriter(path, part.Spec)
	if err != nil {
		return v2part.Descriptor{}, err
	}
	defer w.Abort()
	// Size-tier merges rewrite fresh seal parts at the rest level.
	w.SealCompression()
	if part.Generation != 0 {
		if err = w.IDKeyed(part.Generation); err != nil {
			return v2part.Descriptor{}, err
		}
	}
	var shards []uint16
	for _, segment := range part.Segments {
		for i, kv := range segment {
			if len(shards) == 0 || shards[len(shards)-1] != kv.vshard {
				shards = append(shards, kv.vshard)
			}
			// Ranks order tuples within a segment; each segment's first tuple
			// is checked against the previous segment by name.
			rank := kv.rank
			if part.Generation != 0 {
				// An ID-keyed tuple's rank is its dictionary ID.
				if kv.record != nil {
					err = w.SetPlannedIDRecord(kv.vshard, rank, kv.record)
				} else {
					err = w.SetPlannedIDMetadata(kv.vshard, rank, kv.value)
				}
				if err != nil {
					return v2part.Descriptor{}, err
				}
				continue
			}
			if i == 0 {
				rank = 0
			}
			if kv.record != nil {
				err = w.SetPlannedRecord(kv.vshard, rank, kv.name, kv.record)
			} else {
				err = w.SetPlannedMetadata(kv.vshard, rank, kv.name, kv.value)
			}
			if err != nil {
				return v2part.Descriptor{}, err
			}
		}
	}
	d, err := w.Close()
	if err != nil {
		return v2part.Descriptor{}, err
	}
	d.VShards = shards
	return d, d.Validate()
}

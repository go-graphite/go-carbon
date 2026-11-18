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

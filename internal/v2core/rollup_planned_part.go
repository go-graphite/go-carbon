package v2core

import "github.com/go-graphite/go-carbon/internal/v2part"

// The owner planner already derived authoritative coverage and immutable schema
// definitions. The shared writer still validates ordering/scope and owns packing.
func writePlannedRollupPart(path string, file []coreKV, spec v2part.Spec) (v2part.Descriptor, error) {
	w, err := v2part.NewDenseWriter(path, spec)
	if err != nil {
		return v2part.Descriptor{}, err
	}
	defer w.Abort()
	for _, kv := range file {
		if err = w.SetEncoded(kv.Key, kv.Value); err != nil {
			return v2part.Descriptor{}, err
		}
	}
	return w.Close()
}

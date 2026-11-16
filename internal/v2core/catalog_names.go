package v2core

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

// ScanCatalogNames visits names from L3 metadata in one pinned core snapshot.
// It never decodes archive values or acquires owner locks. Each name may be
// visited once per immutable part: the discovery projection deduplicates them.
// Lifecycle tombstones are owned by the caller and must be applied from the
// SAME pinned snapshot before publishing a live host-wide name generation.
// Numeric/L1/L2 data have no canonical core catalog and are not discovered here.
func ScanCatalogNames(ctx context.Context, snapshot *pebble.Snapshot, visit func(string) error) error {
	prefix := []byte{'G', 'C', 'P', 3}
	it, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: partsformat.PrefixEnd(prefix)})
	if err != nil {
		return err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key, err := partsformat.Decode(it.Key())
		if err != nil {
			return err
		}
		if key.Archive != rollupMetadataArchive {
			continue
		}
		var meta RollupMetadata
		if err := json.Unmarshal(it.Value(), &meta); err != nil {
			return err
		}
		var policy RollupPolicy
		if err := json.Unmarshal(meta.Policy, &policy); err != nil {
			return err
		}
		if key.Partition != 0 || key.Ordinal != 0 || key.BlockStart != 0 || meta.Version != 1 || meta.ID == 0 || meta.Frontier.Order == 0 || policy.Name != key.Metric || policy.Group != key.Group {
			return errors.New("v2 index: invalid core catalog metadata")
		}
		if err := visit(string(policy.Name)); err != nil {
			return err
		}
	}
	return it.Error()
}

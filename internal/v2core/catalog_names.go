package v2core

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/v2dict"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// ScanCatalogNames visits names from L3 metadata in one pinned core snapshot.
// It never decodes archive values or acquires owner locks. Each name may be
// visited once per immutable part: the discovery projection deduplicates them.
// Lifecycle tombstones are owned by the caller and must be applied from the
// SAME pinned snapshot before publishing a live host-wide name generation.
// Numeric/L1/L2 data have no canonical core catalog and are not discovered here.
func ScanCatalogNames(ctx context.Context, snapshot *pebble.Snapshot, visit func(string) error) error {
	return ScanCatalogNamesWithDict(ctx, snapshot, nil, visit)
}

// ScanCatalogNamesWithDict is ScanCatalogNames for a store whose ID-keyed
// parts resolve names through dict. A nil dict fails on an ID-keyed part.
func ScanCatalogNamesWithDict(ctx context.Context, snapshot *pebble.Snapshot, dict *v2dict.Dict, visit func(string) error) error {
	// Both key generations: GCI2 sorts just before GCP3, nothing between.
	it, err := newArchiveIter(ctx, snapshot, &pebble.IterOptions{LowerBound: idKeyMagic, UpperBound: partsformat.PrefixEnd(partKeyMagic)})
	if err != nil {
		return err
	}
	defer it.Close()
	names := keyNames{dict: dict}
	for it.First(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var key partsformat.Key
		if partsformat.IsIDKey(it.Key()) {
			key, err = v2part.DecodeKey(it.Key())
			if err == nil && key.Archive == rollupMetadataArchive {
				err = names.resolve(&key)
			}
		} else {
			key, err = partsformat.Decode(it.Key())
		}
		if err != nil {
			return err
		}
		if key.Archive != rollupMetadataArchive {
			continue
		}
		meta, err := DecodeRollupMetadata(key.Metric, key.Group, it.Value())
		if err != nil {
			return err
		}
		var policy RollupPolicy
		if err := json.Unmarshal(meta.Policy, &policy); err != nil {
			return err
		}
		if key.Ordinal != 0 || key.BlockStart != 0 || (meta.Version != 1 && meta.Version != 2 && meta.Version != 3) || meta.ID == 0 || meta.Frontier.Order == 0 || policy.Name != key.Metric || policy.Group != key.Group {
			return errors.New("v2 index: invalid core catalog metadata")
		}
		if err := visit(string(policy.Name)); err != nil {
			return err
		}
	}
	return it.Error()
}

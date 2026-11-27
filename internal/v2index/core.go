package v2index

import (
	"context"
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

// PublishCore recovers/rebuilds this projection from a caller-pinned vector of
// all hosted core owners. The control owner guarantees a coherent vector and
// supplies lifecycle visibility (including expiry) from that same generation.
// A nil live predicate is allowed only when lifecycle deletion is disabled.
// Failure retains the preceding view. The caller owns and closes snapshots.
func (c *Catalog) PublishCore(ctx context.Context, revision uint64, snapshots []*pebble.Snapshot, live func(string) bool) error {
	var names []string
	for _, snapshot := range snapshots {
		if snapshot == nil {
			return errors.New("v2 index: nil core snapshot")
		}
		if err := v2core.ScanCatalogNames(ctx, snapshot, func(name string) error {
			if live == nil || live(name) {
				names = append(names, name)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.Publish(revision, names)
}

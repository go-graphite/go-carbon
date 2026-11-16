package v2convert

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// NativeCatalogReader owns a coherent checkpoint for the entire catalog walk.
type NativeCatalogReader interface {
	NativeSnapshotReader
	ListCatalogPage(context.Context, string, string, int) ([]chunkstore.Metadata, error)
}

// ConvertNativeCatalog converts every catalog name, using bounded transactions
// and the same shared writer/readback as ConvertNativeCheckpoint. The caller
// opens and verifies the source once. A complete return certifies catalog
// coverage; individual receipts alone certify only their selected names.
func ConvertNativeCatalog(ctx context.Context, source NativeCatalogReader, plan NativeCheckpointPlan, output, prefix string, pageSize, maxBytes, workers int, completed func(NativeReceipt) error) error {
	if source == nil || output == "" || plan.Version != 1 || plan.Now <= 0 || len(plan.Names) != 0 || pageSize < 1 || pageSize > 10000 || maxBytes <= 0 || workers < 1 || workers > 64 {
		return errors.New("invalid whole-catalog conversion options")
	}
	if err := plan.Baseline.Validate(); err != nil {
		return err
	}
	digest, err := plan.InventoryDigest()
	if err != nil || digest != plan.Baseline.Snapshot {
		return errors.New("native checkpoint inventory authority mismatch")
	}
	var inputBytes int64
	for _, f := range plan.Files {
		inputBytes += f.Bytes
	}
	after := ""
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := source.ListCatalogPage(ctx, prefix, after, pageSize)
		if err != nil {
			return err
		}
		if len(page) > pageSize {
			return errors.New("oversized native catalog page")
		}
		if len(page) == 0 {
			return nil
		}
		names := make([]string, len(page))
		previous := after
		for i, m := range page {
			if m.Name <= previous {
				return fmt.Errorf("native catalog failed to advance after %q", previous)
			}
			names[i], previous = m.Name, m.Name
		}
		batch := plan
		batch.Names = names
		// Each bounded transaction is a distinct new physical part. Derive its
		// ID from the certified baseline and selection so retries are stable,
		// while disjoint pages cannot collide in a whole-part inventory.
		identity, err := json.Marshal(struct {
			Baseline NativeBaseline
			Names    []string
		}{plan.Baseline, names})
		if err != nil {
			return err
		}
		part := sha256.Sum256(append([]byte("native-catalog-part-v1\x00"), identity...))
		copy(batch.Baseline.Part[:], part[:16])
		receipt, err := convertNativeCheckpoint(ctx, source, batch, output, maxBytes, workers, digest, inputBytes)
		if err != nil {
			return err
		}
		if completed != nil {
			if err := completed(receipt); err != nil {
				return err
			}
		}
		after = previous
	}
}

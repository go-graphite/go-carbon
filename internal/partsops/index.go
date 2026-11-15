package partsops

import (
	"github.com/go-graphite/go-carbon/carbonserver"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// BuildIndex builds on an immutable image. For host-wide quotas, s.Metrics MUST
// contain the coherent union of all hosted owners, not one vshard's subset;
// independent per-owner indexes would multiply the configured limits.
// The caller must retain a long-lived
// admission index across seals to preserve consumed throughput, and publish a
// refreshed find index only with the corresponding catalog generation. Building
// a fresh admission index on every seal would reset quotas and is forbidden.
func BuildIndex(s Snapshot, opts carbonserver.PartsIndexOptions) (*carbonserver.PartsCatalogIndex, error) {
	metadata := make([]store.Metadata, 0, len(s.Metrics))
	for _, m := range s.Metrics {
		metadata = append(metadata, m.Metadata)
	}
	return carbonserver.NewPartsCatalogIndex(metadata, opts)
}

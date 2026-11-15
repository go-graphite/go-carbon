package carbonserver

import (
	"context"
	"sync/atomic"

	"go.uber.org/zap"
)

// prepareCatalogCheckpoint runs after index updates stop. Reconcile only the
// bounded committed delta since the last complete catalog publication; an
// overflow, deletion, or changed immutable policy leaves restart uncertified.
// The store sequence comparison also rejects a concurrent admin/store write.
func (l *CarbonserverListener) prepareCatalogCheckpoint(ti *trieIndex) string {
	db := l.getMetricIndexStore()
	if db == nil || l.metricStoreIndex != db {
		return ""
	}
	token := db.CatalogCheckpointToken()
	if token == "" {
		return ""
	}
	changes, _, reset, err := db.CatalogChanges(context.Background(), l.metricStoreCursor)
	if err != nil || reset {
		return ""
	}
	for _, m := range changes {
		logical, points := metricStoreLogicalSize(m)
		entry, found, err := ti.snapshot.lookup(metricStoreVirtualPath(m.Name))
		if err != nil {
			return ""
		}
		if found {
			if entry.LogicalSize != logical || entry.DataPoints != points || entry.PhysicalSize != 0 {
				return ""
			}
			continue
		}
		if _, err := ti.insert(metricStoreVirtualPath(m.Name), logical, 0, points, 0); err != nil {
			return ""
		}
	}
	if token != db.CatalogCheckpointToken() {
		return ""
	}
	return token
}

// A validated pending checkpoint still owns its names and points. A matching
// store fence additionally proves that its saved catalog is current, so the
// new process can start at the journal's open cursor instead of rescanning all
// names. The journal is armed before Store.Open performs recovery writes.
// Called with the discovery writer lock held, after successful publication.
func (l *CarbonserverListener) reuseCatalogCheckpoint(ti *trieIndex) {
	db := l.getMetricIndexStore()
	if db == nil {
		return
	}
	if ti.catalogCheckpoint == "" || ti.catalogCheckpoint != db.OpenCatalogCheckpointToken() {
		reason := "store identity or sequence differs"
		if ti.catalogCheckpoint == "" {
			reason = "uncertified saved generation"
		}
		l.logger.Info("shared metric-store saved catalog needs reconciliation", zap.String("reason", reason))
		return
	}
	l.metricStoreIndex, l.metricStoreCursor = db, 1
	atomic.StoreUint64(&l.metrics.MetricsKnown, ti.snapshot.manifest.Records+uint64(ti.getFileCount()))
	l.logger.Info("shared metric-store saved catalog reused",
		zap.Uint64("mapped_metrics", ti.snapshot.manifest.Records),
		zap.Int64("overlay_metrics", ti.getFileCount()),
		zap.Uint64("catalog_cursor", l.metricStoreCursor))
}

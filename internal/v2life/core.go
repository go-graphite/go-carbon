package v2life

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

// CoreCatalog stages catalog publication on the existing core seal transaction.
// One serial owner calls Plan; only its returned Commit may install the image.
// It owns no database, writer lock or second ingestion queue. This checkpoint
// adapter copies metadata per seal and is not a large-catalog scaling claim.
type CoreCatalog struct{ owner *Owner }

type stagedCommitter struct{}

func (stagedCommitter) Commit(context.Context, Batch) error { return nil }

// Activity fields in store.Metadata are intentionally excluded from its JSON.
// Keep the earlier operational catalog's explicit activity envelope on disk.
type coreImage struct {
	Version  uint8
	Snapshot Snapshot
	Metrics  []persistentMetric
}

func coreCatalogKey(scope Scope) []byte { return metadataKey(scope, 'c', "") }

func NewCoreCatalog(image Snapshot) (*CoreCatalog, error) {
	owner, err := NewOwner(image, stagedCommitter{})
	if err != nil {
		return nil, err
	}
	return &CoreCatalog{owner: owner}, nil
}
func (c *CoreCatalog) Snapshot() Snapshot { return c.owner.Snapshot() }

// Plan stages activity and then expiration in one seal. upserts must describe
// original accepted applies, using a portable incarnation and exact admission
// clock; callers must not derive activity from maximum sample timestamps.
// pending must include admitted but unsealed input and unfinished replay.
// Neither failed planning nor Abort changes the visible catalog. Plan must run
// in the same serial owner as core's other seal planners. A caller composing
// plans must preserve all Commit/Abort callbacks and sorted, disjoint file ranges.
// Whole-part Reclaim uses the standalone OpenCatalog adapter instead (never
// on a core database, even when stopped): the
// current core extension API has no range-deletion primitive, and its rollup
// partition zero must never be treated as an expired time partition.
func (c *CoreCatalog) Plan(upserts []Metric, policy *ExpirationPolicy, now time.Time, pending func(string) bool) (v2core.SealPlan, ExpirationPlan, error) {
	staged, err := NewOwner(c.Snapshot(), stagedCommitter{})
	if err != nil {
		return v2core.SealPlan{}, ExpirationPlan{}, err
	}
	if len(upserts) > 0 {
		if err = staged.Apply(context.Background(), upserts, nil); err != nil {
			return v2core.SealPlan{}, ExpirationPlan{}, err
		}
	}
	var expiry ExpirationPlan
	if policy != nil {
		expiry, err = staged.Expire(context.Background(), *policy, now, pending)
		if err != nil {
			return v2core.SealPlan{}, expiry, err
		}
	}
	image := staged.Snapshot()
	if image.Revision == c.Snapshot().Revision {
		return v2core.SealPlan{}, expiry, nil
	}
	disk := coreImage{Version: 1, Snapshot: image}
	disk.Snapshot.Metrics = nil
	for _, m := range image.Metrics {
		disk.Metrics = append(disk.Metrics, persistentMetric{Metric: m, Revision: m.Metadata.Revision, LastUpdate: m.Metadata.LastUpdate, Continuation: m.Metadata.Continuation, FileMigration: m.Metadata.FileMigration})
	}
	data, err := json.Marshal(disk)
	if err != nil {
		return v2core.SealPlan{}, expiry, err
	}
	// Only the scalar checkpoint is replaced. All data, tombstones and frontier
	// become visible in the same core snapshot; no point is read or rewritten.
	plan := v2core.SealPlan{Files: [][]v2core.KV{{{Key: coreCatalogKey(image.Scope), Value: data}}}, Commit: func() { c.owner.view.Store(staged.view.Load()) }}
	return plan, expiry, nil
}

// ReadCoreCatalog reads exactly one catalog checkpoint from a pinned core
// snapshot, never from a separately sampled live database. An absent checkpoint
// means an empty catalog; expiration may start only after recovery completes.
func ReadCoreCatalog(snapshot *pebble.Snapshot, scope Scope) (Snapshot, error) {
	if err := validateScope(scope); err != nil {
		return Snapshot{}, err
	}
	value, closer, err := snapshot.Get(coreCatalogKey(scope))
	if errors.Is(err, pebble.ErrNotFound) {
		return Snapshot{Scope: scope}, nil
	}
	if err != nil {
		return Snapshot{}, err
	}
	defer closer.Close()
	var disk coreImage
	if err = json.Unmarshal(value, &disk); err != nil {
		return Snapshot{}, err
	}
	if disk.Version != 1 || disk.Snapshot.Scope != scope || len(disk.Snapshot.Metrics) != 0 {
		return Snapshot{}, errors.New("invalid lifecycle checkpoint")
	}
	for _, m := range disk.Metrics {
		m.Metric.Metadata.Revision = m.Revision
		m.Metric.Metadata.LastUpdate = m.LastUpdate
		m.Metric.Metadata.Continuation = m.Continuation
		m.Metric.Metadata.FileMigration = m.FileMigration
		disk.Snapshot.Metrics = append(disk.Snapshot.Metrics, m.Metric)
	}
	validated, err := NewCoreCatalog(disk.Snapshot)
	if err != nil {
		return Snapshot{}, err
	}
	return validated.Snapshot(), nil
}

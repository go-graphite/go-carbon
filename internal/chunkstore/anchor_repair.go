package chunkstore

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/cockroachdb/pebble"
)

// AnchorArchive records physical classic-ring consistency, independently of
// Fetch (which cannot diagnose an empty anchor) and logical snapshot export.
type AnchorArchive struct {
	Index     int    `json:"index"`
	Points    int    `json:"points"`
	Misplaced int    `json:"misplaced"`
	Changed   bool   `json:"changed"`
	Blocked   string `json:"blocked,omitempty"`
}
type AnchorRepairPlan struct {
	Archives []AnchorArchive `json:"archives"`
	Writes   []RelayoutKV    `json:"-"`
	Deletes  [][]byte        `json:"-"`
}
type ContinuationAnchorRepairer interface {
	PlanArchiveAnchors(context.Context, ContinuationIO) (AnchorRepairPlan, error)
}
type AnchorRepairResult struct {
	Name             string          `json:"name"`
	Version          TransferVersion `json:"version"`
	Archives         []AnchorArchive `json:"archives"`
	Applied          bool            `json:"applied"`
	PreparationNanos int64           `json:"preparation_ns"`
	LockNanos        int64           `json:"lock_ns"`
	Retries          int             `json:"retries"`
}

// RepairArchiveAnchors prepares from immutable committed bytes outside writer
// stripes. Publication validates the original revision under the ordinary
// transfer lock; a racing foreground update causes a fresh preparation. Cache
// input remains with the sole live daemon and is never flushed or replayed here.
func (s *Store) RepairArchiveAnchors(ctx context.Context, name string, apply bool) (AnchorRepairResult, error) {
	codec, ok := s.continuation.(ContinuationAnchorRepairer)
	if !ok {
		return AnchorRepairResult{}, ErrFormat
	}
	for attempt := 0; attempt < 4; attempt++ {
		preparedAt := time.Now()
		records, err := s.ExportTransfer(ctx, []string{name})
		if err != nil {
			return AnchorRepairResult{}, err
		}
		r := records[0]
		result := AnchorRepairResult{Name: name, Version: r.Version, Retries: attempt}
		if !r.Continuation {
			return result, nil
		}
		plan, err := codec.PlanArchiveAnchors(ctx, transferRecordIO(r))
		result.Archives = plan.Archives
		result.PreparationNanos = time.Since(preparedAt).Nanoseconds()
		if err != nil || !apply || len(plan.Writes)+len(plan.Deletes) == 0 {
			return result, err
		}
		for _, a := range plan.Archives {
			if a.Blocked != "" {
				return result, errors.New("anchor repair requires source-aware reconciliation: " + a.Blocked)
			}
		}
		applied, retry, err := s.publishArchiveAnchors(ctx, r, plan, &result)
		if retry {
			continue
		}
		result.Applied = applied
		return result, err
	}
	return AnchorRepairResult{}, ErrConflict
}

func (s *Store) publishArchiveAnchors(ctx context.Context, r TransferMetric, plan AnchorRepairPlan, result *AnchorRepairResult) (bool, bool, error) {
	// Encoding and batch allocation also stay outside the writer stripe.
	b := s.db.NewIndexedBatch()
	defer b.Close()
	identity := Metadata{MetricConfig: r.Config, ID: r.Version.ID, Generation: r.Version.Generation, Continuation: true}
	kv := s.newContinuationIO(ctx, b, b, identity)
	for _, k := range plan.Deletes {
		if err := kv.Delete(k); err != nil {
			return false, false, err
		}
	}
	for _, p := range plan.Writes {
		if err := kv.Set(p.Key, p.Value); err != nil {
			return false, false, err
		}
	}
	unlock := s.lockTransfer([]string{r.Config.Name}, nil)
	lockedAt := time.Now()
	release := func() { result.LockNanos += time.Since(lockedAt).Nanoseconds(); unlock() }
	m, err := metadataFrom(s.db, r.Config.Name)
	if err == nil && (transferVersion(m) != r.Version || !samePolicy(m.MetricConfig, r.Config) || !m.Continuation) {
		release()
		return false, true, nil
	}
	if err == nil {
		err = s.checkFileOwnership(m)
	}
	if err == nil && m.FileMigration == FileCleanupPending {
		err = ErrConflict
	}
	if err == nil {
		err = ctx.Err()
	}
	if err == nil && m.Revision == math.MaxUint64 {
		err = errors.New("metric revision exhausted")
	}
	if err == nil {
		m.Revision++
		err = b.Set(revisionKey(m), revisionBytes(m), nil)
	}
	if err == nil {
		err = b.Commit(pebble.NoSync)
		if err == nil {
			s.pendingSync.Store(true)
		}
	}
	release()
	if err != nil {
		return false, false, err
	}
	s.invalidateReadCache()
	// Durability waits after releasing writer stripes. A lost acknowledgement
	// may cause a repeated inspection; a completed rotation is then a no-op.
	if err = s.syncWAL(); err != nil {
		return true, false, err
	}
	return true, false, nil
}

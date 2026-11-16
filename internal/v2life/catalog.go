// Package v2life implements owner-local operational metadata for immutable
// parts. It never opens a data part or reads a point before a deletion.
package v2life

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/tags"
)

var ErrConflict = errors.New("catalog revision conflict")

// Scope is stable across physical owner/cluster changes. PlacementGroup is
// assigned by routing configuration, never inferred from the current host.
type Scope struct {
	PlacementGroup partsformat.Group
	VShard         uint16
}

func validateScope(s Scope) error {
	if s.VShard >= partsformat.VirtualShards || s.PlacementGroup == "" || len(s.PlacementGroup) > partsformat.MaxNameBytes {
		return errors.New("invalid placement group/vshard")
	}
	return nil
}

type Metric struct {
	Scope    Scope
	Metadata store.Metadata
}

// Tombstone hides every part (and legacy base) through this incarnation.
// Recreated names must have a strictly larger portable durable Generation;
// this must NOT be a replica-local chunkstore generation. Multi-origin copies
// require an agreed incarnation authority/provenance protocol before use.
// Revision is
// the activity revision checked by the owner, not a data-key range boundary.
type Tombstone struct {
	Scope             Scope
	Name              string
	ThroughGeneration uint64
	Revision          uint64
}

type Snapshot struct {
	Scope             Scope
	Revision          uint64
	Metrics           []Metric
	Tombstones        []Tombstone
	DroppedPartitions []Partition
}

// Batch is one durable metadata publication. Commit must atomically persist
// these edits and the new catalog revision, compare ExpectedRevision, and sync
// before returning. A failed commit must expose none of it. Replay must finish
// before expiration starts. Reads pin the matching catalog/data generation.
// The eventual part engine implements this interface; no data Get is needed.
type Batch struct {
	Scope                      Scope
	ExpectedRevision, Revision uint64
	Upserts                    []Metric
	Deletes                    []Tombstone
	DropPartitions             []Partition
}
type Committer interface {
	Commit(context.Context, Batch) error
}

// Owner has one serial caller, normally the vshard's seal/control queue.
// Snapshot readers do not share a lock with that caller. Metadata copies happen
// per publication, never per data point. This prototype is not a large-catalog
// allocation/scaling claim.
type Owner struct {
	sink Committer
	view atomic.Pointer[Snapshot]
}

func cloneMetric(m Metric) Metric {
	m.Metadata.Retentions = slices.Clone(m.Metadata.Retentions)
	return m
}
func cloneSnapshot(s Snapshot) Snapshot {
	s.Metrics = slices.Clone(s.Metrics)
	for i := range s.Metrics {
		s.Metrics[i] = cloneMetric(s.Metrics[i])
	}
	s.Tombstones = slices.Clone(s.Tombstones)
	s.DroppedPartitions = slices.Clone(s.DroppedPartitions)
	return s
}

// NewOwner restores a committed image, including tombstones which prevent old
// SST copies from resurrecting expired names. There is deliberately no default
// no-op persistence implementation.
func NewOwner(s Snapshot, sink Committer) (*Owner, error) {
	if sink == nil {
		return nil, errors.New("catalog requires a durable committer")
	}
	if err := validateScope(s.Scope); err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	dead := make(map[string]uint64)
	for _, d := range s.Tombstones {
		if d.Scope != s.Scope || d.Name == "" || d.ThroughGeneration == 0 || d.Revision == 0 || dead[d.Name] != 0 || partsformat.Route(partsformat.ID(d.Name)) != s.Scope.VShard {
			return nil, errors.New("invalid catalog tombstone")
		}
		dead[d.Name] = d.ThroughGeneration
	}
	for _, m := range s.Metrics {
		if err := validateMetric(s.Scope, m); err != nil {
			return nil, err
		}
		if seen[m.Metadata.Name] || m.Metadata.Generation <= dead[m.Metadata.Name] {
			return nil, errors.New("duplicate or tombstoned metric")
		}
		seen[m.Metadata.Name] = true
	}
	partitions := make(map[int64]bool)
	for _, p := range s.DroppedPartitions {
		if p.Scope != s.Scope || !p.Sealed || p.ID == "" || p.End <= p.Start || p.LongestSeconds <= 0 || partitions[p.Key] {
			return nil, errors.New("invalid retired partition")
		}
		partitions[p.Key] = true
	}
	o := &Owner{sink: sink}
	s = cloneSnapshot(s)
	o.view.Store(&s)
	return o, nil
}

func validateMetric(scope Scope, m Metric) error {
	n := m.Metadata.Name
	if n == "" || len(n) > partsformat.MaxNameBytes {
		return fmt.Errorf("invalid metric name length %d", len(n))
	}
	if m.Scope != scope || partsformat.Route(partsformat.ID(n)) != scope.VShard {
		return errors.New("metric outside owner scope")
	}
	if m.Metadata.Generation == 0 || m.Metadata.Revision == 0 {
		return errors.New("metric requires durable generation and revision")
	}
	_, err := LongestRetention(m.Metadata.Retentions)
	return err
}

// NormalizeName preserves the existing ingestion choice. With tags disabled
// the name is opaque; with tags enabled today's normalizer is the only one.
func NormalizeName(name string, enabled bool) (string, error) {
	if enabled {
		return tags.Normalize(name)
	}
	return name, nil
}

func (o *Owner) Snapshot() Snapshot { return cloneSnapshot(*o.view.Load()) }

// Apply edits only catalog metadata. All conflict validation precedes Commit;
// deletion never scans part keys. A stale expiration observation cannot remove
// a metric whose sealed activity revision has advanced.
func (o *Owner) Apply(ctx context.Context, upserts []Metric, deletes []Tombstone) error {
	return o.apply(ctx, upserts, deletes, nil)
}

func (o *Owner) apply(ctx context.Context, upserts []Metric, deletes []Tombstone, drops []Partition) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	old := o.view.Load()
	if old.Revision == ^uint64(0) {
		return errors.New("catalog revision exhausted")
	}
	metrics := make(map[string]Metric, len(old.Metrics)+len(upserts))
	dead := make(map[string]Tombstone, len(old.Tombstones)+len(deletes))
	for _, m := range old.Metrics {
		metrics[m.Metadata.Name] = m
	}
	for _, d := range old.Tombstones {
		dead[d.Name] = d
	}
	seen := make(map[string]bool)
	for _, m := range upserts {
		if err := validateMetric(old.Scope, m); err != nil {
			return err
		}
		n := m.Metadata.Name
		if seen[n] {
			return errors.New("duplicate batch metric")
		}
		seen[n] = true
		previous, exists := metrics[n]
		if m.Metadata.Generation <= dead[n].ThroughGeneration || (exists && (m.Metadata.Generation != previous.Metadata.Generation || m.Metadata.Revision <= previous.Metadata.Revision)) {
			return ErrConflict
		}
		metrics[n] = cloneMetric(m)
	}
	for _, d := range deletes {
		m, exists := metrics[d.Name]
		if seen[d.Name] || d.Scope != old.Scope || !exists || m.Metadata.Generation != d.ThroughGeneration || m.Metadata.Revision != d.Revision {
			return ErrConflict
		}
		seen[d.Name] = true
		delete(metrics, d.Name)
		dead[d.Name] = d
	}
	batch := Batch{Scope: old.Scope, ExpectedRevision: old.Revision, Revision: old.Revision + 1, Deletes: slices.Clone(deletes)}
	for _, p := range drops {
		if p.Scope != old.Scope {
			return errors.New("partition outside owner scope")
		}
	}
	batch.DropPartitions = slices.Clone(drops)
	for _, m := range upserts {
		batch.Upserts = append(batch.Upserts, cloneMetric(m))
	}
	next := Snapshot{Scope: old.Scope, Revision: batch.Revision}
	next.DroppedPartitions = append(slices.Clone(old.DroppedPartitions), drops...)
	for _, m := range metrics {
		next.Metrics = append(next.Metrics, m)
	}
	for _, d := range dead {
		next.Tombstones = append(next.Tombstones, d)
	}
	slices.SortFunc(next.Metrics, func(a, b Metric) int { return strings.Compare(a.Metadata.Name, b.Metadata.Name) })
	slices.SortFunc(next.Tombstones, func(a, b Tombstone) int { return strings.Compare(a.Name, b.Name) })
	if err := o.sink.Commit(ctx, batch); err != nil {
		return err
	}
	o.view.Store(&next)
	return nil
}

// Visible applies to every archive, newest part and legacy base alike. An
// occupied NaN is data and must never be interpreted as this tombstone.
func Visible(generation uint64, tombstone Tombstone) bool {
	return tombstone.ThroughGeneration == 0 || generation > tombstone.ThroughGeneration
}

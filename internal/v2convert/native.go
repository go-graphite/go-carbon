package v2convert

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// NativeBaseline is an explicit new, fenced snapshot ordering authority. Native
// archives do not retain the original per-point admission order; Order is the
// caller-certified baseline order, never a reconstructed application order.
// Snapshot binds the coherent checkpoint and catch-up frontier owned by the
// caller. Every later application must be ordered strictly after Order.
// Group/Part describe the baseline's new physical identity, permitting the
// resolved slots of different native source tables to share one dense part.
// As in a live seal, records go to the UTC month partition of their sample time
// (v2core.RollupMonthPartition), each partition carrying the metric envelope.
type NativeBaseline struct {
	Snapshot v2part.Digest
	Source   v2part.Provenance
	Group    v2part.Group
	Part     v2part.PartID
	Order    uint64
}

func (b NativeBaseline) Validate() error {
	if b.Snapshot == (v2part.Digest{}) || b.Source.Incarnation == (v2part.PartID{}) || b.Source.Stream == "" || b.Part == (v2part.PartID{}) || b.Order == 0 {
		return errors.New("native baseline requires snapshot authority, source, part and order")
	}
	_, _, err := v2part.GroupRange(b.Group)
	return err
}

// NativeRecords materializes one resolved metric snapshot into the exact logical
// input accepted by CoreWriter. It performs no archive reduction: existing
// coarse values, occupied NaNs, signed zero and every stored timestamp survive.
// A Snapshot must come from a coherent exclusive checkpoint reader, not loose
// SST scans. The baseline changes ordering identity deliberately; use CoreRecords
// when original GCP3 orders and correction flags are available and must survive.
func NativeRecords(snapshot chunkstore.Snapshot, baseline NativeBaseline) ([]Record, error) {
	if err := baseline.Validate(); err != nil {
		return nil, err
	}
	m := snapshot.Metadata
	if m.ID == 0 || m.Name == "" || m.Revision == 0 || len(snapshot.Archives) != len(m.Retentions) {
		return nil, errors.New("incomplete native snapshot metadata")
	}
	policy := v2core.RollupPolicy{Name: v2part.ID(m.Name), Group: baseline.Group, Config: sealrollup.Config{Method: sealrollup.Method(m.AggregationMethod), XFilesFactor: m.XFilesFactor}}
	for _, r := range m.Retentions {
		policy.Config.Archives = append(policy.Config.Archives, sealrollup.Archive{Step: int64(r.Step), Count: int64(r.Count)})
	}
	encoded, err := v2core.EncodeRollupPolicy(policy)
	if err != nil {
		return nil, err
	}
	digest, err := sealrollup.PolicyDigest(policy.Config)
	if err != nil {
		return nil, err
	}
	meta := v2core.RollupMetadata{Version: 3, ID: m.ID, Policy: encoded, Revision: m.Revision, LastUpdate: m.LastUpdate.UnixNano(), Frontier: sealrollup.Frontier{Order: baseline.Order, Policy: digest}}
	key := v2part.Key{Group: baseline.Group, VShard: v2part.Route(policy.Name), Part: baseline.Part, Metric: policy.Name}
	var records []Record
	var partitions []int64
	put := func(partition int64, archive uint32, slot int64, value []byte) error {
		key.Partition = partition
		key.Archive, key.BlockStart = archive, slot
		encoded, err := v2part.Encode(key)
		if err == nil {
			records = append(records, Record{Key: encoded, Value: value})
		}
		return err
	}
	for archive, a := range snapshot.Archives {
		if a.Retention != m.Retentions[archive] {
			return nil, fmt.Errorf("native archive %d policy differs", archive)
		}
		ar := policy.Config.Archives[archive]
		occupied := make(map[int64]bool, len(a.Points))
		for _, point := range a.Points {
			if point.Timestamp <= 0 || point.Timestamp > math.MaxUint32 || point.Timestamp%ar.Step != 0 {
				return nil, fmt.Errorf("native archive %d has invalid timestamp", archive)
			}
			slot := (point.Timestamp / ar.Step) % ar.Count
			if occupied[slot] {
				return nil, fmt.Errorf("native archive %d has unresolved ring-slot versions", archive)
			}
			occupied[slot] = true
			meta.Frontier.MaxTime = max(meta.Frontier.MaxTime, point.Timestamp)
			r := sealrollup.Record{Delta: sealrollup.Delta{Archive: archive, Slot: slot, Order: baseline.Order, Sample: sealrollup.Sample{Time: point.Timestamp, Bits: math.Float64bits(point.Value), Sequence: baseline.Order}}}
			partition := v2core.RollupMonthPartition(point.Timestamp)
			if !slices.Contains(partitions, partition) {
				partitions = append(partitions, partition)
			}
			if err := put(partition, uint32(archive), slot, sealrollup.EncodeRecord(r)); err != nil {
				return nil, err
			}
		}
	}
	// As in a live seal, the envelope goes with every partition written and
	// with the month of the newest sample.
	if home := v2core.RollupMonthPartition(meta.Frontier.MaxTime); !slices.Contains(partitions, home) {
		partitions = append(partitions, home)
	}
	encoded = v2core.AppendRollupMetadata(nil, meta)
	for _, partition := range partitions {
		if err := put(partition, v2part.MetadataArchive, 0, encoded); err != nil {
			return nil, err
		}
	}
	slices.SortFunc(records, func(a, b Record) int { return bytes.Compare(a.Key, b.Key) })
	return records, nil
}

// NativeSnapshotReader is implemented by chunkstore.TransferSource. The caller
// must retain its exclusively owned coherent checkpoint for this entire call.
type NativeSnapshotReader interface {
	Snapshot(context.Context, string) (chunkstore.Snapshot, error)
}

// MaterializeNativeWorkers reads an explicit bounded set of names from a coherent
// checkpoint. A fixed baseline produces identical records regardless of source
// SST boundaries or input name order. maxBytes bounds the decoded output; source
// Snapshot also requires its own per-metric bound from the checkpoint owner.
// It keeps one source decoder per worker and sorts the resulting logical
// records once. Parallelism does not affect output bytes.
func MaterializeNativeWorkers(ctx context.Context, source NativeSnapshotReader, names []string, baseline NativeBaseline, maxBytes, workers int) ([]Record, error) {
	if source == nil || maxBytes <= 0 {
		return nil, errors.New("native source and positive memory bound required")
	}
	if err := baseline.Validate(); err != nil {
		return nil, err
	}
	names = slices.Clone(names)
	slices.Sort(names)
	if workers < 1 || workers > 64 {
		return nil, errors.New("native workers must be 1..64")
	}
	for i, name := range names {
		if name == "" || (i > 0 && names[i-1] == name) {
			return nil, errors.New("empty or duplicate native name")
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		records []Record
		err     error
	}
	jobs := make(chan string)
	results := make(chan result)
	var wg sync.WaitGroup
	for range min(workers, len(names)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for name := range jobs {
				snapshot, err := source.Snapshot(ctx, name)
				var rows []Record
				if err == nil && snapshot.Metadata.Name != name {
					err = errors.New("native snapshot name mismatch")
				}
				if err == nil {
					rows, err = NativeRecords(snapshot, baseline)
				}
				select {
				case results <- result{rows, err}:
				case <-ctx.Done():
					return
				}
				if err != nil {
					return
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, name := range names {
			select {
			case jobs <- name:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() { wg.Wait(); close(results) }()
	var records []Record
	used := 0
	var failure error
	for result := range results {
		if failure != nil {
			continue
		}
		if result.err != nil {
			failure = result.err
			cancel()
			continue
		}
		for _, row := range result.records {
			if maxBytes-used < 48 || len(row.Key) > maxBytes-used-48 || len(row.Value) > maxBytes-used-48-len(row.Key) {
				failure = errors.New("native materialization exceeds memory bound")
				cancel()
				break
			}
			used += 48 + len(row.Key) + len(row.Value)
			records = append(records, row)
		}
	}
	if failure != nil {
		return nil, failure
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	slices.SortFunc(records, func(a, b Record) int { return bytes.Compare(a.Key, b.Key) })
	return records, nil
}

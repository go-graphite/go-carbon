package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

type TransferFetchProof struct{ Archives, Windows, Points int }

// VerifyTransferFetch is an offline oracle, outside copy timing. It preserves
// the actual encoded image in a private memory store and drives Store.Fetch,
// including classic ring addressing. A movable diagnostic clock makes every
// retained historical point testable within its own retention window. Queries
// stay at most 256 slots wide, including very long sparse retentions.
func (s *Store) VerifyTransferFetch(ctx context.Context, want Snapshot) (proof TransferFetchProof, err error) {
	records, err := s.ExportTransferLimit(ctx, []string{want.Metadata.Name}, math.MaxInt64)
	if err != nil {
		return proof, err
	}
	if len(records) != 1 || records[0].Version != transferVersion(want.Metadata) {
		return proof, ErrConflict
	}
	var epoch atomic.Int64
	epoch.Store(s.now().Unix())
	options := Options{ExactBlocks: s.exactBlocks, TimeBuckets: s.timeBuckets, Continuation: s.continuation, CacheSize: 1 << 20, MemTableSize: 1 << 20, Now: func() time.Time { return time.Unix(epoch.Load(), 0) }}
	if records[0].Continuation && s.timeBuckets {
		options.ColdPack = ColdPackOptions{Mode: "exact", FloatCodec: true}
	}
	shadow, err := Open("/fetch-oracle", options.WithFS(vfs.NewMem()))
	if err != nil {
		return proof, err
	}
	defer func() { err = errors.Join(err, shadow.Close()) }()
	shadow.StopBackground()
	// An absent-name raw import retains the candidate's physical positioning;
	// it must never repair the very image that this oracle is checking.
	if _, err := shadow.ImportTransfer(ctx, records); err != nil {
		return proof, err
	}
	for ai, archive := range want.Archives {
		proof.Archives++
		r := archive.Retention
		age := min(r.MaxRetention(), 256*r.Step)
		if ai > 0 {
			age = max(age, want.Archives[ai-1].Retention.MaxRetention()+1)
		}
		for cursor := 0; cursor < len(archive.Points); {
			if err := ctx.Err(); err != nil {
				return proof, err
			}
			point := archive.Points[cursor]
			from := int(point.Timestamp) - r.Step
			now := from + age
			epoch.Store(int64(now))
			until := min(now, int(point.Timestamp)+255*r.Step)
			series, err := shadow.Fetch(ctx, want.Metadata.Name, from, until)
			if err != nil {
				return proof, fmt.Errorf("Fetch %s archive%d: %w", want.Metadata.Name, ai, err)
			}
			if series == nil || series.Step != r.Step || int(point.Timestamp) < series.FromTime || int(point.Timestamp) >= series.UntilTime {
				return proof, fmt.Errorf("Fetch %s archive%d cannot address stored timestamp%d", want.Metadata.Name, ai, point.Timestamp)
			}
			proof.Windows++
			for cursor < len(archive.Points) && archive.Points[cursor].Timestamp < int64(series.UntilTime) {
				p := archive.Points[cursor]
				i := (int(p.Timestamp) - series.FromTime) / r.Step
				if i < 0 || i >= len(series.Values) || math.Float64bits(series.Values[i]) != math.Float64bits(p.Value) {
					return proof, fmt.Errorf("Fetch %s archive%d lost timestamp%d: expected bits%016x", want.Metadata.Name, ai, p.Timestamp, math.Float64bits(p.Value))
				}
				proof.Points++
				cursor++
			}
		}
	}
	return proof, nil
}

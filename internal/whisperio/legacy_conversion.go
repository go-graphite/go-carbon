package whisperio

import (
	"context"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// StoredSnapshot preserves every archive as currently stored. It deliberately
// avoids MergeOutOfOrder and ArchivePointsWithRollups: historical coarse values
// must not be silently recalculated during offline conversion.
func (c *ContinuationCodec) StoredSnapshot(ctx context.Context, kv store.ContinuationIO, m store.Metadata, clock func() time.Time) (store.Snapshot, error) {
	fs := newContinuationFiles(kv, true).withClock(clock)
	w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, clock))
	if err != nil {
		return store.Snapshot{}, err
	}
	defer w.Close()
	result := store.Snapshot{Metadata: m, Archives: make([]store.Archive, len(m.Retentions))}
	for i, retention := range m.Retentions {
		if err := ctx.Err(); err != nil {
			return store.Snapshot{}, err
		}
		points, err := w.ArchivePoints(i)
		if err != nil {
			return store.Snapshot{}, err
		}
		result.Archives[i] = store.Archive{Retention: retention, Points: make([]store.Point, len(points))}
		for j, point := range points {
			result.Archives[i].Points[j] = store.Point{Timestamp: int64(point.Time), Value: point.Value}
		}
	}
	return result, nil
}

var _ store.StoredArchiveCodec = (*ContinuationCodec)(nil)

package whisperio

import (
	"context"
	"fmt"
	"math"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// boundedWindowSnapshot uses the ordinary archive snapshot algorithm, including
// its NaN, signed-zero, buffered-rollup and circular-slot semantics. Only the
// physical block reader is bounded. Sidecar folding keeps the full reference
// path because folding can rewrite archives outside the requested window.
func (c *ContinuationCodec) boundedWindowSnapshot(ctx context.Context, kv store.ContinuationIO, m store.Metadata, clock func() time.Time, window store.NativeWindow) (store.Snapshot, bool, error) {
	fs := newContinuationFiles(kv, true).withClock(clock)
	w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, clock))
	if err != nil {
		return store.Snapshot{}, false, err
	}
	defer w.Close()
	if !w.IsCompressed() || w.OutOfOrderPath() != "" {
		return store.Snapshot{}, false, nil
	}
	if err := continuationPolicy(w, m); err != nil {
		return store.Snapshot{}, false, err
	}
	// An expired fine archive commonly rejects a selection. Prove that from
	// one visible newest slot instead of decoding every newer physical block.
	for _, bounds := range w.StoredArchiveBounds() {
		if bounds.Archive != 0 {
			continue
		}
		r := m.Retentions[0]
		latest := int64(bounds.Until - r.Step)
		if latest < window.From+int64(r.Step)*int64(r.Count) || latest%int64(r.Step) != 0 {
			continue
		}
		fs.rangeReads = true
		series, err := w.FetchArchiveWindow(whisper.ArchiveWindow{Archive: 0, From: int(latest), Until: int(latest) + r.Step})
		if err != nil {
			return store.Snapshot{}, false, err
		}
		if series != nil && len(series.Values()) == 1 && !math.IsNaN(series.Values()[0]) {
			return store.Snapshot{}, true, fmt.Errorf("source archive 0 has newer ring owners beyond window retention: %w", store.ErrConflict)
		}
	}
	fs.exportWindow = &window
	for _, r := range m.Retentions {
		fs.exportStep = max(fs.exportStep, int64(r.Step))
	}
	snapshot, err := continuationArchiveSnapshot(ctx, w, m)
	return snapshot, true, err
}

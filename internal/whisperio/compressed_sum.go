package whisperio

import (
	"context"
	"errors"
	"sort"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

var errDigestOffGrid = errors.New("compressed digest requires aligned timestamps")

// compressedArchiveSums hashes the exact typed group values, without Whisper's
// block arrays or dense missing-value slots. It is valid only when physical
// blocks have disjoint timestamp ranges and no newer slot owner can hide a
// selected point: the entire selected interval fits one ring and every stored
// or buffered timestamp is below hi. Older out-of-retention points cannot hide
// newer ones. Sidecars and noncanonical legacy layouts keep the reference path.
// A physical CRC cannot replace PointSum: it includes expired/hidden points and
// lacks the logical timestamp/value fingerprint and finite-point count.
func compressedArchiveSums(ctx context.Context, w *whisper.Whisper, ai int, r store.Retention, lo, hi int) ([]store.PointSum, bool, error) {
	if w.OutOfOrderPath() != "" || w.AggregationMethod() == whisper.Mix || hi-lo > r.MaxRetention() {
		return nil, false, nil
	}
	layout, err := w.ContinuationLayout()
	if err != nil {
		return nil, false, err
	}
	var blocks []whisper.ContinuationBlock
	for _, block := range layout.Blocks {
		if block.Archive == ai {
			blocks = append(blocks, block)
		}
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].First < blocks[j].First })
	for i, block := range blocks {
		if block.First <= 0 || block.Last >= hi || i > 0 && blocks[i-1].Last >= block.First {
			return nil, false, nil
		}
	}
	for _, bounds := range w.StoredArchiveBounds() {
		if bounds.Archive == ai && bounds.Until > hi {
			return nil, false, nil
		}
	}
	// Own circular buffers fall within the next archive's rollup windows;
	// ancestor buffers can also recompute this archive. Exclude those ranges
	// from raw groups and read only their small canonical overlay via Whisper.
	var windows []whisper.ArchiveWindow
	for _, window := range w.BufferedRollupWindows() {
		if window.Archive != ai && window.Archive != ai+1 {
			continue
		}
		if window.Archive == ai && window.From >= hi {
			return nil, false, nil
		}
		window.Archive = ai
		window.From = max(lo, window.From)
		window.Until = min(hi, window.Until)
		if window.From < window.Until {
			windows = append(windows, window)
		}
	}
	sort.Slice(windows, func(i, j int) bool { return windows[i].From < windows[j].From })
	merged := windows[:0]
	for _, window := range windows {
		n := len(merged)
		if n > 0 && window.From <= merged[n-1].Until {
			merged[n-1].Until = max(merged[n-1].Until, window.Until)
		} else {
			merged = append(merged, window)
		}
	}
	span := r.Step * sumBucketSlots
	firstBucket := lo / span
	sums := make([]store.PointSum, (hi-1)/span-firstBucket+1)
	add := func(ts int, value float64) {
		sums[ts/span-firstBucket].Add(int64(ts), value)
	}
	f := w.StorageFile().(*continuationFile)
	for _, block := range blocks {
		if block.Last < lo || block.First >= hi {
			continue
		}
		_, err := f.walkStorageBlock(block, nil, lo, hi-1, true, func(p store.Point) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			ts := int(p.Timestamp)
			if ts%r.Step != 0 {
				return errDigestOffGrid
			}
			if ts < lo || ts >= hi {
				return nil
			}
			for _, window := range merged {
				if ts >= window.From && ts < window.Until {
					return nil
				}
			}
			add(ts, p.Value)
			return nil
		})
		if errors.Is(err, errDigestOffGrid) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
	}
	for _, window := range merged {
		series, err := w.FetchArchiveWindow(window)
		if err != nil {
			return nil, false, err
		}
		for i, value := range series.Values() {
			add(series.FromTime()+i*r.Step, value)
		}
	}
	return sums, true, nil
}

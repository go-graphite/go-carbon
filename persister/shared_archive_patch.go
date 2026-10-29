package persister

import (
	"context"
	"math"
	"sort"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
)

type mirrorWindowStore interface {
	UpdateManyWithArchiveValues(context.Context, string, []points.Point, []store.ArchiveValue) error
}

// Retired physical blocks can drop old sparse owners that no incoming point
// replaces. Re-read their old prefix and its derived lower-archive buckets.
// Large discontinuities use the existing full-source repair rather than
// allocating a correction for an arbitrarily long, mostly empty time span.
func mirrorRetiredWindows(w *whisper.Whisper, before []whisper.ArchiveWindow) ([]whisper.ArchiveWindow, bool) {
	after := w.StoredArchiveBounds()
	retentions := w.Retentions()
	var result []whisper.ArchiveWindow
	for _, previous := range before {
		var current *whisper.ArchiveWindow
		for i := range after {
			if after[i].Archive == previous.Archive {
				current = &after[i]
				break
			}
		}
		if current == nil {
			return nil, true
		}
		if current.From <= previous.From {
			continue
		}
		end := min(current.From, previous.Until)
		for archive := previous.Archive; archive < len(retentions); archive++ {
			step := retentions[archive].SecondsPerPoint()
			from := previous.From - previous.From%step
			until := end
			if remainder := until % step; remainder != 0 {
				until += step - remainder
			}
			if until-from > retentions[archive].MaxRetention() {
				return nil, true
			}
			if until > from {
				result = append(result, whisper.ArchiveWindow{Archive: archive, From: from, Until: until})
			}
		}
	}
	return result, false
}

func mirrorInputWindows(w *whisper.Whisper, input []*whisper.TimeSeriesPoint, now int64) []whisper.ArchiveWindow {
	result := w.BufferedRollupWindows()
	retentions := w.Retentions()
	for _, point := range input {
		start := -1
		for i, r := range retentions {
			if now-int64(point.Time) <= int64(r.MaxRetention()) {
				start = i
				break
			}
		}
		if start < 0 {
			continue
		}
		for i := start; i < len(retentions); i++ {
			step := retentions[i].SecondsPerPoint()
			from := point.Time - point.Time%step
			result = append(result, whisper.ArchiveWindow{Archive: i, From: from, Until: from + step})
			// Replacing a circular owner can change an older virtual rollup even
			// though no incoming point belongs to that older lower-archive window.
			for finer := start; finer < i; finer++ {
				evicted := point.Time - retentions[finer].MaxRetention()
				if evicted <= 0 {
					continue
				}
				from := evicted - evicted%step
				result = append(result, whisper.ArchiveWindow{Archive: i, From: from, Until: from + step})
			}
		}
	}
	return result
}

func readMirrorWindows(w *whisper.Whisper, windows []whisper.ArchiveWindow) ([]store.ArchiveValue, error) {
	windows = append(windows, w.BufferedRollupWindows()...)
	sort.Slice(windows, func(i, j int) bool {
		a, b := windows[i], windows[j]
		return a.Archive < b.Archive || a.Archive == b.Archive && a.From < b.From
	})
	var merged []whisper.ArchiveWindow
	for _, window := range windows {
		if window.From <= 0 {
			continue
		}
		if len(merged) > 0 {
			last := &merged[len(merged)-1]
			if last.Archive == window.Archive && window.From <= last.Until {
				last.Until = max(last.Until, window.Until)
				continue
			}
		}
		merged = append(merged, window)
	}
	var result []store.ArchiveValue
	for _, window := range merged {
		// An arbitrarily old/future input can make adjacent ranges larger than
		// one logical archive. Keep each source read bounded to one archive ring.
		count := w.Retentions()[window.Archive].MaxRetention()
		for from := window.From; from < window.Until; {
			until := min(window.Until, from+count)
			series, err := w.FetchArchiveWindow(whisper.ArchiveWindow{Archive: window.Archive, From: from, Until: until})
			if err != nil {
				return nil, err
			}
			for i, value := range series.Values() {
				result = append(result, store.ArchiveValue{Archive: window.Archive, Timestamp: int64(series.FromTime() + i*series.Step()), Value: value, Present: !math.IsNaN(value)})
			}
			from = until
		}
	}
	return result, nil
}
